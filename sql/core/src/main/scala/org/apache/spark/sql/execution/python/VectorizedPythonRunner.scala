/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.python

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, OutputStream}
import java.net.Socket

import scala.collection.JavaConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.stream.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo._

import org.apache.spark.{SparkEnv, SparkFiles, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonRDD, SpecialLengths}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator}
import org.apache.spark.sql.execution.vectorized.{ArrowColumnVector, ArrowUtils}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class VectorizedPythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    batchSize: Int,
    bufferSize: Int,
    reuse_worker: Boolean,
    argOffsets: Array[Array[Int]]) extends Logging {

  require(funcs.length == argOffsets.length, "argOffsets should have the same length as funcs")

  // All the Python functions should have the same exec, version and envvars.
  private val envVars = funcs.head.funcs.head.envVars
  private val pythonExec = funcs.head.funcs.head.pythonExec
  private val pythonVer = funcs.head.funcs.head.pythonVer

  // TODO: support accumulator in multiple UDF
  private val accumulator = funcs.head.funcs.head.accumulator

  // todo: return column batch?
  def compute(
      inputRows: Iterator[InternalRow],
      schema: StructType,
      partitionIndex: Int,
      context: TaskContext): Iterator[InternalRow] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread
    if (reuse_worker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    val worker: Socket = env.createPythonWorker(pythonExec, envVars.asScala.toMap)
    // Whether is the worker released into idle pool
    @volatile var released = false

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = new WriterThread(
      env, worker, inputRows, ArrowUtils.toArrowSchema(schema), partitionIndex, context)

    context.addTaskCompletionListener { context =>
      writerThread.shutdownOnTaskCompletion()
      if (!reuse_worker || !released) {
        try {
          worker.close()
        } catch {
          case e: Exception =>
            logWarning("Failed to close worker socket", e)
        }
      }
    }

    writerThread.start()

    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
    val reader = new ArrowStreamReader(stream, ArrowColumnVector.allocator)

    new Iterator[InternalRow] {
      private val arrowBatch = reader.getVectorSchemaRoot
      private val vectors = arrowBatch.getFieldVectors.asScala.map { arrowVector =>
        new ArrowColumnVector(arrowVector)
      }
      private val schema = ArrowUtils.fromArrowSchema(arrowBatch.getSchema)
      private val result = new SpecificInternalRow(schema)

      private[this] var batchLoaded = true
      private[this] var numRows = 0
      private[this] var currentRowId = 0

      override def hasNext: Boolean = batchLoaded && (currentRowId < numRows || {
        batchLoaded = reader.loadNextBatch()
        if (batchLoaded) {
          currentRowId = 0
          numRows = arrowBatch.getRowCount
        } else {
          // end of arrow batches, handle some special signal
          assert(stream.readInt() == SpecialLengths.TIMING_DATA)
          // Timing data from worker
          val bootTime = stream.readLong()
          val initTime = stream.readLong()
          val finishTime = stream.readLong()
          val boot = bootTime - startTime
          val init = initTime - bootTime
          val finish = finishTime - initTime
          val total = finishTime - startTime
          logInfo("Times: total = %s, boot = %s, init = %s, finish = %s".format(total, boot,
            init, finish))
          val memoryBytesSpilled = stream.readLong()
          val diskBytesSpilled = stream.readLong()
          context.taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
          context.taskMetrics.incDiskBytesSpilled(diskBytesSpilled)

          assert(stream.readInt() == SpecialLengths.END_OF_DATA_SECTION)
          // We've finished the data section of the output, but we can still
          // read some accumulator updates:
          val numAccumulatorUpdates = stream.readInt()
          (1 to numAccumulatorUpdates).foreach { _ =>
            val updateLen = stream.readInt()
            val update = new Array[Byte](updateLen)
            stream.readFully(update)
            accumulator.add(update)
          }
          // Check whether the worker is ready to be re-used.
          if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
            if (reuse_worker) {
              env.releasePythonWorker(pythonExec, envVars.asScala.toMap, worker)
              released = true
            }
          }
          // todo: we need something like `read.end()`, which release all the resources, but leave
          // the input stream open. `reader.close` will close the socket and we can't reuse worker.
          arrowBatch.close()
          // todo: how to handle PYTHON_EXCEPTION_THROWN?
        }
        hasNext // skip empty batches if any
      })

      override def next(): InternalRow = {
        if (hasNext) {
          schema.map(_.dataType).zipWithIndex.foreach {
            case (dt, index) => dt match {
              case IntegerType => result.setInt(index, vectors(index).getInt(currentRowId))
              case LongType => result.setLong(index, vectors(index).getLong(currentRowId))
            }
          }
          currentRowId += 1
          result
        } else {
          throw new NoSuchElementException
        }
      }
    }
  }

  class WriterThread(
      env: SparkEnv,
      worker: Socket,
      inputRows: Iterator[InternalRow],
      schema: Schema,
      partitionIndex: Int,
      context: TaskContext)
    extends Thread(s"stdout writer for $pythonExec") {

    @volatile private var _exception: Exception = null

    private val pythonIncludes = funcs.flatMap(_.funcs.flatMap(_.pythonIncludes.asScala)).toSet
    private val broadcastVars = funcs.flatMap(_.funcs.flatMap(_.broadcastVars.asScala))

    setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the Python process. */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      assert(context.isCompleted)
      this.interrupt()
    }

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        TaskContext.setTaskContext(context)

        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)
        // Python version of driver
        PythonRDD.writeUTF(pythonVer, dataOut)
        // Write out the TaskContextInfo
        dataOut.writeInt(context.stageId())
        dataOut.writeInt(context.partitionId())
        dataOut.writeInt(context.attemptNumber())
        dataOut.writeLong(context.taskAttemptId())
        // sparkFilesDir
        PythonRDD.writeUTF(SparkFiles.getRootDirectory(), dataOut)
        // Python includes (*.zip and *.egg files)
        dataOut.writeInt(pythonIncludes.size)
        for (include <- pythonIncludes) {
          PythonRDD.writeUTF(include, dataOut)
        }
        // Broadcast variables
        val oldBids = PythonRDD.getWorkerBroadcasts(worker)
        val newBids = broadcastVars.map(_.id).toSet
        // number of different broadcasts
        val toRemove = oldBids.diff(newBids)
        val cnt = toRemove.size + newBids.diff(oldBids).size
        dataOut.writeInt(cnt)
        for (bid <- toRemove) {
          // remove the broadcast from worker
          dataOut.writeLong(- bid - 1)  // bid >= 0
          oldBids.remove(bid)
        }
        for (broadcast <- broadcastVars) {
          if (!oldBids.contains(broadcast.id)) {
            // send new broadcast
            dataOut.writeLong(broadcast.id)
            PythonRDD.writeUTF(broadcast.value.path, dataOut)
            oldBids.add(broadcast.id)
          }
        }
        dataOut.flush()

        // 2 means arrow mode
        dataOut.writeInt(2)
        dataOut.writeInt(funcs.length)
        funcs.zip(argOffsets).foreach { case (chained, offsets) =>
          dataOut.writeInt(offsets.length)
          offsets.foreach(dataOut.writeInt)
          dataOut.writeInt(chained.funcs.length)
          chained.funcs.foreach { f =>
            dataOut.writeInt(f.command.length)
            dataOut.write(f.command)
          }
        }
        dataOut.flush()

        val arrowWriter = GenerateArrowWriter.generate(schema)
        arrowWriter.initialize(batchSize)
        arrowWriter.writeAll(inputRows, dataOut)

        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case e: Exception if context.isCompleted || context.isInterrupted =>
          logDebug("Exception thrown after task completion (likely due to cleanup)", e)
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          _exception = e
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }
      }
    }
  }

}

abstract class ArrowWriter {
  def initialize(batchSize: Int): Unit
  def writeAll(inputRows: Iterator[InternalRow], out: OutputStream): Unit
}

object GenerateArrowWriter extends CodeGenerator[Schema, ArrowWriter] with Logging {

  protected def canonicalize(in: Schema): Schema = in
  protected def bind(in: Schema, inputSchema: Seq[Attribute]): Schema = in

  private def fieldTypeToVectorClass(arrowType: ArrowType, nullable: Boolean): String =
    (arrowType, nullable) match {
      case (ArrowType.Bool.INSTANCE, true) =>
        classOf[NullableBitVector].getName
      case (intType: ArrowType.Int, true) if intType.getBitWidth() == 8 * 4 =>
        classOf[NullableIntVector].getName
      case (intType: ArrowType.Int, true) if intType.getBitWidth() == 8 * 8 =>
        classOf[NullableBigIntVector].getName
      case (ArrowType.Utf8.INSTANCE, true) =>
        classOf[NullableVarCharVector].getName
    }

  protected def create(schema: Schema): ArrowWriter = {
    val ctx = newCodeGenContext()
    val schemaName = ctx.addReferenceObj("schema", schema)

    val (vectorNames, vectorTypes) = schema.getFields().asScala.map { field =>
      val name = ctx.freshName(s"${field.getName}Vector")
      val fieldType = field.getFieldType
      (name, (fieldType.getType, fieldType.isNullable))
    }.unzip

    val initFieldVectors = vectorNames.zip(vectorTypes).zipWithIndex.map {
      case ((name, (arrowType, nullable)), idx) =>
      val cls = fieldTypeToVectorClass(arrowType, nullable)
      s"$cls $name = ($cls) root.getFieldVectors().get($idx);"
    }.mkString("\n")

    val allocateMemories = vectorNames.map { name =>
      s"$name.allocateNew();"
    }.mkString("\n")

    val writeValues = vectorNames.zip(vectorTypes.map(_._1)).zipWithIndex.map {
      case ((name, ArrowType.Bool.INSTANCE), idx) =>
        s"$name.getMutator().setSafe(rowId, row.getBoolean($idx) ? 1 : 0);"
      case ((name, intType: ArrowType.Int), idx) if intType.getBitWidth() == 8 * 4 =>
        s"$name.getMutator().setSafe(rowId, row.getInt($idx));"
      case ((name, intType: ArrowType.Int), idx) if intType.getBitWidth() == 8 * 8 =>
        s"$name.getMutator().setSafe(rowId, row.getLong($idx));"
      case ((name, ArrowType.Utf8.INSTANCE), idx) =>
        s"$name.getMutator().set(rowId, row.getUTF8String($idx).getBytes());"
    }.mkString("\n")

    val setValueCounts = vectorNames.map { name =>
      s"$name.getMutator().setValueCount(rowId);"
    }.mkString("\n")

    val codeBody = s"""
      import java.io.IOException;
      import java.io.OutputStream;
      import scala.collection.Iterator;
      import ${classOf[VectorSchemaRoot].getName};
      import ${classOf[ArrowStreamWriter].getName};
      import ${classOf[ArrowWriter].getName};
      import ${classOf[ArrowColumnVector].getName};

      public SpecificArrowWriter generate(Object[] references) {
        return new SpecificArrowWriter(references);
      }

      class SpecificArrowWriter extends ArrowWriter {

        private Object[] references;
        ${ctx.declareMutableStates()}

        private int batchSize;

        public SpecificArrowWriter(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        public void initialize(int batchSize) {
          this.batchSize = batchSize;
        }

        public void writeAll(Iterator<InternalRow> inputRows, OutputStream out) throws IOException {
          VectorSchemaRoot root = VectorSchemaRoot.create($schemaName, ArrowColumnVector.allocator);
          $initFieldVectors

          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out);
          writer.start();

          try {
            while (inputRows.hasNext()) {
              int rowId = 0;
              $allocateMemories
              while (inputRows.hasNext() && rowId < batchSize) {
                InternalRow row = (InternalRow) inputRows.next();
                $writeValues
                rowId += 1;
              }
              $setValueCounts
              root.setRowCount(rowId);
              writer.writeBatch();
            }
          } finally {
            writer.end();
            out.flush();
            root.close();
          }
        }

        ${ctx.initNestedClasses()}
        ${ctx.declareNestedClasses()}
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated ArrowWriter:\n${CodeFormatter.format(code)}")

    CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[ArrowWriter]
  }
}
