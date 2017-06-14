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

package org.apache.spark.sql.execution.vectorized

import org.apache.arrow.vector.complex.ListVector

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, StringType}

class ArrowColumnVectorSuite extends SparkFunSuite {

  test("basic") {
    val vector = new ArrowColumnVector(10, BooleanType)
    vector.putBoolean(0, true)
    vector.putBoolean(1, false)
    assert(vector.getBoolean(0) == true)
    assert(vector.getBoolean(1) == false)
  }

  test("string") {
    val vector = new ArrowColumnVector(10, StringType)
    // todo: for existing `UTF8String`, we should just pass the memory address and length to this
    // mutator, but the Arrow side forces us to pass a `ArrowBuf`, how to create `ArrowBuf` from
    // an existing memory address?
    vector.stringMutator.set(0, "abc".getBytes("utf8"))
    vector.stringMutator.set(1, "hello".getBytes("utf8"))
    assert(vector.getUTF8String(0).toString == "abc")
    assert(vector.getUTF8String(1).toString == "hello")
  }

  test("array") {
    val vector = new ArrowColumnVector(10, ArrayType(IntegerType))
    // todo: figure out the write api.
    val writer = vector.listData.getWriter
    writer.startList()
    writer.writeInt(0)
    writer.writeInt(1)
    writer.endList()

    writer.startList()
    writer.writeInt(2)
    writer.writeInt(3)
    writer.writeInt(4)
    writer.endList()

    val array = vector.getArray(0)
    assert(array.numElements() == 2)
    assert(array.getInt(0) == 0)
    assert(array.getInt(1) == 1)

    val array2 = vector.getArray(1)
    assert(array2.numElements() == 3)
    assert(array2.getInt(0) == 2)
    assert(array2.getInt(1) == 3)
    assert(array2.getInt(2) == 4)
  }

  test("nested array") {
    val vector = new ArrowColumnVector(10, ArrayType(ArrayType(IntegerType)))
    val outerWriter = vector.listData.getMutator
    val innerWriter = vector.listData.getDataVector.asInstanceOf[ListVector].getWriter

    outerWriter.startNewValue(0)

    innerWriter.startList()
    innerWriter.writeInt(0)
    innerWriter.endList()
    innerWriter.startList()
    innerWriter.writeInt(1)
    innerWriter.writeInt(2)
    innerWriter.endList()

    outerWriter.endValue(0, 2)

    outerWriter.startNewValue(1)
    innerWriter.startList()
    innerWriter.writeInt(3)
    innerWriter.endList()
    outerWriter.endValue(1, 1)

    val outerArray1 = vector.getArray(0)
    assert(outerArray1.numElements() == 2)
    val innerArray1_1 = outerArray1.getArray(0)
    assert(innerArray1_1.numElements() == 1)
    assert(innerArray1_1.getInt(0) == 0)
    val innerArray1_2 = outerArray1.getArray(1)
    assert(innerArray1_2.numElements() == 2)
    assert(innerArray1_2.getInt(0) == 1)
    assert(innerArray1_2.getInt(1) == 2)

    val outerArray2 = vector.getArray(1)
    assert(outerArray2.numElements() == 1)
    val innerArray2_1 = outerArray2.getArray(0)
    assert(innerArray2_1.numElements() == 1)
    assert(innerArray2_1.getInt(0) == 3)
  }
}
