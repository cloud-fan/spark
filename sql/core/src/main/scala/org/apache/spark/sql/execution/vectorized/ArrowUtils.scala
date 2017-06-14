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

import scala.collection.JavaConverters._

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.sql.types._

object ArrowUtils {

  def toArrowType(dt: DataType): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType => new ArrowType.Int(8 * 8, true)
    case StringType => ArrowType.Utf8.INSTANCE
    case _: ArrayType => ArrowType.List.INSTANCE
    case _ => throw new IllegalStateException("unknown type: " + dt.simpleString)
  }

  def fromArrowType(dt: ArrowType): DataType = dt match {
    case ArrowType.Bool.INSTANCE => BooleanType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => ByteType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
    case ArrowType.Utf8.INSTANCE => StringType
  }

  def toArrowSchema(schema: StructType): Schema = {
    new Schema(schema.map { field =>
      // todo: nested type
      Field.nullable(field.name, toArrowType(field.dataType))
    }.asJava)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      StructField(field.getName, fromArrowType(field.getFieldType.getType))
    })
  }
}
