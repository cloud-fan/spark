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

package org.apache.spark.sql.execution.vectorized;

import java.math.BigInteger;
import java.util.Iterator;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Utilities to help manipulate data associate with ColumnVectors. These should be used mostly
 * for debugging or other non-performance critical paths.
 * These utilities are mostly used to convert ColumnVectors into other formats.
 */
public class ColumnVectorUtils {
  /**
   * Populates the entire `col` with `row[fieldIdx]`
   */
  public static void populate(ColumnVector col, InternalRow row, int fieldIdx) {
    int capacity = col.capacity;
    DataType t = col.dataType();

    if (row.isNullAt(fieldIdx)) {
      col.putNulls(0, capacity);
    } else {
      if (t == DataTypes.BooleanType) {
        col.putBooleans(0, capacity, row.getBoolean(fieldIdx));
      } else if (t == DataTypes.ByteType) {
        col.putBytes(0, capacity, row.getByte(fieldIdx));
      } else if (t == DataTypes.ShortType) {
        col.putShorts(0, capacity, row.getShort(fieldIdx));
      } else if (t == DataTypes.IntegerType) {
        col.putInts(0, capacity, row.getInt(fieldIdx));
      } else if (t == DataTypes.LongType) {
        col.putLongs(0, capacity, row.getLong(fieldIdx));
      } else if (t == DataTypes.FloatType) {
        col.putFloats(0, capacity, row.getFloat(fieldIdx));
      } else if (t == DataTypes.DoubleType) {
        col.putDoubles(0, capacity, row.getDouble(fieldIdx));
      } else if (t == DataTypes.StringType) {
        UTF8String v = row.getUTF8String(fieldIdx);
        byte[] bytes = v.getBytes();
        for (int i = 0; i < capacity; i++) {
          col.putByteArray(i, bytes);
        }
      } else if (t instanceof DecimalType) {
        DecimalType dt = (DecimalType)t;
        Decimal d = row.getDecimal(fieldIdx, dt.precision(), dt.scale());
        if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
          col.putInts(0, capacity, (int)d.toUnscaledLong());
        } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
          col.putLongs(0, capacity, d.toUnscaledLong());
        } else {
          final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
          byte[] bytes = integer.toByteArray();
          for (int i = 0; i < capacity; i++) {
            col.putByteArray(i, bytes);
          }
        }
      } else if (t instanceof CalendarIntervalType) {
        CalendarInterval c = (CalendarInterval)row.get(fieldIdx, t);
        col.getChildColumn(0).putInts(0, capacity, c.months);
        col.getChildColumn(1).putLongs(0, capacity, c.microseconds);
      } else if (t instanceof DateType) {
        col.putInts(0, capacity, row.getInt(fieldIdx));
      } else if (t instanceof TimestampType) {
        col.putLongs(0, capacity, row.getLong(fieldIdx));
      }
    }
  }

  /**
   * Returns the array data as the java primitive array.
   * For example, an array of IntegerType will return an int[].
   * Throws exceptions for unhandled schemas.
   */
  public static Object toPrimitiveJavaArray(ColumnVector.Array array) {
    DataType dt = array.data.dataType();
    if (dt instanceof IntegerType) {
      int[] result = new int[array.length];
      ColumnVector data = array.data;
      for (int i = 0; i < result.length; i++) {
        if (data.isNullAt(array.offset + i)) {
          throw new RuntimeException("Cannot handle NULL values.");
        }
        result[i] = data.getInt(array.offset + i);
      }
      return result;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Converts an iterator of rows into a single ColumnBatch.
   */
  public static ColumnarBatch toBatch(
      StructType schema, MemoryMode memMode, Iterator<Row> row) {
    ColumnarBatch batch = ColumnarBatch.allocate(schema, memMode);
    RowWriter writer = (RowWriter) ColumnVectorWriter.create(schema);
    int index = 0;
    while (row.hasNext()) {
      Row r = row.next();
      writer.nextRow(r);
      writer.write(batch, index);
      index++;
    }
    batch.setNumRows(index);
    return batch;
  }
}

// TODO: generalize and publish it.
interface ColumnVectorWriter {
  void write(ColumnVector vector, int index);

  static ColumnVectorWriter create(DataType dt) {
    if (dt instanceof ArrayType) {
      return new SeqWriter(((ArrayType) dt).elementType());
    } else if (dt instanceof StructType) {
      return new RowWriter((StructType) dt);
    } else {
      return new SimpleWriter(dt);
    }
  }
}

class RowWriter implements ColumnVectorWriter {
  private ColumnVectorWriter[] childWriters;

  private Row row = null;
  private int rowCount = 0;

  public RowWriter(StructType schema) {
    childWriters = new ColumnVectorWriter[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      childWriters[i] = ColumnVectorWriter.create(schema.apply(i).dataType());
    }
  }

  public void nextRow(Row row) {
    this.row = row;
  }

  @Override
  public void write(ColumnVector vector, int index) {
    if (row == null) {
      vector.putNull(index);
      for (int i = 0; i < childWriters.length; i++) {
        vector.getChildColumn(i).reserve(rowCount + 1);
        vector.childColumns[i].putNull(rowCount);
      }
      rowCount++;
    } else {
      for (int i = 0; i < childWriters.length; i++) {
        if (childWriters[i] instanceof RowWriter) {
          ((RowWriter) childWriters[i]).nextRow(row.getStruct(i));
        } else if (childWriters[i] instanceof SeqWriter) {
          ((SeqWriter) childWriters[i]).nextSeq(row.<Object>getSeq(i));
        } else {
          ((SimpleWriter) childWriters[i]).nextValue(row.get(i));
        }
        vector.getChildColumn(i).reserve(rowCount + 1);
        childWriters[i].write(vector.getChildColumn(i), rowCount);
      }
      rowCount++;
    }
  }

  public void write(ColumnarBatch batch, int index) {
    if (row == null) {
      for (int i = 0; i < childWriters.length; i++) {
        batch.column(i).reserve(rowCount + 1);
        batch.column(i).putNull(rowCount);
      }
      rowCount++;
    } else {
      for (int i = 0; i < childWriters.length; i++) {
        if (childWriters[i] instanceof RowWriter) {
          ((RowWriter) childWriters[i]).nextRow(row.getStruct(i));
        } else if (childWriters[i] instanceof SeqWriter) {
          ((SeqWriter) childWriters[i]).nextSeq(row.<Object>getSeq(i));
        } else {
          ((SimpleWriter) childWriters[i]).nextValue(row.get(i));
        }
        batch.column(i).reserve(rowCount + 1);
        childWriters[i].write(batch.column(i), rowCount);
      }
      rowCount++;
    }
  }
}

class SeqWriter implements ColumnVectorWriter {
  private ColumnVectorWriter elementWriter;

  scala.collection.Seq<Object> seq = null;

  public SeqWriter(DataType et) {
    elementWriter = ColumnVectorWriter.create(et);
  }

  public void nextSeq(scala.collection.Seq<Object> seq) {
    this.seq = seq;
  }

  @Override
  public void write(ColumnVector vector, int index) {
    if (seq == null) {
      vector.putNull(index);
    } else {
      vector.arrayData().reserve(vector.elementsWritten + seq.size());
      if (elementWriter instanceof RowWriter) {
        RowWriter writer = (RowWriter) elementWriter;
        for (int i = 0; i < seq.size(); i++) {
          writer.nextRow((Row) seq.apply(i));
          writer.write(vector.arrayData(), i + vector.elementsWritten);
        }
      } else if (elementWriter instanceof SeqWriter) {
        SeqWriter writer = (SeqWriter) elementWriter;
        for (int i = 0; i < seq.size(); i++) {
          writer.nextSeq((scala.collection.Seq<Object>) seq.apply(i));
          writer.write(vector.arrayData(), i + vector.elementsWritten);
        }
      } else {
        SimpleWriter writer = (SimpleWriter) elementWriter;
        for (int i = 0; i < seq.size(); i++) {
          writer.nextValue(seq.apply(i));
          writer.write(vector.arrayData(), i + vector.elementsWritten);
        }
      }
      vector.putArrayOffsetAndSize(index, vector.elementsWritten, seq.size());
      vector.elementsWritten += seq.size();
    }
  }
}

class SimpleWriter implements ColumnVectorWriter {
  private Object value;
  private DataType dt;

  public SimpleWriter(DataType dt) {
    this.dt = dt;
  }

  public void nextValue(Object value) {
    this.value = value;
  }

  @Override
  public void write(ColumnVector vector, int index) {
    if (value == null) {
      vector.putNull(index);
    } else {
      if (dt == DataTypes.BooleanType) {
        vector.putBoolean(index, (Boolean) value);
      } else if (dt == DataTypes.ByteType) {
        vector.putByte(index, (Byte) value);
      } else if (dt == DataTypes.ShortType) {
        vector.putShort(index, (Short) value);
      } else if (dt == DataTypes.IntegerType) {
        vector.putInt(index, (Integer) value);
      } else if (dt == DataTypes.LongType) {
        vector.putLong(index, (Long) value);
      } else if (dt == DataTypes.FloatType) {
        vector.putFloat(index, (Float) value);
      } else if (dt == DataTypes.DoubleType) {
        vector.putDouble(index, (Double) value);
      } else if (dt instanceof DecimalType) {
        int precision = ((DecimalType) dt).precision();
        vector.putDecimal(index, Decimal.apply((java.math.BigDecimal) value), precision);
      } else if (dt == DataTypes.BinaryType) {
        vector.putByteArray(index, (byte[]) value);
      } else if (dt == DataTypes.StringType) {
        UTF8String str = UTF8String.fromString((String) value);
        vector.putBinary(index, str.getBaseObject(), str.getBaseOffset(), str.numBytes());
      } else if (dt == DataTypes.CalendarIntervalType) {
        CalendarInterval c = (CalendarInterval) value;
        vector.getChildColumn(0).reserve(index + 1);
        vector.getChildColumn(0).putInt(index, c.months);
        vector.getChildColumn(1).reserve(index + 1);
        vector.getChildColumn(1).putLong(index, c.microseconds);
      } else {
        throw new IllegalStateException(dt.toString() + " is not primitive type.");
      }
    }
  }
}
