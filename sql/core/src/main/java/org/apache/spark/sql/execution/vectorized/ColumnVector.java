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

import java.math.BigDecimal;
import java.math.BigInteger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class represents a column of values and provides the main APIs to access the data
 * values. It supports all the types and contains get/put APIs as well as their batched versions.
 * The batched versions are preferable whenever possible.
 *
 * To handle nested schemas, ColumnVector has two types: Arrays and Structs. In both cases these
 * columns have child columns. All of the data is stored in the child columns and the parent column
 * contains nullability, and in the case of Arrays, the lengths and offsets into the child column.
 * Lengths and offsets are encoded identically to INTs.
 * Maps are just a special case of a two field struct.
 * Strings are handled as an Array of ByteType.
 *
 * Capacity: The data stored is dense but the arrays are not fixed capacity. It is the
 * responsibility of the caller to call reserve() to ensure there is enough room before adding
 * elements. This means that the put() APIs do not check as in common cases (i.e. flat schemas),
 * the lengths are known up front.
 *
 * Most of the APIs take the rowId as a parameter. This is the batch local 0-based row id for values
 * in the current RowBatch.
 *
 * A ColumnVector should be considered immutable once originally created. In other words, it is not
 * valid to call put APIs after reads until reset() is called.
 *
 * ColumnVectors are intended to be reused.
 */
public abstract class ColumnVector implements AutoCloseable {
  /**
   * Allocates a column to store elements of `type` on or off heap.
   * Capacity is the initial capacity of the vector and it will grow as necessary. Capacity is
   * in number of elements, not number of bytes.
   */
  public static ColumnVector allocate(int capacity, DataType type, MemoryMode mode) {
    if (mode == MemoryMode.OFF_HEAP) {
      return new OffHeapColumnVector(capacity, type);
    } else {
      return new OnHeapColumnVector(capacity, type);
    }
  }

  /**
   * Holder object to return an array. This object is intended to be reused. Callers should
   * copy the data out if it needs to be stored.
   */
  public static final class Array extends ArrayData {
    // The data for this array. This array contains elements from
    // data[offset] to data[offset + length).
    public final ColumnVector data;
    public int length;
    public int offset;

    protected Array(ColumnVector data) {
      this.data = data;
    }

    @Override
    public int numElements() { return length; }

    @Override
    public ArrayData copy() {
      throw new UnsupportedOperationException();
    }

    // TODO: this is extremely expensive.
    @Override
    public Object[] array() {
      DataType dt = data.dataType();
      Object[] list = new Object[length];

      if (dt instanceof BooleanType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getBoolean(offset + i);
          }
        }
      } else if (dt instanceof ByteType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getByte(offset + i);
          }
        }
      } else if (dt instanceof ShortType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getShort(offset + i);
          }
        }
      } else if (dt instanceof IntegerType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getInt(offset + i);
          }
        }
      } else if (dt instanceof FloatType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getFloat(offset + i);
          }
        }
      } else if (dt instanceof DoubleType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getDouble(offset + i);
          }
        }
      } else if (dt instanceof LongType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = data.getLong(offset + i);
          }
        }
      } else if (dt instanceof DecimalType) {
        DecimalType decType = (DecimalType)dt;
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = getDecimal(i, decType.precision(), decType.scale());
          }
        }
      } else if (dt instanceof StringType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = getUTF8String(i).toString();
          }
        }
      } else if (dt instanceof CalendarIntervalType) {
        for (int i = 0; i < length; i++) {
          if (!data.isNullAt(offset + i)) {
            list[i] = getInterval(i);
          }
        }
      } else {
        throw new UnsupportedOperationException("Type " + dt);
      }
      return list;
    }

    @Override
    public boolean isNullAt(int ordinal) { return data.isNullAt(offset + ordinal); }

    @Override
    public boolean getBoolean(int ordinal) {
      return data.getBoolean(offset + ordinal);
    }

    @Override
    public byte getByte(int ordinal) { return data.getByte(offset + ordinal); }

    @Override
    public short getShort(int ordinal) {
      return data.getShort(offset + ordinal);
    }

    @Override
    public int getInt(int ordinal) { return data.getInt(offset + ordinal); }

    @Override
    public long getLong(int ordinal) { return data.getLong(offset + ordinal); }

    @Override
    public float getFloat(int ordinal) {
      return data.getFloat(offset + ordinal);
    }

    @Override
    public double getDouble(int ordinal) { return data.getDouble(offset + ordinal); }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      return data.getDecimal(offset + ordinal, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return data.getUTF8String(offset + ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return data.getBinary(offset + ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      int month = data.getChildColumn(0).getInt(offset + ordinal);
      long microseconds = data.getChildColumn(1).getLong(offset + ordinal);
      return new CalendarInterval(month, microseconds);
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return data.getStruct(offset + ordinal);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return data.getArray(offset + ordinal);
    }

    @Override
    public MapData getMap(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void update(int ordinal, Object value) { throw new UnsupportedOperationException(); }

    @Override
    public void setNullAt(int ordinal) { throw new UnsupportedOperationException(); }
  }

  /**
   * Returns the data type of this column.
   */
  public final DataType dataType() { return type; }

  /**
   * Resets this column for writing. The currently stored values are no longer accessible.
   */
  public void reset() {
    if (isConstant) return;

    if (childColumns != null) {
      for (ColumnVector c: childColumns) {
        c.reset();
      }
    }
    numNulls = 0;
    elementsWritten = 0;
    if (anyNullsSet) {
      putNotNulls(0, capacity);
      anyNullsSet = false;
    }
  }

  /**
   * Cleans up memory for this column. The column is not usable after this.
   * TODO: this should probably have ref-counted semantics.
   */
  public abstract void close();

  public void reserve(int requiredCapacity) {
    if (requiredCapacity > capacity) {
      int newCapacity = (int) Math.min(MAX_CAPACITY, requiredCapacity * 2L);
      if (requiredCapacity <= newCapacity) {
        try {
          reserveInternal(newCapacity);
        } catch (OutOfMemoryError outOfMemoryError) {
          throwUnsupportedException(requiredCapacity, outOfMemoryError);
        }
      } else {
        throwUnsupportedException(requiredCapacity, null);
      }
    }
  }

  private void throwUnsupportedException(int requiredCapacity, Throwable cause) {
    String message = "Cannot reserve additional contiguous bytes in the vectorized reader " +
        "(requested = " + requiredCapacity + " bytes). As a workaround, you can disable the " +
        "vectorized reader by setting " + SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key() +
        " to false.";

    if (cause != null) {
      throw new RuntimeException(message, cause);
    } else {
      throw new RuntimeException(message);
    }
  }

  /**
   * Ensures that there is enough storage to store capacity elements. That is, the put() APIs
   * must work for all rowIds < capacity.
   */
  protected abstract void reserveInternal(int capacity);

  /**
   * Returns the base object of the data of this column vector if it's a byte column vector, can be
   * null if the data is off-heap.
   */
  public abstract Object binaryBaseObject();

  /**
   * Returns the base offset of the data of this column vector if it's a byte column vector.
   */
  public abstract long binaryBaseOffset();

  /**
   * Returns the number of nulls in this column.
   */
  public final int numNulls() { return numNulls; }

  /**
   * Returns true if any of the nulls indicator are set for this column. This can be used
   * as an optimization to prevent setting nulls.
   */
  public final boolean anyNullsSet() { return anyNullsSet; }

  /**
   * Sets the value at rowId to null/not null.
   */
  public abstract void putNotNull(int rowId);
  public abstract void putNull(int rowId);

  /**
   * Sets the values from [rowId, rowId + count) to null/not null.
   */
  public abstract void putNulls(int rowId, int count);
  public abstract void putNotNulls(int rowId, int count);

  /**
   * Returns whether the value at rowId is NULL.
   */
  public abstract boolean isNullAt(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putBoolean(int rowId, boolean value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putBooleans(int rowId, int count, boolean value);

  /**
   * Returns the value for rowId.
   */
  public abstract boolean getBoolean(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putByte(int rowId, byte value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putBytes(int rowId, int count, byte value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putBytes(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + length) to [base + offset, base + offset + length);
   */
  public abstract void putBytes(int rowId, Object base, long offset, int length);

  /**
   * Returns the value for rowId.
   */
  public abstract byte getByte(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putShort(int rowId, short value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putShorts(int rowId, int count, short value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putShorts(int rowId, int count, short[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract short getShort(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putInt(int rowId, int value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putInts(int rowId, int count, int value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putInts(int rowId, int count, int[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be 4-byte little endian ints.
   */
  public abstract void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract int getInt(int rowId);

  /**
   * Returns the dictionary Id for rowId.
   * This should only be called when the ColumnVector is dictionaryIds.
   * We have this separate method for dictionaryIds as per SPARK-16928.
   */
  public abstract int getDictId(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putLong(int rowId, long value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putLongs(int rowId, int count, long value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putLongs(int rowId, int count, long[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be 8-byte little endian longs.
   */
  public abstract void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract long getLong(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putFloat(int rowId, float value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putFloats(int rowId, int count, float value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putFloats(int rowId, int count, float[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be ieee formatted floats.
   */
  public abstract void putFloats(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract float getFloat(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putDouble(int rowId, double value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putDoubles(int rowId, int count, double value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putDoubles(int rowId, int count, double[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be ieee formatted doubles.
   */
  public abstract void putDoubles(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the value for rowId.
   */
  public abstract double getDouble(int rowId);

  /**
   * Returns a utility object to get structs.
   */
  public ColumnarBatch.Row getStruct(int rowId) {
    resultStruct.rowId = rowId;
    return resultStruct;
  }

  /**
   * Returns a utility object to get structs.
   * provided to keep API compatibility with InternalRow for code generation
   */
  public ColumnarBatch.Row getStruct(int rowId, int size) {
    resultStruct.rowId = rowId;
    return resultStruct;
  }

  /**
   * After writing array elements to the child column vector, call this method to set the offset and
   * size of the written array.
   */
  public void putArrayOffsetAndSize(int rowId, int offset, int size) {
    long offsetAndSize = (((long) offset) << 32) | size;
    putLong(rowId, offsetAndSize);
  }

  /**
   * Returns the array at rowid.
   */
  public final Array getArray(int rowId) {
    long offsetAndSize = getLong(rowId);
    resultArray.offset = (int) (offsetAndSize >> 32);
    resultArray.length = (int) offsetAndSize;
    return resultArray;
  }

  public final void putBinary(int rowId, Object base, long offset, int numBytes) {
    arrayData().reserve(elementsWritten + numBytes);
    arrayData().putBytes(elementsWritten, base, offset, numBytes);
    putArrayOffsetAndSize(rowId, elementsWritten, numBytes);
    elementsWritten += numBytes;
  }

  public final void putByteArray(int rowId, byte[] value) {
    putBinary(rowId, value, Platform.BYTE_ARRAY_OFFSET, value.length);
  }

  public final void putByteArray(int rowId, byte[] value, int offset, int length) {
    putBinary(rowId, value, Platform.BYTE_ARRAY_OFFSET + offset, length);
  }

  /**
   * Returns the value for rowId.
   */
  public MapData getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the decimal for rowId.
   */
  public final Decimal getDecimal(int rowId, int precision, int scale) {
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(getInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(rowId), precision, scale);
    } else {
      // TODO: best perf?
      byte[] bytes = getBinary(rowId);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }


  public final void putDecimal(int rowId, Decimal value, int precision) {
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      putInt(rowId, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      putLong(rowId, value.toUnscaledLong());
    } else {
      BigInteger bigInteger = value.toJavaBigDecimal().unscaledValue();
      putByteArray(rowId, bigInteger.toByteArray());
    }
  }

  /**
   * Returns the UTF8String for rowId.
   */
  public final UTF8String getUTF8String(int rowId) {
    if (dictionary == null) {
      ColumnVector.Array a = getArray(rowId);
      return UTF8String.fromAddress(
        a.data.binaryBaseObject(), a.data.binaryBaseOffset() + a.offset, a.length);
    } else {
      byte[] bytes = dictionary.decodeToBinary(dictionaryIds.getDictId(rowId));
      return UTF8String.fromBytes(bytes);
    }
  }

  /**
   * Returns the byte array for rowId.
   */
  public final byte[] getBinary(int rowId) {
    if (dictionary == null) {
      ColumnVector.Array a = getArray(rowId);
      byte[] bytes = new byte[a.length];
      Platform.copyMemory(a.data.binaryBaseObject(), a.data.binaryBaseOffset() + a.offset,
        bytes, Platform.BYTE_ARRAY_OFFSET, a.length);
      return bytes;
    } else {
      return dictionary.decodeToBinary(dictionaryIds.getDictId(rowId));
    }
  }

  /**
   * Returns the data for the underlying array.
   */
  public final ColumnVector arrayData() { return childColumns[0]; }

  /**
   * Returns the ordinal's child data column.
   */
  public final ColumnVector getChildColumn(int ordinal) { return childColumns[ordinal]; }

  /**
   * Returns true if this column is an array.
   */
  public final boolean isArray() { return resultArray != null; }

  /**
   * Marks this column as being constant.
   */
  public final void setIsConstant() { isConstant = true; }

  /**
   * Maximum number of rows that can be stored in this column.
   */
  protected int capacity;

  /**
   * Upper limit for the maximum capacity for this column.
   */
  @VisibleForTesting
  protected int MAX_CAPACITY = Integer.MAX_VALUE;

  /**
   * Data type for this column.
   */
  protected final DataType type;

  /**
   * Number of nulls in this column. This is an optimization for the reader, to skip NULL checks.
   */
  protected int numNulls;

  /**
   * True if there is at least one NULL byte set. This is an optimization for the writer, to skip
   * having to clear NULL bits.
   */
  protected boolean anyNullsSet;

  /**
   * True if this column's values are fixed. This means the column values never change, even
   * across resets.
   */
  protected boolean isConstant;

  /**
   * Default size of each array length value. This grows as necessary.
   */
  protected static final int DEFAULT_ARRAY_LENGTH = 4;

  /**
   * Tracks how many elements have been written, only valid for array column vector.
   */
  protected int elementsWritten;

  /**
   * If this is a nested type (array or struct), the column for the child data.
   */
  protected final ColumnVector[] childColumns;

  /**
   * Reusable Array holder for getArray().
   */
  protected final Array resultArray;

  /**
   * Reusable Struct holder for getStruct().
   */
  protected final ColumnarBatch.Row resultStruct;

  /**
   * The Dictionary for this column.
   *
   * If it's not null, will be used to decode the value in getXXX().
   */
  protected Dictionary dictionary;

  /**
   * Reusable column for ids of dictionary.
   */
  protected ColumnVector dictionaryIds;

  /**
   * Update the dictionary.
   */
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  /**
   * Returns true if this column has a dictionary.
   */
  public boolean hasDictionary() { return this.dictionary != null; }

  /**
   * Reserve a integer column for ids of dictionary.
   */
  public ColumnVector reserveDictionaryIds(int capacity) {
    if (dictionaryIds == null) {
      dictionaryIds = allocate(capacity, DataTypes.IntegerType,
        this instanceof OnHeapColumnVector ? MemoryMode.ON_HEAP : MemoryMode.OFF_HEAP);
    } else {
      dictionaryIds.reset();
      dictionaryIds.reserve(capacity);
    }
    return dictionaryIds;
  }

  /**
   * Returns the underlying integer column for ids of dictionary.
   */
  public ColumnVector getDictionaryIds() {
    return dictionaryIds;
  }

  /**
   * Sets up the common state and also handles creating the child columns if this is a nested
   * type.
   */
  protected ColumnVector(int capacity, DataType type, MemoryMode memMode) {
    this.capacity = capacity;
    this.type = type;

    if (type instanceof ArrayType || type instanceof BinaryType || type instanceof StringType
        || DecimalType.isByteArrayDecimalType(type)) {
      DataType childType;
      int childCapacity = capacity;
      if (type instanceof ArrayType) {
        childType = ((ArrayType)type).elementType();
      } else {
        childType = DataTypes.ByteType;
        childCapacity *= DEFAULT_ARRAY_LENGTH;
      }
      this.childColumns = new ColumnVector[1];
      this.childColumns[0] = ColumnVector.allocate(childCapacity, childType, memMode);
      this.resultArray = new Array(this.childColumns[0]);
      this.resultStruct = null;
    } else if (type instanceof StructType) {
      StructType st = (StructType)type;
      this.childColumns = new ColumnVector[st.fields().length];
      for (int i = 0; i < childColumns.length; ++i) {
        this.childColumns[i] = ColumnVector.allocate(capacity, st.fields()[i].dataType(), memMode);
      }
      this.resultArray = null;
      this.resultStruct = new ColumnarBatch.Row(this.childColumns);
    } else if (type instanceof CalendarIntervalType) {
      // Two columns. Months as int. Microseconds as Long.
      this.childColumns = new ColumnVector[2];
      this.childColumns[0] = ColumnVector.allocate(capacity, DataTypes.IntegerType, memMode);
      this.childColumns[1] = ColumnVector.allocate(capacity, DataTypes.LongType, memMode);
      this.resultArray = null;
      this.resultStruct = new ColumnarBatch.Row(this.childColumns);
    } else {
      this.childColumns = null;
      this.resultArray = null;
      this.resultStruct = null;
    }
  }
}
