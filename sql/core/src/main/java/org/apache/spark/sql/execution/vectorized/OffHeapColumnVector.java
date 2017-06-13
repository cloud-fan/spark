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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * Column data backed using offheap memory.
 */
public final class OffHeapColumnVector extends ColumnVector {

  private static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  // The data stored in these two allocations need to maintain binary compatible. We can
  // directly pass this buffer to external components.
  private long nulls;
  // The actually data of this column vector will be stored here. If it's an array column vector,
  // we will store the offsets and lengths here, and store the element data in child column vector.
  private long data;

  protected OffHeapColumnVector(int capacity, DataType type) {
    super(capacity, type, MemoryMode.OFF_HEAP);

    nulls = 0;
    data = 0;

    reserveInternal(capacity);
    reset();
  }

  public long valuesNativeAddress() {
    return data;
  }

  @Override
  public Object binaryBaseObject() {
    return null;
  }

  @Override
  public long binaryBaseOffset() {
    return data;
  }

  @Override
  public void close() {
    Platform.freeMemory(nulls);
    Platform.freeMemory(data);
    nulls = 0;
    data = 0;
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    Platform.putByte(null, nulls + rowId, (byte) 0);
  }

  @Override
  public void putNull(int rowId) {
    Platform.putByte(null, nulls + rowId, (byte) 1);
    ++numNulls;
    anyNullsSet = true;
  }

  @Override
  public void putNulls(int rowId, int count) {
    long offset = nulls + rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      Platform.putByte(null, offset, (byte) 1);
    }
    anyNullsSet = true;
    numNulls += count;
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    if (!anyNullsSet) return;
    long offset = nulls + rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      Platform.putByte(null, offset, (byte) 0);
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    return Platform.getByte(null, nulls + rowId) == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    Platform.putByte(null, data + rowId, (byte)((value) ? 1 : 0));
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    byte v = (byte)((value) ? 1 : 0);
    for (int i = 0; i < count; ++i) {
      Platform.putByte(null, data + rowId + i, v);
    }
  }

  @Override
  public boolean getBoolean(int rowId) { return Platform.getByte(null, data + rowId) == 1; }

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    Platform.putByte(null, data + rowId, value);

  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    for (int i = 0; i < count; ++i) {
      Platform.putByte(null, data + rowId + i, value);
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null, data + rowId, count);
  }

  @Override
  public void putBytes(int rowId, Object base, long offset, int length) {
    Platform.copyMemory(base, offset, null, data + rowId, length);
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      return Platform.getByte(null, data + rowId);
    } else {
      return (byte) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  //
  // APIs dealing with shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    Platform.putShort(null, data + 2 * rowId, value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    long offset = data + 2 * rowId;
    for (int i = 0; i < count; ++i, offset += 2) {
      Platform.putShort(null, offset, value);
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.SHORT_ARRAY_OFFSET + srcIndex * 2,
        null, data + 2 * rowId, count * 2);
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      return Platform.getShort(null, data + 2 * rowId);
    } else {
      return (short) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  //
  // APIs dealing with ints
  //

  @Override
  public void putInt(int rowId, int value) {
    Platform.putInt(null, data + 4 * rowId, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    long offset = data + 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putInt(null, offset, value);
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4,
        null, data + 4 * rowId, count * 4);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
          null, data + 4 * rowId, count * 4);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = data + 4 * rowId;
      for (int i = 0; i < count; ++i, offset += 4, srcOffset += 4) {
        Platform.putInt(null, offset,
            java.lang.Integer.reverseBytes(Platform.getInt(src, srcOffset)));
      }
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      return Platform.getInt(null, data + 4 * rowId);
    } else {
      return dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
    }
  }

  /**
   * Returns the dictionary Id for rowId.
   * This should only be called when the ColumnVector is dictionaryIds.
   * We have this separate method for dictionaryIds as per SPARK-16928.
   */
  public int getDictId(int rowId) {
    assert(dictionary == null)
            : "A ColumnVector dictionary should not have a dictionary for itself.";
    return Platform.getInt(null, data + 4 * rowId);
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(null, data + 8 * rowId, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    long offset = data + 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      Platform.putLong(null, offset, value);
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET + srcIndex * 8,
        null, data + 8 * rowId, count * 8);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
          null, data + 8 * rowId, count * 8);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = data + 8 * rowId;
      for (int i = 0; i < count; ++i, offset += 8, srcOffset += 8) {
        Platform.putLong(null, offset,
            java.lang.Long.reverseBytes(Platform.getLong(src, srcOffset)));
      }
    }
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      return Platform.getLong(null, data + 8 * rowId);
    } else {
      return dictionary.decodeToLong(dictionaryIds.getDictId(rowId));
    }
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    Platform.putFloat(null, data + rowId * 4, value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    long offset = data + 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putFloat(null, offset, value);
    }
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.FLOAT_ARRAY_OFFSET + srcIndex * 4,
        null, data + 4 * rowId, count * 4);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
          null, data + rowId * 4, count * 4);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = data + 4 * rowId;
      for (int i = 0; i < count; ++i, offset += 4) {
        Platform.putFloat(null, offset, bb.getFloat(srcIndex + (4 * i)));
      }
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      return Platform.getFloat(null, data + rowId * 4);
    } else {
      return dictionary.decodeToFloat(dictionaryIds.getDictId(rowId));
    }
  }


  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    Platform.putDouble(null, data + rowId * 8, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    long offset = data + 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      Platform.putDouble(null, offset, value);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8,
      null, data + 8 * rowId, count * 8);
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
        null, data + rowId * 8, count * 8);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = data + 8 * rowId;
      for (int i = 0; i < count; ++i, offset += 8) {
        Platform.putDouble(null, offset, bb.getDouble(srcIndex + (8 * i)));
      }
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      return Platform.getDouble(null, data + rowId * 8);
    } else {
      return dictionary.decodeToDouble(dictionaryIds.getDictId(rowId));
    }
  }

  // Split out the slow path.
  @Override
  protected void reserveInternal(int newCapacity) {
    int oldCapacity = (this.data == 0L) ? 0 : capacity;
    if (this.resultArray != null) {
      // need a long as offset and length for each array.
      this.data = Platform.reallocateMemory(data, oldCapacity * 8, newCapacity * 8);
    } else if (type instanceof ByteType || type instanceof BooleanType) {
      this.data = Platform.reallocateMemory(data, oldCapacity, newCapacity);
    } else if (type instanceof ShortType) {
      this.data = Platform.reallocateMemory(data, oldCapacity * 2, newCapacity * 2);
    } else if (type instanceof IntegerType || type instanceof FloatType ||
        type instanceof DateType || DecimalType.is32BitDecimalType(type)) {
      this.data = Platform.reallocateMemory(data, oldCapacity * 4, newCapacity * 4);
    } else if (type instanceof LongType || type instanceof DoubleType ||
        DecimalType.is64BitDecimalType(type) || type instanceof TimestampType) {
      this.data = Platform.reallocateMemory(data, oldCapacity * 8, newCapacity * 8);
    } else if (resultStruct != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
    this.nulls = Platform.reallocateMemory(nulls, oldCapacity, newCapacity);
    Platform.setMemory(nulls + oldCapacity, (byte)0, newCapacity - oldCapacity);
    capacity = newCapacity;
  }
}
