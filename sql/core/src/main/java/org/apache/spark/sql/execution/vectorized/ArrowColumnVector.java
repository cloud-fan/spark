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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

public class ArrowColumnVector extends ColumnVector {
  // All arrow column vectors share one allocator
  private static BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  public static class ArrowArray extends Array {
    private ListVector vector;
    public FieldReader data;
    private UInt4Vector offsets;

    protected ArrowArray(ListVector vector) {
      super(null);
      this.vector = vector;
      this.offsets = vector.getOffsetVector();
      this.data = vector.getDataVector().getReader();
    }

    @Override
    public int numElements() {
      return offsets.getAccessor().get(offset + 1) - offsets.getAccessor().get(offset);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return vector.getAccessor().isNull(ordinal);
    }

    private void setPosition(int ordinal) {
      data.setPosition(offsets.getAccessor().get(offset) + ordinal);
    }

    @Override
    public boolean getBoolean(int ordinal) {
      setPosition(ordinal);
      // todo: why this API has boxing?
      return data.readBoolean();
    }

    @Override
    public int getInt(int ordinal) {
      setPosition(ordinal);
      return data.readInteger();
    }

    private ArrowArray nestedArray;

    @Override
    public ArrayData getArray(int ordinal) {
      if (nestedArray == null) {
        nestedArray = new ArrowArray((ListVector) vector.getDataVector());
      }
      nestedArray.offset = offsets.getAccessor().get(offset) + ordinal;
      return nestedArray;
    }
  }

  private NullableBitVector boolData;
  private NullableBitVector.Mutator boolMutator;

  private NullableTinyIntVector byteData;
  private NullableTinyIntVector.Mutator byteMutator;

  private NullableIntVector intData;
  private NullableIntVector.Mutator intMutator;

  private NullableVarBinaryVector stringData;
  public NullableVarBinaryVector.Mutator stringMutator;

  public ListVector listData;
  private ArrowArray arrowArray;

  public ArrowColumnVector(int capacity, DataType type) {
    // todo: do not create childColumns
    super(capacity, type, MemoryMode.OFF_HEAP);
    reserveInternal(capacity);
  }

  @Override
  public Array getArray(int rowId) {
    arrowArray.offset = rowId;
    return arrowArray;
  }

  @Override
  public void close() {

  }

  private NullableVarBinaryHolder stringResult = new NullableVarBinaryHolder();

  @Override
  public UTF8String getUTF8String(int rowId) {
    stringData.getAccessor().get(rowId, stringResult);
    if (stringResult.isSet == 0) {
      return null;
    } else {
      return UTF8String.fromAddress(null,
        stringResult.buffer.memoryAddress() + stringResult.start,
        stringResult.end - stringResult.start);
    }
  }

  private ArrowType toArrowType(DataType dt) {
    if (dt == DataTypes.BooleanType) {
      return ArrowType.Bool.INSTANCE;
    } else if (dt == DataTypes.ByteType) {
      return new ArrowType.Int(8, true);
    } else if (dt == DataTypes.IntegerType) {
      return new ArrowType.Int(8 * 4, true);
    } else if (dt == DataTypes.StringType) {
      return ArrowType.Utf8.INSTANCE;
    } else {
      throw new IllegalStateException("unknown type: " + dt.simpleString());
    }
  }

  private List<Field> getFields(DataType dt) {
    List<Field> children = new ArrayList<>(1);
    Field f;
    if (dt instanceof ArrayType) {
      DataType et = ((ArrayType) dt).elementType();
      f = new Field("ArrayElement", FieldType.nullable(ArrowType.List.INSTANCE), getFields(et));
    } else {
      f = Field.nullable("ArrayElement", toArrowType(dt));
    }
    children.add(f);
    return children;
  }

  private void allocateListVector(ListVector vector, int capacity) {
    vector.setInitialCapacity(capacity);
    vector.allocateNew();
    if (vector.getDataVector() instanceof ListVector) {
      allocateListVector((ListVector) vector.getDataVector(), DEFAULT_ARRAY_LENGTH * capacity);
    }
  }

  @Override
  protected void reserveInternal(int capacity) {
    if (type instanceof ArrayType) {
      if (listData == null) {
        DataType et = ((ArrayType) type).elementType();
        listData = ListVector.empty("ListData", allocator);
        listData.initializeChildrenFromFields(getFields(et));
        allocateListVector(listData, capacity);
        arrowArray = new ArrowArray(listData);
      }
    } else if (type == DataTypes.BooleanType) {
      if (boolData == null) {
        boolData = new NullableBitVector("BoolData", allocator);
        boolData.setInitialCapacity(capacity);
        boolData.allocateNew();
        boolMutator = boolData.getMutator();
      }
    } else if (type == DataTypes.ByteType) {
      if (byteData == null) {
        byteData = new NullableTinyIntVector("ByteData", allocator);
        byteData.setInitialCapacity(capacity);
        byteData.allocateNew();
        byteMutator = byteData.getMutator();
      }
    } else if (type == DataTypes.IntegerType) {
      if (intData == null) {
        intData = new NullableIntVector("IntData", allocator);
        intData.setInitialCapacity(capacity);
        intData.allocateNew();
        intMutator = intData.getMutator();
      }
    } else if (type == DataTypes.StringType) {
      if (stringData == null) {
        stringData = new NullableVarBinaryVector("StringData", allocator);
        stringData.setInitialCapacity(capacity);
        stringData.allocateNew();
        stringMutator = stringData.getMutator();
      }
    }
  }

  @Override
  public long nullsNativeAddress() {
    return 0;
  }

  @Override
  public long valuesNativeAddress() {
    return 0;
  }

  @Override
  public void putNotNull(int rowId) {

  }

  @Override
  public void putNull(int rowId) {
    // todo: why the setNull is not in the Mutator interface?
    if (boolMutator != null) {
      boolMutator.setNull(rowId);
    } else if (byteMutator != null) {
      byteMutator.setNull(rowId);
    } else if (listData != null) {
      listData.getWriter().writeNull();
    }
  }

  @Override
  public void putNulls(int rowId, int count) {

  }

  @Override
  public void putNotNulls(int rowId, int count) {

  }

  @Override
  public boolean isNullAt(int rowId) {
    return false;
  }

  @Override
  public void putBoolean(int rowId, boolean value) {
    int data = 0;
    if (value) {
      data = 1;
    }
    boolMutator.set(rowId, data);
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    // todo: does Arrow has batch put API?
  }

  @Override
  public boolean getBoolean(int rowId) {
    return boolData.getAccessor().get(rowId) == 1;
  }

  @Override
  public void putByte(int rowId, byte value) {
    byteMutator.set(rowId, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {

  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public byte getByte(int rowId) {
    return byteData.getAccessor().get(rowId);
  }

  @Override
  public void putShort(int rowId, short value) {

  }

  @Override
  public void putShorts(int rowId, int count, short value) {

  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {

  }

  @Override
  public short getShort(int rowId) {
    return 0;
  }

  @Override
  public void putInt(int rowId, int value) {
    intMutator.set(rowId, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {

  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {

  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public int getInt(int rowId) {
    return intData.getAccessor().get(rowId);
  }

  @Override
  public int getDictId(int rowId) {
    return 0;
  }

  @Override
  public void putLong(int rowId, long value) {

  }

  @Override
  public void putLongs(int rowId, int count, long value) {

  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {

  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public long getLong(int rowId) {
    return 0;
  }

  @Override
  public void putFloat(int rowId, float value) {

  }

  @Override
  public void putFloats(int rowId, int count, float value) {

  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {

  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public float getFloat(int rowId) {
    return 0;
  }

  @Override
  public void putDouble(int rowId, double value) {

  }

  @Override
  public void putDoubles(int rowId, int count, double value) {

  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {

  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public double getDouble(int rowId) {
    return 0;
  }

  @Override
  public void putArray(int rowId, int offset, int length) {

  }

  @Override
  public int getArrayLength(int rowId) {
    return 0;
  }

  @Override
  public int getArrayOffset(int rowId) {
    return 0;
  }

  @Override
  public void loadBytes(Array array) {

  }

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int count) {
    return 0;
  }
}
