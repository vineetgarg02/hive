/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;

public class UDFMaskVectorLong extends UDFMaskBaseVector {

  public UDFMaskVectorLong(int colNum, int outputColumnNum) {
    super(colNum, outputColumnNum);
  }

  public UDFMaskVectorLong() {
    super();
  }

  public void transform(ColumnVector outputColVector, ColumnVector inputVector, int idx) {
    boolean isBooleanType =
        this.getInputTypeInfos()[0].getTypeName().equals(serdeConstants.BOOLEAN_TYPE_NAME);

    // boolean data type is represented as long and masking is not supported for boolean
    if(isBooleanType) {
      outputColVector.isNull[idx] = true;
      outputColVector.noNulls = false;
      return;
    }
    outputColVector.isNull[idx] = false;

    long[] inputCol = ((LongColumnVector)inputVector).vector;
    long[] outputCol = ((LongColumnVector)outputColVector).vector;

    long value = inputCol[idx];
    long val = value;

    if(value < 0) {
      val *= -1;
    }

    long ret = 0;
    long pos = 1;
    for(int i = 0; val != 0; i++) {
      ret += maskedNumber * pos;

      val /= 10;
      pos *= 10;
    }

    if(value < 0) {
      ret *= -1;
    }
    outputCol[idx] = ret;
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, getColNum());
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
