/**
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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncAbsDecimalToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncAbsDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncAbsLongToLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * GenericUDFAbs.
 *
 */
@Description(name = "enforce_constraint",
    value = "_FUNC_(x) - Internal check on scalar subquery expression to make sure atmost one row is returned",
    extended = "For internal use only")
@VectorizedExpressions({FuncAbsLongToLong.class, FuncAbsDoubleToDouble.class, FuncAbsDecimalToDecimal.class})
public class GenericUDFEnforceConstraint extends GenericUDF {
  private final BooleanWritable resultBool = new BooleanWritable();
  private transient BooleanObjectInspector boi;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length > 1) {
      throw new UDFArgumentLengthException(
          "Invalid number of arguments. Constraint enforcement UDF expected one argument but received: " + arguments.length);
    }

    boi = (BooleanObjectInspector) arguments[0];
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object a = arguments[0].get();
    boolean result = boi.get(a);

    if(result == false) {
      throw new UDFArgumentLengthException(
          "Constraint violated by a value!");
    }
    resultBool.set(true);
    return resultBool;
  }

  @Override
  protected String getFuncName() {
    return "enforce_constraint";
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

}
