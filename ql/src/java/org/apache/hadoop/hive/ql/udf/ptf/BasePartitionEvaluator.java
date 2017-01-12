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
package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.LeadLagInfo;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum.GenericUDAFSumEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * This class is mostly used for RANGE windowing type to do some optimization. ROWS
 * windowing type can support streaming.
 *
 */
public class BasePartitionEvaluator {
  protected final GenericUDAFEvaluator wrappedEvaluator;
  protected final WindowFrameDef winFrame;
  protected final PTFPartition partition;
  protected final List<PTFExpressionDef> parameters;
  protected final ObjectInspector outputOI;

  /**
   * Internal class to represent a window range in a partition by searching the
   * relative position (ROWS) or relative value (RANGE) of the current row
   */
  protected static class Range
  {
    int start;
    int end;
    PTFPartition p;

    public Range(int start, int end, PTFPartition p)
    {
      this.start = start;
      this.end = end;
      this.p = p;
    }

    public PTFPartitionIterator<Object> iterator()
    {
      return p.range(start, end);
    }

    public int getDiff(Range prevRange) {
      return this.start - prevRange.start + this.end - prevRange.end;
    }

    public int getSize() {
      return end - start;
    }
  }

  public BasePartitionEvaluator(
      GenericUDAFEvaluator wrappedEvaluator,
      WindowFrameDef winFrame,
      PTFPartition partition,
      List<PTFExpressionDef> parameters,
      ObjectInspector outputOI) {
    this.wrappedEvaluator = wrappedEvaluator;
    this.winFrame = winFrame;
    this.partition = partition;
    this.parameters = parameters;
    this.outputOI = outputOI;
  }

  /**
   * Get the aggregation for the whole partition. Used in the case where windowing
   * is unbounded or the function value is calculated based on all the rows in the
   * partition such as percent_rank().
   * @return the aggregated result
   * @throws HiveException
   */
  public Object getPartitionAgg() throws HiveException {
    return calcFunctionValue(partition.iterator(), null);
  }

  /**
   * Given the current row, get the aggregation for the window
   *
   * @throws HiveException
   */
  public Object iterate(int currentRow, LeadLagInfo leadLagInfo) throws HiveException {
    Range range = getRange(winFrame, currentRow, partition);
    PTFPartitionIterator<Object> pItr = range.iterator();
    return calcFunctionValue(pItr, leadLagInfo);
  }

  /**
   * Given a partition iterator, calculate the function value
   * @param pItr  the partition pointer
   * @return      the function value
   * @throws HiveException
   */
  protected Object calcFunctionValue(PTFPartitionIterator<Object> pItr, LeadLagInfo leadLagInfo)
      throws HiveException {
    // To handle the case like SUM(LAG(f)) over(), aggregation function includes
    // LAG/LEAD call
    PTFOperator.connectLeadLagFunctionsToPartition(leadLagInfo, pItr);

    AggregationBuffer aggBuffer = wrappedEvaluator.getNewAggregationBuffer();
    Object[] argValues = new Object[parameters == null ? 0 : parameters.size()];
    while(pItr.hasNext())
    {
      Object row = pItr.next();
      int i = 0;
      if ( parameters != null ) {
        for(PTFExpressionDef param : parameters)
        {
          argValues[i++] = param.getExprEvaluator().evaluate(row);
        }
      }
      wrappedEvaluator.aggregate(aggBuffer, argValues);
    }

    // The object is reused during evaluating, make a copy here
    return ObjectInspectorUtils.copyToStandardObject(wrappedEvaluator.evaluate(aggBuffer), outputOI);
  }

  protected static Range getRange(WindowFrameDef winFrame, int currRow, PTFPartition p)
      throws HiveException {
    BoundaryDef startB = winFrame.getStart();
    BoundaryDef endB = winFrame.getEnd();

    int start, end;
    if (winFrame.getWindowType() == WindowType.ROWS) {
      start = getRowBoundaryStart(startB, currRow);
      end = getRowBoundaryEnd(endB, currRow, p);
    } else {
      ValueBoundaryScanner vbs = ValueBoundaryScanner.getScanner(winFrame);
      start = vbs.computeStart(currRow, p);
      end = vbs.computeEnd(currRow, p);
    }
    start = start < 0 ? 0 : start;
    end = end > p.size() ? p.size() : end;
    return new Range(start, end, p);
  }

  private static int getRowBoundaryStart(BoundaryDef b, int currRow) throws HiveException {
    Direction d = b.getDirection();
    int amt = b.getAmt();
    switch(d) {
    case PRECEDING:
      if (amt == BoundarySpec.UNBOUNDED_AMOUNT) {
        return 0;
      }
      else {
        return currRow - amt;
      }
    case CURRENT:
      return currRow;
    case FOLLOWING:
      return currRow + amt;
    }
    throw new HiveException("Unknown Start Boundary Direction: " + d);
  }

  private static int getRowBoundaryEnd(BoundaryDef b, int currRow, PTFPartition p) throws HiveException {
    Direction d = b.getDirection();
    int amt = b.getAmt();
    switch(d) {
    case PRECEDING:
      if ( amt == 0 ) {
        return currRow + 1;
      }
      return currRow - amt + 1;
    case CURRENT:
      return currRow + 1;
    case FOLLOWING:
      if (amt == BoundarySpec.UNBOUNDED_AMOUNT) {
        return p.size();
      }
      else {
        return currRow + amt + 1;
      }
    }
    throw new HiveException("Unknown End Boundary Direction: " + d);
  }

  /**
   * The base type for sum operator evaluator when a partition data is available
   * and streaming process is not possible. Some optimization can be done for such
   * case.
   *
   */
  public static abstract class SumPartitionEvaluator<ResultType extends Writable> extends BasePartitionEvaluator {
    protected final WindowSumAgg<ResultType> sumAgg;

    public SumPartitionEvaluator(
        GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame,
        PTFPartition partition,
        List<PTFExpressionDef> parameters,
        ObjectInspector outputOI) {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
      sumAgg = new WindowSumAgg<ResultType>();
    }

    static class WindowSumAgg<ResultType> extends AbstractAggregationBuffer {
      Range prevRange;
      ResultType prevSum;
      boolean empty;
    }

    public abstract ResultType add(ResultType t1, ResultType t2);
    public abstract ResultType minus(ResultType t1, ResultType t2);

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object iterate(int currentRow, LeadLagInfo leadLagInfo) throws HiveException {
      // Currently sum(distinct) not supported in PartitionEvaluator
      if (((GenericUDAFSumEvaluator)wrappedEvaluator).isWindowingDistinct()) {
        return super.iterate(currentRow, leadLagInfo);
      }

      Range currentRange = getRange(winFrame, currentRow, partition);
      ResultType result;
      if (currentRow == 0 ||  // Reset for the new partition
          sumAgg.prevRange == null ||
          currentRange.getSize() <= currentRange.getDiff(sumAgg.prevRange)) {
        result = (ResultType)calcFunctionValue(currentRange.iterator(), leadLagInfo);
        sumAgg.prevRange = currentRange;
        sumAgg.empty = false;
        sumAgg.prevSum = result;
      } else {
        // Given the previous range and the current range, calculate the new sum
        // from the previous sum and the difference to save the computation.
        Range r1 = new Range(sumAgg.prevRange.start, currentRange.start, partition);
        Range r2 = new Range(sumAgg.prevRange.end, currentRange.end, partition);
        ResultType sum1 = (ResultType)calcFunctionValue(r1.iterator(), leadLagInfo);
        ResultType sum2 = (ResultType)calcFunctionValue(r2.iterator(), leadLagInfo);
        result = add(minus(sumAgg.prevSum, sum1), sum2);
        sumAgg.prevRange = currentRange;
        sumAgg.prevSum = result;
      }

      return result;
    }
  }

  public static class SumPartitionDoubleEvaluator extends SumPartitionEvaluator<DoubleWritable> {
    public SumPartitionDoubleEvaluator(GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame, PTFPartition partition,
        List<PTFExpressionDef> parameters, ObjectInspector outputOI) {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
    }

    @Override
    public DoubleWritable add(DoubleWritable t1, DoubleWritable t2) {
      if (t1 == null && t2 == null) return null;
      return new DoubleWritable((t1 == null ? 0 : t1.get()) + (t2 == null ? 0 : t2.get()));
    }

    @Override
    public DoubleWritable minus(DoubleWritable t1, DoubleWritable t2) {
      if (t1 == null && t2 == null) return null;
      return new DoubleWritable((t1 == null ? 0 : t1.get()) - (t2 == null ? 0 : t2.get()));
    }
  }

  public static class SumPartitionLongEvaluator extends SumPartitionEvaluator<LongWritable> {
    public SumPartitionLongEvaluator(GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame, PTFPartition partition,
        List<PTFExpressionDef> parameters, ObjectInspector outputOI) {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
    }

    @Override
    public LongWritable add(LongWritable t1, LongWritable t2) {
      if (t1 == null && t2 == null) return null;
      return new LongWritable((t1 == null ? 0 : t1.get()) + (t2 == null ? 0 : t2.get()));
    }

    @Override
    public LongWritable minus(LongWritable t1, LongWritable t2) {
      if (t1 == null && t2 == null) return null;
      return new LongWritable((t1 == null ? 0 : t1.get()) - (t2 == null ? 0 : t2.get()));
    }
  }

  public static class SumPartitionHiveDecimalEvaluator extends SumPartitionEvaluator<HiveDecimalWritable> {
    public SumPartitionHiveDecimalEvaluator(GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame, PTFPartition partition,
        List<PTFExpressionDef> parameters, ObjectInspector outputOI) {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
    }

    @Override
    public HiveDecimalWritable add(HiveDecimalWritable t1, HiveDecimalWritable t2) {
      if (t1 == null && t2 == null) return null;
      if (t1 == null) {
        return t2;
      } else {
        if (t2 != null) {
          t1.mutateAdd(t2);
        }
        return t1;
      }
    }

    @Override
    public HiveDecimalWritable minus(HiveDecimalWritable t1, HiveDecimalWritable t2) {
      if (t1 == null && t2 == null) return null;
      if (t1 == null) {
        t2.mutateNegate();
        return t2;
      } else {
        if (t2 != null) {
          t1.mutateSubtract(t2);
        }
        return t1;
      }
    }
  }
}