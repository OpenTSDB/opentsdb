// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data.types.numeric.aggregators;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType;
import org.apache.commons.math3.util.ResizableDoubleArray;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.exceptions.IllegalDataException;

public class PercentilesFactories {

  public static class P999Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "P999";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return P999;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class P99Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "P99";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return P99;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class P95Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "P95";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return P95;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class P90Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "P90";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return P90;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class P75Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "P75";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return P75;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class P50Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "P50";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return P50;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP999R3Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP999R3";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP999R3;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP99R3Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP99R3";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP99R3;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP95R3Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP95R3";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP95R3;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP90R3Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP90R3";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP90R3;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP75R3Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP75R3";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP75R3;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP50R3Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP50R3";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP50R3;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP999R7Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP999R7";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP999R7;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP99R7Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP99R7";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP99R7;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP95R7Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP95R7";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP95R7;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP90R7Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP90R7";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP90R7;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP75R7Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP75R7";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP75R7;
    }

    @Override
    public String type() {
      return TYPE;
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  public static class EP50R7Factory extends BaseTSDBPlugin implements 
      NumericAggregatorFactory {
    public static final String TYPE = "EP50R7";
    
    @Override
    public NumericAggregator newAggregator(boolean infectious_nan) {
      return EP50R7;
    }

    @Override
    public String type() {
      return TYPE;
    }

    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
      this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
      return Deferred.fromResult(null);
    }
    
    @Override
    public String version() {
      return "3.0.0";
    }
    
  }
  
  private static final class PercentileAgg extends BaseNumericAggregator {
    private final Double percentile;
    private final EstimationType estimation;

    public PercentileAgg(final Double percentile, final String name) {
        this(percentile, name, null);
    }

    public PercentileAgg(final Double percentile, final String name, 
        final EstimationType est) {
      super(name);
      Preconditions.checkArgument(percentile > 0 && percentile <= 100, 
          "Invalid percentile value");
      this.percentile = percentile;
      this.estimation = est;
    }

    @Override
    public void run(final long[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final MutableNumericValue dp) {
      if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      
      final Percentile percentile =
        this.estimation == null
            ? new Percentile(this.percentile)
            : new Percentile(this.percentile).withEstimationType(estimation);
      final ResizableDoubleArray local_values = new ResizableDoubleArray();
      for (int i = start_offset; i < end_offset; i++) {
        local_values.addElement(values[i]);
      }
      percentile.setData(local_values.getElements());
      final double p = percentile.evaluate();
      if (p % 1 == 0) {
        dp.resetValue((long) p);
      } else {
        dp.resetValue(p);
      }
    }

    @Override
    public void run(final double[] values, 
                    final int start_offset,
                    final int end_offset, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (end_offset < 1) {
        throw new IllegalDataException("End offset must be greater than 0");
      }
      
      final Percentile percentile = new Percentile(this.percentile);
      final ResizableDoubleArray local_values = new ResizableDoubleArray();
      int nans = 0;
      for (int i = start_offset; i < end_offset; i++) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          nans++;
          continue;
        }
        local_values.addElement(values[i]);
      }
      if (nans == end_offset - start_offset || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        percentile.setData(local_values.getElements());
        dp.resetValue(percentile.evaluate());
      }
    }

  }

  private static final NumericAggregator P999 = new PercentileAgg(99.9d, "p9999");
  private static final NumericAggregator P99 = new PercentileAgg(99d, "p999");
  private static final NumericAggregator P95 = new PercentileAgg(95d, "p95");
  private static final NumericAggregator P90 = new PercentileAgg(90d, "p90");
  private static final NumericAggregator P75 = new PercentileAgg(75d, "p75");
  private static final NumericAggregator P50 = new PercentileAgg(50d, "p50");
  
  private static final NumericAggregator EP999R3 = new PercentileAgg(
      99.9d, "ep999r3", EstimationType.R_3);
  private static final NumericAggregator EP99R3 = new PercentileAgg(
      99d, "ep99r3", EstimationType.R_3);
  private static final NumericAggregator EP95R3 = new PercentileAgg(
      95d, "ep95r3", EstimationType.R_3);
  private static final NumericAggregator EP90R3 = new PercentileAgg(
      90d, "ep90r3", EstimationType.R_3);
  private static final NumericAggregator EP75R3 = new PercentileAgg(
      75d, "ep75r3", EstimationType.R_3);
  private static final NumericAggregator EP50R3 = new PercentileAgg(
      50d, "ep50r3", EstimationType.R_3);
  
  private static final NumericAggregator EP999R7 = new PercentileAgg(
      99.9d, "ep999r7", EstimationType.R_7);
  private static final NumericAggregator EP99R7 = new PercentileAgg(
      99d, "ep99r7", EstimationType.R_7);
  private static final NumericAggregator EP95R7 = new PercentileAgg(
      95d, "ep95r7", EstimationType.R_7);
  private static final NumericAggregator EP90R7 = new PercentileAgg(
      90d, "ep90r7", EstimationType.R_7);
  private static final NumericAggregator EP75R7 = new PercentileAgg(
      75d, "ep75r7", EstimationType.R_7);
  private static final NumericAggregator EP50R7 = new PercentileAgg(
      50d, "ep50r7", EstimationType.R_7);
}
