package net.opentsdb.query.expression;
/**
 * Copyright 2015 The opentsdb Authors
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import net.opentsdb.core.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TimeShift implements Expression {
  /**
   * in place modify of TsdbResult array to increase timestamps by timeshift
   * @param data_query
   * @param results
   * @param params
   * @return
   */
  @Override
  public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> results, List<String> params) {
    //not 100% sure what to do here -> do I need to think of the case where I have no data points
    if(results == null || results.isEmpty()) {
      return new DataPoints[]{};
    }
    if(params == null || results.isEmpty()) {
      throw new IllegalArgumentException("Need amount of timeshift to perform timeshift");
    }

    String param = params.get(0);
    if (param == null || param.length() == 0) {
      throw new IllegalArgumentException("Invalid timeshift='" + param + "'");
    }

    param = param.trim();

    long timeshift = -1;
    if (param.startsWith("'") && param.endsWith("'")) {
      timeshift = parseParam(param) / 1000;
    } else {
      throw new RuntimeException("Invalid timeshift parameter: eg '10min'");
    }

    if (timeshift <= 0) {
      throw new RuntimeException("timeshift <= 0");
    }

    DataPoints[] inputPoints = results.get(0);
    DataPoints[] outputPoints = new DataPoints[inputPoints.length];
    for(int n = 0; n < inputPoints.length; n++) {
      outputPoints[n] = shift(inputPoints[n], timeshift);
    }
    return outputPoints;
  }

  public static long parseParam(String param) {
    char[] chars = param.toCharArray();
    int tuIndex = 0;
    for (int c = 1; c < chars.length; c++) {
      if (Character.isDigit(chars[c])) {
        tuIndex++;
      } else {
        break;
      }
    }

    if (tuIndex == 0) {
      throw new RuntimeException("Invalid Parameter: " + param);
    }

    int time = Integer.parseInt(param.substring(1, tuIndex + 1));
    String unit = param.substring(tuIndex + 1, param.length() - 1);
    if ("sec".equals(unit)) {
      return TimeUnit.MILLISECONDS.convert(time, TimeUnit.SECONDS);
    } else if ("min".equals(unit)) {
      return TimeUnit.MILLISECONDS.convert(time, TimeUnit.MINUTES);
    } else if ("hr".equals(unit)) {
      return TimeUnit.MILLISECONDS.convert(time, TimeUnit.HOURS);
    } else if ("day".equals(unit) || "days".equals(unit)) {
      return TimeUnit.MILLISECONDS.convert(time, TimeUnit.DAYS);
    } else if ("week".equals(unit) || "weeks".equals(unit)) {
      //didn't have week so small cheat here
      return TimeUnit.MILLISECONDS.convert(time*7, TimeUnit.DAYS);
    }
    else {
      throw new RuntimeException("unknown time unit=" + unit);
    }
  }

  /**
   * Adjusts the timestamp of each datapoint by timeshift
   * @param points The data points to factor
   * @param timeshift The factor to multiply by
   * @return The resulting data points
   */
  private DataPoints shift(final DataPoints points, final long timeshift) {
    // TODO(cl) - Using an array as the size function may not return the exact
    // results and we should figure a way to avoid copying data anyway.
    final List<DataPoint> dps = new ArrayList<DataPoint>();
    final boolean shift_is_int = (timeshift == Math.floor(timeshift)) &&
            !Double.isInfinite(timeshift);
    final SeekableView view = points.iterator();
    while (view.hasNext()) {
      DataPoint pt = view.next();
      if (shift_is_int) {
        dps.add(MutableDataPoint.ofLongValue(pt.timestamp() + timeshift,
                pt.longValue()));
      } else {
        // NaNs are fine here, they'll just be re-computed as NaN
        dps.add(MutableDataPoint.ofDoubleValue(pt.timestamp() + timeshift,
                timeshift * pt.toDouble()));
      }
    }
    final DataPoint[] results = new DataPoint[dps.size()];
    dps.toArray(results);
    return new PostAggregatedDataPoints(points, results);
  }

    @Override
  public String writeStringField(List<String> params, String inner_expression) {
      return "timeshift(" + inner_expression + ")";
  }
}
