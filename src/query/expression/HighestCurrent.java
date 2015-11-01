// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import net.opentsdb.core.AggregationIterator;
import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.Aggregators.Interpolation;

public class HighestCurrent implements Expression {

  @Override
  public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults,
                               List<String> params) {
    if (queryResults == null || queryResults.isEmpty()) {
      throw new NullPointerException("Query results cannot be empty");
    }

    if (params == null || params.isEmpty()) {
      throw new NullPointerException("Need aggregation window for moving average");
    }

    String param = params.get(0);
    if (param == null || param.length() == 0) {
      throw new NullPointerException("Invalid window='" + param + "'");
    }

    int k = Integer.parseInt(param.trim());

    int size = 0;
    for (DataPoints[] results: queryResults) {
      size = size + results.length;
    }

    PostAggregatedDataPoints[] seekablePoints = new PostAggregatedDataPoints[size];
    int ix=0;
    for (DataPoints[] results: queryResults) {
      for (DataPoints dpoints: results) {
        List<DataPoint> mutablePoints = new ArrayList<DataPoint>();
        for (DataPoint point: dpoints) {
          mutablePoints.add(point.isInteger() ?
                  MutableDataPoint.ofLongValue(point.timestamp(), point.longValue())
                  : MutableDataPoint.ofDoubleValue(point.timestamp(), point.doubleValue()));
        }
        seekablePoints[ix++] = new PostAggregatedDataPoints(dpoints,
                mutablePoints.toArray(new DataPoint[mutablePoints.size()]));
      }
    }

    if (k >= size) {
      return seekablePoints;
    }

    SeekableView[] views = new SeekableView[size];
    for (int i=0; i<size; i++) {
      views[i] = seekablePoints[i].iterator();
    }

    MaxLatestAggregator aggregator = new
            MaxLatestAggregator(Aggregators.Interpolation.LERP,
            "maxLatest", size, data_query.startTime(), data_query.endTime());

    SeekableView view = (new AggregationIterator(views,
            data_query.startTime(), data_query.endTime(),
            aggregator, Aggregators.Interpolation.LERP, false));

    // slurp all the points
    while (view.hasNext()) {
      DataPoint mdp = view.next();
      Object o = mdp.isInteger() ? mdp.longValue() : mdp.doubleValue();
    }

    long[] maxLongs = aggregator.getLongMaxes();
    double[] maxDoubles = aggregator.getDoubleMaxes();
    Entry[] maxesPerTS = new Entry[size];
    if (aggregator.hasDoubles() && aggregator.hasLongs()) {
      for (int i=0; i<size; i++) {
        maxesPerTS[i] = new Entry(Math.max((double)maxLongs[i], maxDoubles[i]), i);
      }
    } else if (aggregator.hasLongs() && !aggregator.hasDoubles()) {
      for (int i=0; i<size; i++) {
        maxesPerTS[i] = new Entry((double) maxLongs[i], i);
      }
    } else if (aggregator.hasDoubles() && !aggregator.hasLongs()) {
      for (int i=0; i<size; i++) {
        maxesPerTS[i] = new Entry(maxDoubles[i], i);
      }
    }

    Arrays.sort(maxesPerTS, new Comparator<Entry>() {
      @Override
      public int compare(Entry o1, Entry o2) {
        return -1 * Double.compare(o1.val, o2.val);
      }
    });

    DataPoints[] results = new DataPoints[k];
    for (int i=0; i<k; i++) {
      results[i] = seekablePoints[maxesPerTS[i].pos];
    }

    return results;
  }

  class Entry {
    public Entry(double val, int pos) {
      this.val = val;
      this.pos = pos;
    }
    double val;
    int pos;
  }

  @Override
  public String writeStringField(List<String> queryParams, String innerExpression) {
    return "highestCurrent(" + innerExpression + ")";
  }

  
  public static class MaxLatestAggregator extends Aggregator {
    private final int size;
    private final long[] maxLongs;
    private final double[] maxDoubles;
    private final long start;
    private final long end;
    private boolean hasLongs = false;
    private boolean hasDoubles = false;
    private long latestTS = -1;

    public MaxLatestAggregator(Interpolation method, String name, int size,
                               long startTimeInMillis, long endTimeInMillis) {
      super(method, name);
      this.size = size;
      this.start = startTimeInMillis;
      this.end = endTimeInMillis;


      this.maxLongs = new long[size];
      this.maxDoubles = new double[size];

      for (int i=0; i<size; i++) {
        maxDoubles[i] = Double.MIN_VALUE;
        maxLongs[i] = Long.MIN_VALUE;
      }
    }

    @Override
    public long runLong(Longs values) {
      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        //data point falls outside required range
        if (ts < start || ts > end) {
          return 0;
        }
      }

      long[] longs = new long[size];
      int ix = 0;
      longs[ix++] = values.nextLongValue();
      while (values.hasNextValue()) {
        longs[ix++] = values.nextLongValue();
      }

      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        if (ts > latestTS) {
          System.arraycopy(longs, 0, maxLongs, 0, size);
        }
      }

      hasLongs = true;
      return 0;
    }

    @Override
    public double runDouble(Doubles values) {
      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        //data point falls outside required range
        if (ts < start || ts > end) {
          return 0;
        }
      }

      double[] doubles = new double[size];
      int ix = 0;
      doubles[ix++] = values.nextDoubleValue();
      while (values.hasNextValue()) {
        doubles[ix++] = values.nextDoubleValue();
      }

      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        if (ts > latestTS) {
          System.arraycopy(doubles, 0, maxDoubles, 0, size);
        }
      }

      hasDoubles = true;
      return 0;
    }

    public long[] getLongMaxes() {
      return maxLongs;
    }

    public double[] getDoubleMaxes() {
      return maxDoubles;
    }

    public boolean hasLongs() {
      return hasLongs;
    }

    public boolean hasDoubles() {
      return hasDoubles;
    }
    
  }
}
