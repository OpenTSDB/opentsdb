package net.opentsdb.query.expression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.opentsdb.core.AggregationIterator;
import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.Aggregators.Interpolation;

import com.google.common.collect.Lists;

public class MovingAverage implements Expression {

  @Override
  public DataPoints[] evaluate(final TSQuery data_query, List<DataPoints[]> queryResults, List<String> params) {
    if (queryResults == null || queryResults.isEmpty()) {
      return new DataPoints[]{};
    }

    if (params == null || params.isEmpty()) {
      throw new NullPointerException("Need aggregation window for moving average");
    }

    String param = params.get(0);
    if (param == null || param.length() == 0) {
      throw new NullPointerException("Invalid window='" + param + "'");
    }

    param = param.trim();

    long numPoints = -1;
    boolean isTimeUnit = false;
    if (param.matches("[0-9]+")) {
      numPoints = Integer.parseInt(param);
    } else if (param.startsWith("'") && param.endsWith("'")) {
      numPoints = parseParam(param);
      isTimeUnit = true;
    }

    if (numPoints <= 0) {
      throw new RuntimeException("numPoints <= 0");
    }

    int size = 0;
    for (DataPoints[] results: queryResults) {
      size = size + results.length;
    }
    
    PostAggregatedDataPoints[] seekablePoints = new PostAggregatedDataPoints[size];
    int ix=0;
    // one or more queries (m=...&m=...&m=...)
    for (DataPoints[] results: queryResults) {
      // group bys (m=sum:foo{host=*})
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
    
    SeekableView[] views = new SeekableView[size];
    for (int i=0; i<size; i++) {
      views[i] = seekablePoints[i].iterator();
    }
    
    SeekableView view = new AggregationIterator(views,
            data_query.startTime(), data_query.endTime(),
            new MovingAverageAggregator(Aggregators.Interpolation.LERP, "movingAverage", numPoints, isTimeUnit),
            Aggregators.Interpolation.LERP, false);
    
    List<DataPoint> points = Lists.newArrayList();
    while (view.hasNext()) {
      DataPoint mdp = view.next();
      points.add(mdp.isInteger() ?
              MutableDataPoint.ofLongValue(mdp.timestamp(), mdp.longValue()) :
              MutableDataPoint.ofDoubleValue(mdp.timestamp(), mdp.doubleValue()));
    }

    if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
      return new DataPoints[]{new PostAggregatedDataPoints(queryResults.get(0)[0],
              points.toArray(new DataPoint[points.size()]))};
    } else {
      return new DataPoints[]{};
    }
  }

  public long parseParam(String param) {
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
    String unit = param.substring(tuIndex+1, param.length() - 1);

    if ("min".equals(unit)) {
      return TimeUnit.MILLISECONDS.convert(time, TimeUnit.MINUTES);
    } else if ("hr".equals(unit)) {
      return TimeUnit.MILLISECONDS.convert(time, TimeUnit.HOURS);
    } else if ("sec".equals(unit)) {
      return TimeUnit.MILLISECONDS.convert(time, TimeUnit.SECONDS);
    } else {
      throw new RuntimeException("unknown time unit=" + unit);
    }

  }

  @Override
  public String writeStringField(List<String> queryParams, String innerExpression) {
    return "movingAverage(" + innerExpression + ")";
  }

  static final class MovingAverageAggregator extends Aggregator {
    private LinkedList<SumPoint> list = new LinkedList<SumPoint>();
    private final long numPoints;
    private final boolean isTimeUnit;
    
    public MovingAverageAggregator(final Interpolation method, final String name, long numPoints, boolean isTimeUnit) {
      super(method, name);
      this.numPoints = numPoints;
      this.isTimeUnit = isTimeUnit;
    }

    @Override
    public long runLong(final Longs values) {
      long sum = values.nextLongValue();
      while (values.hasNextValue()) {
        sum += values.nextLongValue();
      }

      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        list.addFirst(new SumPoint(ts, sum));
      }

      long result=0; int count=0;

      Iterator<SumPoint> iter = list.iterator();
      SumPoint first = iter.next();
      boolean conditionMet = false;

      // now sum up the preceeding points
      while(iter.hasNext()) {
        SumPoint next = iter.next();
        result += (Long) next.val;
        count++;
        if (!isTimeUnit && count >= numPoints) {
          conditionMet = true;
          break;
        } else if (isTimeUnit && ((first.ts - next.ts) > numPoints)) {
          conditionMet = true;
          break;
        }
      }

      if (!conditionMet || count == 0) {
        return 0;
      }

      return result/count;
    }

    @Override
    public double runDouble(Doubles values) {
      double sum = values.nextDoubleValue();
      while (values.hasNextValue()) {
        sum += values.nextDoubleValue();
      }

      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        list.addFirst(new SumPoint(ts, sum));
      }

      double result=0; int count=0;

      Iterator<SumPoint> iter = list.iterator();
      SumPoint first = iter.next();
      boolean conditionMet = false;

      // now sum up the preceeding points
      while(iter.hasNext()) {
        SumPoint next = iter.next();
        result += (Double) next.val;
        count++;
        if (!isTimeUnit && count >= numPoints) {
          conditionMet = true;
          break;
        } else if (isTimeUnit && ((first.ts - next.ts) > numPoints)) {
          conditionMet = true;
          break;
        }
      }

      if (!conditionMet || count == 0) {
        return 0;
      }

      return result/count;
    }

    class SumPoint {
      long ts;
      Object val;
      public SumPoint(long ts, Object val) {
        this.ts = ts;
        this.val = val;
      }
    }
  }
}
