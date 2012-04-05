package net.opentsdb.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.SeekableView;

public class ArithmeticExpressionResultDataPoints implements DataPoints {
  private final String metricName;
  private final List<DataPoint> dataPoints = new ArrayList<DataPoint>();

  public ArithmeticExpressionResultDataPoints(final String metricName) {
    this.metricName = metricName;
  }

  public void add(DataPoint dataPoint) {
    dataPoints.add(dataPoint);
  }

  @Override
  public String metricName() {
    return metricName;
  }

  @Override
  public Map<String, String> getTags() {
    return new HashMap<String, String>();
  }

  @Override
  public List<String> getAggregatedTags() {
    return new ArrayList<String>();
  }

  @Override
  public int size() {
    return dataPoints.size();
  }

  @Override
  public int aggregatedSize() {
    return 0;
  }

  @Override
  public SeekableView iterator() {
    return new ArithmeticExpressionResultSeekableView(dataPoints);
  }

  @Override
  public long timestamp(int i) {
    return dataPoints.get(i).timestamp();
  }

  @Override
  public boolean isInteger(int i) {
    return dataPoints.get(i).isInteger();
  }

  @Override
  public long longValue(int i) {
    return dataPoints.get(i).longValue();
  }

  @Override
  public double doubleValue(int i) {
    return dataPoints.get(i).doubleValue();
  }

  private class ArithmeticExpressionResultSeekableView implements SeekableView {
    private final List<DataPoint> dataPoints;
    private int index = 0;

    public ArithmeticExpressionResultSeekableView(
        final List<DataPoint> dataPoints) {
      this.dataPoints = dataPoints;
    }

    @Override
    public boolean hasNext() {
      return (index + 1) < dataPoints.size();
    }

    @Override
    public DataPoint next() {
      DataPoint result = null;

      if (index + 1 == dataPoints.size()) {
        throw new NoSuchElementException(
            "no more elements in iterator (index = " + index + ")");
      } else {
        result = dataPoints.get(++index);
      }

      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void seek(long timestamp) {
      if (timestamp <= 0) {
        throw new IllegalArgumentException("timestamp must be positive: "
            + timestamp);
      } else {
        index = 0;

        while (hasNext() && next().timestamp() >= timestamp) {
          // do nothing, just let the time pass (the goal is to update the index)
        }
      }
    }
  }
}