package net.opentsdb.expression;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.SeekableView;

public class MetricNode extends ArithmeticNode {
  private static final long serialVersionUID = 6594055430692570904L;

  private String name;
  private DataPoints[] dataPoints;

  public MetricNode(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public DataPoints[] getDataPoints() {
    return dataPoints;
  }

  public void setDataPoints(DataPoints[] dataPoints) {
    this.dataPoints = dataPoints;
  }

  public List<TimestampValue> getDataPointsValues() {
    List<TimestampValue> result = new ArrayList<TimestampValue>();

    for (DataPoints dataPoints : this.dataPoints) {
      final SeekableView seekableView = dataPoints.iterator();

      while (seekableView.hasNext()) {
        final DataPoint dataPoint = seekableView.next();

        result.add(new TimestampValue(dataPoint.timestamp(), dataPoint
            .toDouble()));
      }
    }

    return result;
  }
}