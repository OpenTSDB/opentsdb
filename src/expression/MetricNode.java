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
    this.name = name.replaceAll("\\\"", "");
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

  public TimestampValues[] getDataPointsValues() {
    List<TimestampValues> result = new ArrayList<TimestampValues>();

    if (this.dataPoints != null) {
      for (DataPoints dataPoints : this.dataPoints) {
        final SeekableView seekableView = dataPoints.iterator();
        final TimestampValues values = new TimestampValues();

        while (seekableView.hasNext()) {
          final DataPoint dataPoint = seekableView.next();

          values.add(new TimestampValue(dataPoint.timestamp(), dataPoint
              .toDouble()));
        }

        result.add(values);
      }
    }

    return result.toArray(new TimestampValues[] {});
  }
}