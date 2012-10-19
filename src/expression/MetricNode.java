package net.opentsdb.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

  public ArithmeticNodeResult[] getDataPointsValues() {
    List<ArithmeticNodeResult> result = new ArrayList<ArithmeticNodeResult>();

    if (this.dataPoints != null) {
      for (DataPoints dataPoints : this.dataPoints) {
        final SeekableView seekableView = dataPoints.iterator();
        final ArithmeticNodeResult values = new ArithmeticNodeResult(
            getDataPointsName(dataPoints));

        while (seekableView.hasNext()) {
          final DataPoint dataPoint = seekableView.next();

          values.add(new TimestampValue(dataPoint.timestamp(), dataPoint
              .toDouble()));
        }

        result.add(values);
      }
    }

    return result.toArray(new ArithmeticNodeResult[] {});
  }

  private String getDataPointsName(DataPoints dataPoints) {
    StringBuilder result = new StringBuilder();

    result.append(dataPoints.metricName()).append("{");

    for (Map.Entry<String, String> tag : dataPoints.getTags().entrySet()) {
      result.append(tag.getKey()).append("=").append(tag.getValue())
          .append(", ");
    }

    if (result.length() > dataPoints.metricName().length() + 1) {
      result.delete(result.length() - 2, result.length());
    }

    result.append("}");

    return result.toString();
  }
}