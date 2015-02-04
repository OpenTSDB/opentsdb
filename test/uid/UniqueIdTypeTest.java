package net.opentsdb.uid;

import net.opentsdb.core.Const;
import org.junit.Test;

import static org.junit.Assert.*;

public class UniqueIdTypeTest {
  @Test
  public void stringToUniqueIdTypeMetric() throws Exception {
    assertEquals(UniqueIdType.METRIC, UniqueIdType.fromValue("Metric"));
  }

  @Test
  public void stringToUniqueIdTypeMetrics() throws Exception {
    assertEquals(UniqueIdType.METRIC, UniqueIdType.fromValue("MeTRIcs"));
  }

  @Test
  public void uniqueIdTypeMetricToValue() {
    assertEquals(Const.METRICS_QUAL, UniqueIdType.METRIC.toValue());
  }

  @Test
  public void stringToUniqueIdTypeTagk() throws Exception {
    assertEquals(UniqueIdType.TAGK, UniqueIdType.fromValue("TagK"));
  }

  @Test
  public void uniqueIdTypeTagKeyToValue() {
    assertEquals(Const.TAG_NAME_QUAL, UniqueIdType.TAGK.toValue());
  }

  @Test
  public void stringToUniqueIdTypeTagv() throws Exception {
    assertEquals(UniqueIdType.TAGV, UniqueIdType.fromValue("TagV"));
  }

  @Test
  public void uniqueIdTypeTagValueToValue() {
    assertEquals(Const.TAG_VALUE_QUAL, UniqueIdType.TAGV.toValue());
  }

  @Test (expected = NullPointerException.class)
  public void stringToUniqueIdTypeNull() throws Exception {
    UniqueIdType.fromValue(null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeEmpty() throws Exception {
    UniqueIdType.fromValue("");
  }

  @Test (expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeInvalid() throws Exception {
    UniqueIdType.fromValue("Not a type");
  }
}