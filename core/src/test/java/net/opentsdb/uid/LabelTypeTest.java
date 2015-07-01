package net.opentsdb.uid;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LabelTypeTest {
  @Test
  public void stringToUniqueIdTypeMetric() throws Exception {
    assertEquals(LabelType.METRIC, LabelType.fromValue("Metric"));
  }

  @Test
  public void stringToUniqueIdTypeTagk() throws Exception {
    assertEquals(LabelType.TAGK, LabelType.fromValue("TagK"));
  }

  @Test
  public void uniqueIdTypeTagKeyToValue() {
    assertEquals("tagk", LabelType.TAGK.toValue());
  }

  @Test
  public void stringToUniqueIdTypeTagv() throws Exception {
    assertEquals(LabelType.TAGV, LabelType.fromValue("TagV"));
  }

  @Test
  public void uniqueIdTypeTagValueToValue() {
    assertEquals("tagv", LabelType.TAGV.toValue());
  }

  @Test(expected = NullPointerException.class)
  public void stringToUniqueIdTypeNull() throws Exception {
    LabelType.fromValue(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeEmpty() throws Exception {
    LabelType.fromValue("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeInvalid() throws Exception {
    LabelType.fromValue("Not a type");
  }
}
