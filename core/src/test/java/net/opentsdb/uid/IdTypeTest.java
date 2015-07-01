package net.opentsdb.uid;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class IdTypeTest {
  @Test
  public void stringToUniqueIdTypeMetric() throws Exception {
    assertEquals(IdType.METRIC, IdType.fromValue("Metric"));
  }

  @Test
  public void stringToUniqueIdTypeTagk() throws Exception {
    assertEquals(IdType.TAGK, IdType.fromValue("TagK"));
  }

  @Test
  public void uniqueIdTypeTagKeyToValue() {
    assertEquals("tagk", IdType.TAGK.toValue());
  }

  @Test
  public void stringToUniqueIdTypeTagv() throws Exception {
    assertEquals(IdType.TAGV, IdType.fromValue("TagV"));
  }

  @Test
  public void uniqueIdTypeTagValueToValue() {
    assertEquals("tagv", IdType.TAGV.toValue());
  }

  @Test(expected = NullPointerException.class)
  public void stringToUniqueIdTypeNull() throws Exception {
    IdType.fromValue(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeEmpty() throws Exception {
    IdType.fromValue("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeInvalid() throws Exception {
    IdType.fromValue("Not a type");
  }
}
