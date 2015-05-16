package net.opentsdb.utils;

import net.opentsdb.utils.MoreLongs;
import org.junit.Test;

import static org.junit.Assert.*;

public class MoreLongsTest {
  @Test
  public void parseLongSimple() {
    assertEquals(0, MoreLongs.parseLong("0"));
    assertEquals(0, MoreLongs.parseLong("+0"));
    assertEquals(0, MoreLongs.parseLong("-0"));
    assertEquals(1, MoreLongs.parseLong("1"));
    assertEquals(1, MoreLongs.parseLong("+1"));
    assertEquals(-1, MoreLongs.parseLong("-1"));
    assertEquals(4242, MoreLongs.parseLong("4242"));
    assertEquals(4242, MoreLongs.parseLong("+4242"));
    assertEquals(-4242, MoreLongs.parseLong("-4242"));
  }

  @Test
  public void parseLongMaxValue() {
    assertEquals(Long.MAX_VALUE, MoreLongs.parseLong(Long.toString(Long.MAX_VALUE)));
  }

  @Test
  public void parseLongMinValue() {
    assertEquals(Long.MIN_VALUE, MoreLongs.parseLong(Long.toString(Long.MIN_VALUE)));
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongEmptyString() {
    MoreLongs.parseLong("");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongMalformed() {
    MoreLongs.parseLong("42a51");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongMalformedPlus() {
    MoreLongs.parseLong("+");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongMalformedMinus() {
    MoreLongs.parseLong("-");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongValueTooLarge() {
    MoreLongs.parseLong("18446744073709551616");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongValueTooLargeSubtle() {
    MoreLongs.parseLong("9223372036854775808"); // MAX_VALUE + 1
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongValueTooSmallSubtle() {
    MoreLongs.parseLong("-9223372036854775809"); // MIN_VALUE - 1
  }
}