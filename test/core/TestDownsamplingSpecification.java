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
package net.opentsdb.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDownsamplingSpecification {
  @Test
  public void testCtor() {
    final long interval = 1234567L;
    final Aggregator function = Aggregators.SUM;
    final FillPolicy fill_policy = FillPolicy.ZERO;

    final DownsamplingSpecification ds = new DownsamplingSpecification(
      interval, function, fill_policy);

    assertEquals(interval, ds.getInterval());
    assertEquals(function, ds.getFunction());
    assertEquals(fill_policy, ds.getFillPolicy());
  }

  @Test
  public void testStringCtor() {
    final DownsamplingSpecification ds = new DownsamplingSpecification(
      "15m-avg-nan");

    assertEquals(900000L, ds.getInterval());
    assertEquals(Aggregators.AVG, ds.getFunction());
    assertEquals(FillPolicy.NOT_A_NUMBER, ds.getFillPolicy());
  }

  @Test
  public void testToString() {
    assertEquals("DownsamplingSpecification{interval=4532019, function=zimsum, "
        + "fillPolicy=NOT_A_NUMBER}",
      new DownsamplingSpecification(
        4532019L,
        Aggregators.ZIMSUM,
        FillPolicy.NOT_A_NUMBER).toString());
  }

  @Test(expected = RuntimeException.class)
  public void testBadInterval() {
    new DownsamplingSpecification("blah-avg-lerp");
  }

  @Test(expected = RuntimeException.class)
  public void testBadFunction() {
    new DownsamplingSpecification("1m-hurp-lerp");
  }

  @Test(expected = RuntimeException.class)
  public void testBadFillPolicy() {
    new DownsamplingSpecification("10m-avg-max");
  }

  @Test (expected = IllegalArgumentException.class)
  public void testNoneAgg() {
    new DownsamplingSpecification("1m-none-lerp");
  }
}

