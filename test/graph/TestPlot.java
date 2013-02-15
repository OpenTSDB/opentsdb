// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.graph;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class TestPlot {
  @Test
  public void testYRangeNull() {
    double[] actual = Plot.getBounds(null);
    assertEquals(Double.MIN_VALUE, actual[0], 0d);
    assertEquals(Double.MAX_VALUE, actual[1], 0d);
  }

  @Test
  public void testYRangeMatchesRegexButNotValidNumber() {
    double[] actual = Plot.getBounds("[0..0:]");
    assertEquals(Double.MIN_VALUE, actual[0], 0d);
    assertEquals(Double.MAX_VALUE, actual[1], 0d);
  }

  @Test
  public void testYRangeImpliedAutoMinMax() {
    double[] actual = Plot.getBounds("[:]");
    assertEquals(Double.MIN_VALUE, actual[0], 0d);
    assertEquals(Double.MAX_VALUE, actual[1], 0d);
  }

  @Test
  public void testYRangeNegative() {
    double[] actual = Plot.getBounds("[-10:]");
    assertEquals(-10d, actual[0], 0d);
    assertEquals(Double.MAX_VALUE, actual[1], 0d);
  }

  @Test
  public void testYRangeDecimal() {
    double[] actual = Plot.getBounds("[:-20.1]");
    assertEquals(Double.MIN_VALUE, actual[0], 0d);
    assertEquals(-20.1d, actual[1], 0.1d);
  }

  @Test
  public void testYRangeMinAndMax() {
    double[] actual = Plot.getBounds("[10:30]");
    assertEquals(10, actual[0], 0d);
    assertEquals(30, actual[1], 0d);
  }

  @Test
  public void testYRangeMin() {
    double[] actual = Plot.getBounds("[10:*]");
    assertEquals(10, actual[0], 0d);
    assertEquals(Double.MAX_VALUE, actual[1], 0d);
  }

  @Test
  public void testYRangeMax() {
    double[] actual = Plot.getBounds("[*:10]");
    assertEquals(Double.MIN_VALUE, actual[0], 0d);
    assertEquals(10, actual[1], 0d);
  }

  @Test
  public void testYRangeMinLowerBoundWoMinUpperBound() {
    double[] actual = Plot.getBounds("[-1.2 < *:10]");
    assertEquals(-1.2, actual[0], 0d);
    assertEquals(10, actual[1], 0d);
  }

  @Test
  public void testYRangeMinLowerBound() {
    double[] actual = Plot.getBounds("[-1.2 < * < 2.23:10]");
    assertEquals(-1.2, actual[0], 0d);
    assertEquals(10, actual[1], 0d);
  }

  @Test
  public void testYRangeNoUsefulBound() {
    double[] actual = Plot.getBounds("[* < 2.23:10 < *]");
    assertEquals(Double.MIN_VALUE, actual[0], 0d);
    assertEquals(Double.MAX_VALUE, actual[1], 0d);
  }

  @Test
  public void testYRangeMaxUpperBound() {
    double[] actual = Plot.getBounds("[* < 2.23:10 < * < 20]");
    assertEquals(Double.MIN_VALUE, actual[0], 0d);
    assertEquals(20, actual[1], 0d);
  }

  @Test
  public void testYRangeExcessWhitespace() {
    double[] actual = Plot.getBounds("[  -1  <  *  <  2  :  10  <  *  <  20  ]");
    assertEquals(-1, actual[0], 0d);
    assertEquals(20, actual[1], 0d);
  }

  @Test
  public void testYRangeNoWhitespace() {
    double[] actual = Plot.getBounds("[-1<*<2:10<*<20]");
    assertEquals(-1, actual[0], 0d);
    assertEquals(20, actual[1], 0d);
  }

  @Test
  public void testYRangeUnknownMin() {
    double[] actual = Plot.getBounds("[date_specification:20]");
    assertEquals(Double.MIN_VALUE, actual[0], 0d);
    assertEquals(20, actual[1], 0d);
  }

  @Test
  public void testYRangeUnknownMax() {
    double[] actual = Plot.getBounds("[20:date_specification]");
    assertEquals(20, actual[0], 0d);
    assertEquals(Double.MAX_VALUE, actual[1], 0d);
  }
}
