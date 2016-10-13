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
package net.opentsdb.query.expression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import net.opentsdb.core.FillPolicy;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.query.expression.VariableIterator.SetOperator;

import org.apache.commons.jexl2.JexlException;
import org.hbase.async.Bytes;
import org.junit.Test;

public class TestExpressionIterator extends BaseTimeSyncedIteratorTest {

  @Test
  public void ctor() throws Exception {
    final ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, false, false);
    assertEquals(2, exp.getVariableNames().size());
    assertTrue(exp.getVariableNames().contains("a"));
    assertTrue(exp.getVariableNames().contains("b"));
    assertFalse(exp.getVariableNames().contains("+")); // I'm not a variable :(
    assertNull(exp.values());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNoVariables() throws Exception {
    new ExpressionIterator("ei", "1 + 1", SetOperator.INTERSECTION, false, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullExpression() throws Exception {
    new ExpressionIterator("ei", null, SetOperator.INTERSECTION, false, false);
  }
  
  @Test (expected = JexlException.class)
  public void ctorBadExpression() throws Exception {
    new ExpressionIterator("ei", " a / ", SetOperator.INTERSECTION, false, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyExpression() throws Exception {
    new ExpressionIterator("ei", "", SetOperator.INTERSECTION, false, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctorNullOperator() throws Exception {
    new ExpressionIterator("ei", "a + b", null, false, false);
  }
  
  @Test
  public void aPlusBWithTwoSeries() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    double[] values = new double[] { 12, 18 };
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(values[0], dps[0].toDouble(), 0.0001);
      assertEquals(values[1], dps[1].toDouble(), 0.0001);
      
      values[0] += 2;
      values[1] += 2;
      ts += 60000;
      its = exp.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void aMinusBWithTwoSeries() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a - b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(-10, dps[0].toDouble(), 0.0001);
      assertEquals(-10, dps[1].toDouble(), 0.0001);
      ts += 60000;
      its = exp.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void aTimesBWithTwoSeries() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a * b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(11, dps[0].toDouble(), 0.0001);
    assertEquals(56, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(24, dps[0].toDouble(), 0.0001);
    assertEquals(75, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(39, dps[0].toDouble(), 0.0001);
    assertEquals(96, dps[1].toDouble(), 0.0001);
    
    assertFalse(exp.hasNext());
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void aDivideBWithTwoSeries() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a / b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(0.0909, dps[0].toDouble(), 0.0001);
    assertEquals(0.2857, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(0.1666, dps[0].toDouble(), 0.0001);
    assertEquals(0.3333, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(0.2307, dps[0].toDouble(), 0.0001);
    assertEquals(0.375, dps[1].toDouble(), 0.0001);
    
    assertFalse(exp.hasNext());
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void aModBWithTwoSeries() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a % b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    double[] values = new double[] { 1, 4 };
    while (exp.hasNext()) {
      exp.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(values[0]++, dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, dps[1].toDouble(), 0.0001);
      ts += 60000;
      its = exp.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void aDivideByZeroWithTwoSeries() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
  
    // Jexl apparently happily allows this, just emits a zero
    ExpressionIterator exp = new ExpressionIterator("ei", "a / 0", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(0, dps[0].toDouble(), 0.0001);
      assertEquals(0, dps[1].toDouble(), 0.0001);
      ts += 60000;
      its = exp.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void doubleVariableAndPrecedence() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + (b * b)", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(122, dps[0].toDouble(), 0.0001);
    assertEquals(200, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(146, dps[0].toDouble(), 0.0001);
    assertEquals(230, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(172, dps[0].toDouble(), 0.0001);
    assertEquals(262, dps[1].toDouble(), 0.0001);
    
    assertFalse(exp.hasNext());
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void doubleVariableAndPrecedenceChanged() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "(a + b) * b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(132, dps[0].toDouble(), 0.0001);
    assertEquals(252, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(168, dps[0].toDouble(), 0.0001);
    assertEquals(300, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(208, dps[0].toDouble(), 0.0001);
    assertEquals(352, dps[1].toDouble(), 0.0001);
    
    assertFalse(exp.hasNext());
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }

  @Test
  public void aPlusScalarDropB() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + 1", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    double[] values = new double[] { 2, 5 };
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(values[0]++, dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, dps[1].toDouble(), 0.0001);
      ts += 60000;
      its = exp.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void missingRequiredVariable() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b + c", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
  }
  
  @Test
  public void aPlusBMissingPointsDefaultFillZero() throws Exception {
    threeSameEGaps();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(3, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    assertEquals(1, dps[0].toDouble(), 0.0001);
    assertEquals(4, dps[1].toDouble(), 0.0001);
    assertEquals(0, dps[2].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    assertEquals(0, dps[0].toDouble(), 0.0001);
    assertEquals(20, dps[1].toDouble(), 0.0001);
    assertEquals(8, dps[2].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    assertEquals(16, dps[0].toDouble(), 0.0001);
    assertEquals(0, dps[1].toDouble(), 0.0001);
    assertEquals(28, dps[2].toDouble(), 0.0001);
    
    assertFalse(exp.hasNext());
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("G"), dps[2].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void aPlusBMissingPointsFillOne() throws Exception {
    threeSameEGaps();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    iterators.get("a").setFillPolicy(new NumericFillPolicy(FillPolicy.SCALAR, 1));
    iterators.get("b").setFillPolicy(new NumericFillPolicy(FillPolicy.SCALAR, 1));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(3, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    assertEquals(2, dps[0].toDouble(), 0.0001);
    assertEquals(5, dps[1].toDouble(), 0.0001);
    assertEquals(2, dps[2].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    assertEquals(2, dps[0].toDouble(), 0.0001);
    assertEquals(20, dps[1].toDouble(), 0.0001);
    assertEquals(9, dps[2].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    assertEquals(16, dps[0].toDouble(), 0.0001);
    assertEquals(2, dps[1].toDouble(), 0.0001);
    assertEquals(28, dps[2].toDouble(), 0.0001);
    
    assertFalse(exp.hasNext());
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("G"), dps[2].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void aPlusBMissingPointsFillInfectiousNaN() throws Exception {
    threeSameEGaps();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    iterators.get("a").setFillPolicy(
        new NumericFillPolicy(FillPolicy.NOT_A_NUMBER, Double.NaN));
    iterators.get("b").setFillPolicy(
        new NumericFillPolicy(FillPolicy.NOT_A_NUMBER, Double.NaN));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(3, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    assertTrue(Double.isNaN(dps[0].toDouble()));
    assertTrue(Double.isNaN(dps[1].toDouble()));
    assertTrue(Double.isNaN(dps[2].toDouble()));
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    assertTrue(Double.isNaN(dps[0].toDouble()));
    assertEquals(20, dps[1].toDouble(), 0.0001);
    assertTrue(Double.isNaN(dps[2].toDouble()));
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    assertEquals(16, dps[0].toDouble(), 0.0001);
    assertTrue(Double.isNaN(dps[1].toDouble()));
    assertEquals(28, dps[2].toDouble(), 0.0001);
    
    assertFalse(exp.hasNext());
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("G"), dps[2].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void aPlusBResultsOffsetDefaultFill() throws Exception {
    timeOffset();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(1, dps[0].toDouble(), 0.0001);
    assertEquals(4, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(2, dps[0].toDouble(), 0.0001);
    assertEquals(5, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(13, dps[0].toDouble(), 0.0001);
    assertEquals(16, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(14, dps[0].toDouble(), 0.0001);
    assertEquals(17, dps[1].toDouble(), 0.0001);
    
    assertFalse(exp.hasNext());
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void aPlusBOneAggedOneTaggedUseQueryTagsWoutQueryTags() throws Exception {
    oneAggedTheOtherTagged();
    queryAB_AggAll();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, true, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(1, dps.length);
    // TODO - fix the TODO in the set operators to join tags
    //validateMeta(dps, true);
    
    long ts = 1431561600000L;
    double value = 13;
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(value, dps[0].toDouble(), 0.0001);
      
      value += 3;
      ts += 60000;
      its = exp.nextTimestamp();
    }

    // TODO - fix the TODO in the set operators to join tags
    //assertEquals(0, dps[0].tags().size());
    assertEquals(2, dps[0].aggregatedTags().size());
    assertTrue(dps[0].aggregatedTags().contains(TAGV_UIDS.get("D")));
    assertTrue(dps[0].aggregatedTags().contains(TAGV_UIDS.get("E")));
    // TODO - make sure the tags are empty once the expression data does it's
    // thing
    //assertTrue(dps[0].tags().isEmpty());
  }
  
  @Test
  public void singleNestedExpression() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator ei = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, false, false);
    ei.addResults("a", iterators.get("a"));
    ei.addResults("b", iterators.get("b"));
    ei.compile();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "x * 2", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("x", ei);
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    double[] values = new double[] { 24, 36 };
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(values[0], dps[0].toDouble(), 0.0001);
      assertEquals(values[1], dps[1].toDouble(), 0.0001);
      
      values[0] += 4;
      values[1] += 4;
      ts += 60000;
      its = exp.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void doubleNestedExpression() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator e1 = new ExpressionIterator("e1", "a + b", 
        SetOperator.INTERSECTION, false, false);
    e1.addResults("a", iterators.get("a"));
    e1.addResults("b", iterators.get("b"));
    e1.compile();
    
    ExpressionIterator e2 = new ExpressionIterator("e2", "e1 * 2", 
        SetOperator.INTERSECTION, false, false);
    e2.addResults("e1", e1);
    e2.compile();
    
    ExpressionIterator e3 = new ExpressionIterator("e3", "e2 * 2", 
        SetOperator.INTERSECTION, false, false);
    e3.addResults("e2", e2);
    
    e3.compile();
    final ExpressionDataPoint[] dps = e3.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    double[] values = new double[] { 48, 72 };
    long its = e3.nextTimestamp();
    while (e3.hasNext()) {
      e3.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(values[0], dps[0].toDouble(), 0.0001);
      assertEquals(values[1], dps[1].toDouble(), 0.0001);
      
      values[0] += 8;
      values[1] += 8;
      ts += 60000;
      its = e3.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test (expected = IllegalDataException.class)
  public void noIntersectionFound() throws Exception {
    threeDifE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addResultsMissingId() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b + c", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addResultsMissingSubQuery() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b + c", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addResultsMissingResults() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b + c", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
  }

  @Test
  public void unionOneExtraSeries() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.UNION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(3, dps.length);
    // TODO - fix the TODO in the set operators to join tags
    //validateMeta(dps, true);
    
    long ts = 1431561600000L;
    double[] values = new double[] { 12, 18, 17 };
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(ts, dps[2].timestamp());
      assertEquals(values[0], dps[0].toDouble(), 0.0001);
      assertEquals(values[1], dps[1].toDouble(), 0.0001);
      assertEquals(values[2], dps[2].toDouble(), 0.0001);
      
      values[0] += 2;
      values[1] += 2;
      values[2] += 1;
      ts += 60000;
      its = exp.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      // TODO - fix the TODO in the set operators to join tags
      //assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void unionOffset() throws Exception {
    timeOffset();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.UNION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
 
    long ts = 1431561600000L;
    long its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(1, dps[0].toDouble(), 0.0001);
    assertEquals(4, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(2, dps[0].toDouble(), 0.0001);
    assertEquals(5, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(13, dps[0].toDouble(), 0.0001);
    assertEquals(16, dps[1].toDouble(), 0.0001);
    ts += 60000;
    
    its = exp.nextTimestamp();
    exp.next(its);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(14, dps[0].toDouble(), 0.0001);
    assertEquals(17, dps[1].toDouble(), 0.0001);
    
    assertFalse(exp.hasNext());
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
    assertArrayEquals(TAGV_UIDS.get("D"), dps[0].tags().get(TAGV_UIDS.get("D")));
    assertArrayEquals(TAGV_UIDS.get("F"), dps[1].tags().get(TAGV_UIDS.get("D")));
  }
  
  @Test
  public void unionNoIntersection() throws Exception {
    threeDifE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.UNION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(6, dps.length);
    validateMeta(dps, false);
    
    long ts = 1431561600000L;
    double[] values = new double[] { 1, 11, 4, 14, 7, 17 };
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      for (int i = 0; i < values.length; i++) {
        assertEquals(ts, dps[i].timestamp());
        assertEquals(values[i], dps[i].toDouble(), 0.0001);
        ++values[i];
      }
      
      ts += 60000;
      its = exp.nextTimestamp();
    }
  }
  
  @Test
  public void unionSingleSeriesIteration() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.UNION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    double[] values = new double[] { 12, 18, 17 };
    
    for (int i = 0; i < dps.length; i++) {
      long ts = 1431561600000L;
      while (exp.hasNext(i)) {
        exp.next(i);
        assertEquals(ts, dps[i].timestamp());
        assertEquals(values[i], dps[i].toDouble(), 0.001);

        ts += 60000;
        if (i < dps.length - 1) {
          values[i] += 2;
        } else {
          values[i]++;
        }
      }
    }
  }
  
  @Test
  public void intersectionSingleSeriesIteration() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a + b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    double[] values = new double[] { 12, 18 };
    
    for (int i = 0; i < dps.length; i++) {
      long ts = 1431561600000L;
      while (exp.hasNext(i)) {
        exp.next(i);
        assertEquals(ts, dps[i].timestamp());
        assertEquals(values[i], dps[i].toDouble(), 0.001);
        ts += 60000;
        values[i] += 2;
      }
    }
    
  }
  
  @Test
  public void aGreaterThanb() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a > b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    double[] values = new double[] { 0, 0 };
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(values[0], dps[0].toDouble(), 0.0001);
      assertEquals(values[1], dps[1].toDouble(), 0.0001);
      
      ts += 60000;
      its = exp.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
  }
  
  @Test
  public void aLessThanb() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    remapResults();
    
    ExpressionIterator exp = new ExpressionIterator("ei", "a < b", 
        SetOperator.INTERSECTION, false, false);
    exp.addResults("a", iterators.get("a"));
    exp.addResults("b", iterators.get("b"));
    
    exp.compile();
    final ExpressionDataPoint[] dps = exp.values();
    assertEquals(2, dps.length);
    validateMeta(dps, true);
    
    long ts = 1431561600000L;
    double[] values = new double[] { 1, 1 };
    long its = exp.nextTimestamp();
    while (exp.hasNext()) {
      exp.next(its);
      
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(values[0], dps[0].toDouble(), 0.0001);
      assertEquals(values[1], dps[1].toDouble(), 0.0001);
      
      ts += 60000;
      its = exp.nextTimestamp();
    }
    
    for (int i = 0; i < dps.length; i++) {
      assertEquals(2, dps[i].tags().size());
      assertTrue(dps[i].aggregatedTags().isEmpty());
    }
  }
  
  /**
   * Makes sure the series contain both metrics
   * @param dps The results to validate
   * @param common_e The common e
   */
  private void validateMeta(final ExpressionDataPoint[] dps, 
      final boolean common_e) {
    for (int i = 0; i < dps.length; i++) {
      // TODO - change this guy to a byteset :( Since it's a bloody list we
      // can't do a "contains" because it checks for the address of the byte 
      // arrays
      boolean found = false;
      for (final byte[] metric : dps[i].metricUIDs()) {
        if (Bytes.memcmp(TAGV_UIDS.get("A"), metric) == 0) {
          found = true;
        } else if (Bytes.memcmp(TAGV_UIDS.get("B"), metric) == 0) {
          found = true;
          break;
        }
      }
      if (!found) {
        fail("Missing a metric");
      }
      
      if (common_e) {
        assertArrayEquals(TAGV_UIDS.get("E"), dps[i].tags().get(TAGV_UIDS.get("E")));
      }
    }
  }

  private void remapResults() {
    iterators.clear();
    iterators.put("a", new TimeSyncedIterator("a", 
        query.getQueries().get(0).getFilterTagKs(), results.get("0").getValue()));
    iterators.put("b", new TimeSyncedIterator("b", 
        query.getQueries().get(1).getFilterTagKs(), results.get("1").getValue()));
  }
}
