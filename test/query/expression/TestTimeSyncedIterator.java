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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.FillPolicy;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
public class TestTimeSyncedIterator extends BaseTimeSyncedIteratorTest {

  @Test
  public void ctor() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    
    assertEquals(3, it.size());
    assertTrue(it.hasNext());
    assertEquals(0, it.getIndex());
    assertEquals("0", it.getId());
    assertEquals(results.get("0").getKey().getFilterTagKs(), it.getQueryTagKs());
    assertTrue(results.get("0").getValue() == it.getDataPoints());
    assertEquals(3, it.values().length);
    assertEquals(1, it.getQueryTagKs().size());
    assertEquals(FillPolicy.ZERO, it.getFillPolicy().getPolicy());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullId() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    new TimeSyncedIterator(null, 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyId() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    new TimeSyncedIterator("", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
  }
  
  @Test
  public void ctorNullQueryTags() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", null, 
        results.get("0").getValue());
    
    assertEquals(3, it.size());
    assertTrue(it.hasNext());
    assertEquals(0, it.getIndex());
    assertEquals("0", it.getId());
    assertNull(it.getQueryTagKs());
    assertTrue(results.get("0").getValue() == it.getDataPoints());
    assertEquals(3, it.values().length);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullDPs() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    new TimeSyncedIterator("0", results.get("0").getKey().getFilterTagKs(), 
        null);
  }
  
  @Test
  public void threeSeries() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    double[] values = new double[] { 1, 4, 7 };
    while (it.hasNext()) {
      final DataPoint[] dps = it.next(ts);
      assertEquals(3, dps.length);
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(ts, dps[2].timestamp());
      assertEquals(values[0]++, dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, dps[1].toDouble(), 0.0001);
      assertEquals(values[2]++, dps[2].toDouble(), 0.0001);
      ts = it.nextTimestamp();
    }
    assertEquals(Long.MAX_VALUE, ts);
  }
  
  @Test
  public void threeSeriesEmitter() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    final DataPoint[] dps = it.values();
    double[] values = new double[] { 1, 4, 7 };
    while (it.hasNext()) {
      it.next(ts);
      assertEquals(3, dps.length);
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(ts, dps[2].timestamp());
      assertEquals(values[0]++, dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, dps[1].toDouble(), 0.0001);
      assertEquals(values[2]++, dps[2].toDouble(), 0.0001);
      ts = it.nextTimestamp();
    }
    assertEquals(Long.MAX_VALUE, ts);
  }
  
  @Test
  public void nullASeries() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    // mimic the intersector kicking out a series
    it.nullIterator(0);
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    final DataPoint[] dps = it.values();
    double[] values = new double[] { 1, 4, 7 };
    while (it.hasNext()) {
      it.next(ts);
      assertEquals(3, dps.length);
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(ts, dps[2].timestamp());
      assertEquals(0, dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, dps[1].toDouble(), 0.0001);
      assertEquals(values[2]++, dps[2].toDouble(), 0.0001);
      ts = it.nextTimestamp();
    }
    assertEquals(Long.MAX_VALUE, ts);
  }
  
  @Test
  public void nullAll() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    // mimic the intersector kicking out all series.
    it.nullIterator(0);
    it.nullIterator(1);
    it.nullIterator(2);

    assertEquals(Long.MAX_VALUE, it.nextTimestamp());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void singleSeries() throws Exception {
    threeDisjointSameE();
    queryA_DD();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    double value = 1;
    while (it.hasNext()) {
      final DataPoint[] dps = it.next(ts);
      assertEquals(1, dps.length);
      assertEquals(ts, dps[0].timestamp());
      assertEquals(value++, dps[0].toDouble(), 0.0001);
      ts = it.nextTimestamp();
    }
    assertEquals(Long.MAX_VALUE, ts);
  }
  
  @Test
  public void noData() throws Exception {
    setDataPointStorage();
    queryA_DD();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    
    assertEquals(0, it.values().length);
    assertEquals(Long.MAX_VALUE, it.nextTimestamp());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void threeSeriesMissing() throws Exception {
    threeSameEGaps();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    it.setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER));
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    DataPoint[] dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(1, dps[0].toDouble(), 0.0001);
    assertEquals(4, dps[1].toDouble(), 0.0001);
    assertTrue(Double.isNaN(dps[2].toDouble()));
    ts = it.nextTimestamp();
    
    dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertTrue(Double.isNaN(dps[0].toDouble()));
    assertEquals(5, dps[1].toDouble(), 0.0001);
    assertEquals(8, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
    
    dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(3, dps[0].toDouble(), 0.0001);
    assertTrue(Double.isNaN(dps[1].toDouble()));
    assertEquals(9, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
  }
  
  @Test
  public void threeSeriesMissingFillZero() throws Exception {
    threeSameEGaps();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    it.setFillPolicy(new NumericFillPolicy(FillPolicy.ZERO));
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    DataPoint[] dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(1, dps[0].toDouble(), 0.0001);
    assertEquals(4, dps[1].toDouble(), 0.0001);
    assertEquals(0, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
    
    dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(0, dps[0].toDouble(), 0.0001);
    assertEquals(5, dps[1].toDouble(), 0.0001);
    assertEquals(8, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
    
    dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(3, dps[0].toDouble(), 0.0001);
    assertEquals(0, dps[1].toDouble(), 0.0001);
    assertEquals(9, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
  }
  
  @Test
  public void threeSeriesMissingNull() throws Exception {
    threeSameEGaps();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    it.setFillPolicy(new NumericFillPolicy(FillPolicy.NULL));
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    DataPoint[] dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(1, dps[0].toDouble(), 0.0001);
    assertEquals(4, dps[1].toDouble(), 0.0001);
    assertTrue(Double.isNaN(dps[2].toDouble()));
    ts = it.nextTimestamp();
    
    dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertTrue(Double.isNaN(dps[0].toDouble()));
    assertEquals(5, dps[1].toDouble(), 0.0001);
    assertEquals(8, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
    
    dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(3, dps[0].toDouble(), 0.0001);
    assertTrue(Double.isNaN(dps[1].toDouble()));
    assertEquals(9, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
  }
  
  @Test
  public void threeSeriesMissingScalar() throws Exception {
    threeSameEGaps();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    it.setFillPolicy(new NumericFillPolicy(FillPolicy.SCALAR, 42));
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    DataPoint[] dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(1, dps[0].toDouble(), 0.0001);
    assertEquals(4, dps[1].toDouble(), 0.0001);
    assertEquals(42, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
    
    dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(42, dps[0].toDouble(), 0.0001);
    assertEquals(5, dps[1].toDouble(), 0.0001);
    assertEquals(8, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
    
    dps = it.next(ts);
    assertEquals(ts, dps[0].timestamp());
    assertEquals(ts, dps[1].timestamp());
    assertEquals(ts, dps[2].timestamp());
    
    assertEquals(3, dps[0].toDouble(), 0.0001);
    assertEquals(42, dps[1].toDouble(), 0.0001);
    assertEquals(9, dps[2].toDouble(), 0.0001);
    ts = it.nextTimestamp();
  }
  
  @Test
  public void failToNextTS() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    int i = 0;
    boolean broke = false;
    while (it.hasNext()) {
      // in this case the caller fails to advance the TS so we keep getting the
      // same data points over and over again.
      it.next(ts);
      ++i;
      if (i > 100) {
        broke = true;
        break;
      }
    }
    if (!broke) {
      fail("Expected to iterate over 100 times");
    }
  }

  @Test
  public void nextTimestampDoesntAdvance() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    
    assertEquals(1431561600000L, it.nextTimestamp());
    assertEquals(1431561600000L, it.nextTimestamp());
    assertEquals(1431561600000L, it.nextTimestamp());
    assertEquals(1431561600000L, it.nextTimestamp());
    assertEquals(1431561600000L, it.nextTimestamp());
    assertEquals(1431561600000L, it.nextTimestamp());
    assertEquals(1431561600000L, it.nextTimestamp());
  }
  
  @Test
  public void nextExceptionNoException() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    it.next(it.nextTimestamp());
    it.next(it.nextTimestamp());
    it.next(it.nextTimestamp());
    it.next(it.nextTimestamp());
    assertEquals(Long.MAX_VALUE, it.nextTimestamp());
  }
  
  @Test
  public void getCopy() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    final TimeSyncedIterator it = new TimeSyncedIterator("0", 
        results.get("0").getKey().getFilterTagKs(), 
        results.get("0").getValue());
    
    final ITimeSyncedIterator copy = it.getCopy();
    assertTrue(copy != it);
    
    long ts = it.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    double[] values = new double[] { 1, 4, 7 };
    while (it.hasNext()) {
      final DataPoint[] dps = it.next(ts);
      assertEquals(3, dps.length);
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(ts, dps[2].timestamp());
      assertEquals(values[0]++, dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, dps[1].toDouble(), 0.0001);
      assertEquals(values[2]++, dps[2].toDouble(), 0.0001);
      ts = it.nextTimestamp();
    }
    assertEquals(Long.MAX_VALUE, ts);
    
    ts = copy.nextTimestamp();
    assertEquals(1431561600000L, ts);
    
    values = new double[] { 1, 4, 7 };
    while (copy.hasNext()) {
      final DataPoint[] dps = copy.next(ts);
      assertEquals(3, dps.length);
      assertEquals(ts, dps[0].timestamp());
      assertEquals(ts, dps[1].timestamp());
      assertEquals(ts, dps[2].timestamp());
      assertEquals(values[0]++, dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, dps[1].toDouble(), 0.0001);
      assertEquals(values[2]++, dps[2].toDouble(), 0.0001);
      ts = copy.nextTimestamp();
    }
    assertEquals(Long.MAX_VALUE, ts);
  }
}
