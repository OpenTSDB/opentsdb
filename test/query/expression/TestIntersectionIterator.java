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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.hbase.async.HBaseClient;
import org.hbase.async.Bytes.ByteMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.FillPolicy;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.storage.MockBase;
import net.opentsdb.utils.ByteSet;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TimeSyncedIterator.class })
public class TestIntersectionIterator extends BaseTimeSyncedIteratorTest  {

  /** used for the flattenTags tests */
  private static final byte[] UID1 = new byte[] { 0, 0, 1 };
  private static final byte[] UID2 = new byte[] { 0, 0, 2 };
  private static final byte[] UID3 = new byte[] { 0, 0, 3 };  
  private ByteMap<byte[]> tags;
  private ByteSet agg_tags;
  private ITimeSyncedIterator sub;
  private ByteSet query_tags;
  
  @Before
  public void beforeLocal() throws Exception {
    tags = new ByteMap<byte[]>();
    tags.put(UID1, UID1);
    tags.put(UID2, UID2);
    
    agg_tags = new ByteSet();
    agg_tags.add(UID3);
    
    sub = mock(ITimeSyncedIterator.class);
    query_tags = new ByteSet();
    query_tags.add(UID1);
    when(sub.getQueryTagKs()).thenReturn(query_tags);
  }

  @Test (expected = NullPointerException.class)
  public void ctorNullResults() {
    new IntersectionIterator("it", null, true, true);
  }
  
  @Test
  public void ctorEmptyResults() {
    final IntersectionIterator it = new IntersectionIterator("it",
        new HashMap<String, ITimeSyncedIterator>(), true, true);
    assertEquals(0, it.getSeriesSize());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void twoAndThreeSeries() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(2, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 1, 4, 11, 14 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(2, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(values[0]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, set_dps[1].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(2, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(values[2]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[3]++, set_dps[1].toDouble(), 0.0001);

      ts += 60000;
    }
  }
  
  @Test
  public void twoAndThreeSeriesExtraDPinKickedSeries() throws Exception {
    // in this case we want to make sure the kicked series doesn't cause us
    // to dump a bunch of nulls
    oneExtraSameE();
    queryAB_Dstar();
    
    final HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561630, 14, tags).joinUninterruptibly();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(2, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 1, 4, 11, 14 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(2, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(values[0]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, set_dps[1].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(2, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(values[2]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[3]++, set_dps[1].toDouble(), 0.0001);

      ts += 60000;
    }
  }
  
  @Test
  public void threeSeriesIntersectToTwo() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(2, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 1, 7, 11, 17 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(2, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(values[0]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, set_dps[1].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(2, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(values[2]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[3]++, set_dps[1].toDouble(), 0.0001);

      ts += 60000;
    }
  }
  
  @Test
  public void threeSeriesIntersectToExtraDPsinKicked() throws Exception {
    reduceToOne();
    queryAB_Dstar();
    
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561630, 1024, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "Q");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561630, 1024, tags).joinUninterruptibly();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(1, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 7, 17 };
    while (it.hasNext()) {
      it.next();

      DataPoint[] set_dps = dps.get("0");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[0]++, set_dps[0].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[1]++, set_dps[0].toDouble(), 0.0001);

      ts += 60000;
    }
  }
  
  @Test
  public void threeSeriesIntersectToOne() throws Exception {
    reduceToOne();
    queryAB_Dstar();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(1, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 7, 17 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[0]++, set_dps[0].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[1]++, set_dps[0].toDouble(), 0.0001);

      ts += 60000;
    }
  }
  
  @Test
  public void threeSeriesAggedIntoOne() throws Exception {
    threeSameE();
    queryAB_AggAll();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(1, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 12, 42 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[0], set_dps[0].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[1], set_dps[0].toDouble(), 0.0001);
      
      values[0] += 3;
      values[1] += 3;

      ts += 60000;
    }
  }
  
  @Test
  public void threeSeriesFullIntersetWithNaNs() throws Exception {
    threeSameEGaps();
    queryAB_Dstar();
    for (ITimeSyncedIterator iterator : iterators.values()) {
      iterator.setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER));
    }
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(3, it.getSeriesSize());
    
    long ts = 1431561600000L;
    it.next();
    
    DataPoint[] set_dps = dps.get("0");
    assertEquals(3, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(ts, set_dps[2].timestamp());
    assertEquals(1, set_dps[0].toDouble(), 0.0001);
    assertEquals(4, set_dps[1].toDouble(), 0.0001);
    assertTrue(Double.isNaN(set_dps[2].toDouble()));
    
    // whole series is NaN'd
    set_dps = dps.get("1");
    assertEquals(3, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(ts, set_dps[2].timestamp());
    assertTrue(Double.isNaN(set_dps[0].toDouble()));
    assertTrue(Double.isNaN(set_dps[1].toDouble()));
    assertTrue(Double.isNaN(set_dps[2].toDouble()));

    ts += 60000;
    it.next();
    
    set_dps = dps.get("0");
    assertEquals(3, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(ts, set_dps[2].timestamp());
    assertTrue(Double.isNaN(set_dps[0].toDouble()));
    assertEquals(5, set_dps[1].toDouble(), 0.0001);
    assertEquals(8, set_dps[2].toDouble(), 0.0001);
    
    set_dps = dps.get("1");
    assertEquals(3, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(ts, set_dps[2].timestamp());
    assertTrue(Double.isNaN(set_dps[0].toDouble()));
    assertEquals(15, set_dps[1].toDouble(), 0.0001);
    assertTrue(Double.isNaN(set_dps[2].toDouble()));

    ts += 60000;
    it.next();
    
    set_dps = dps.get("0");
    assertEquals(3, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(ts, set_dps[2].timestamp());
    assertEquals(3, set_dps[0].toDouble(), 0.0001);
    assertTrue(Double.isNaN(set_dps[1].toDouble()));
    assertEquals(9, set_dps[2].toDouble(), 0.0001);
    
    set_dps = dps.get("1");
    assertEquals(3, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(ts, set_dps[2].timestamp());
    assertEquals(13, set_dps[0].toDouble(), 0.0001);
    assertTrue(Double.isNaN(set_dps[1].toDouble()));
    assertEquals(19, set_dps[2].toDouble(), 0.0001);

    assertFalse(it.hasNext());
  }
  
  @Test
  public void twoSeriesTimeOffset() throws Exception {
    timeOffset();
    queryAB_Dstar();
    for (ITimeSyncedIterator iterator : iterators.values()) {
      iterator.setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER));
    }
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(2, it.getSeriesSize());
    
    long ts = 1431561600000L;
    it.next();
    
    DataPoint[] set_dps = dps.get("0");
    assertEquals(2, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(1, set_dps[0].toDouble(), 0.0001);
    assertEquals(4, set_dps[1].toDouble(), 0.0001);
    
    set_dps = dps.get("1");
    assertEquals(2, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertTrue(Double.isNaN(set_dps[0].toDouble()));
    assertTrue(Double.isNaN(set_dps[1].toDouble()));

    ts += 60000;
    it.next();
    
    set_dps = dps.get("0");
    assertEquals(2, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(2, set_dps[0].toDouble(), 0.0001);
    assertEquals(5, set_dps[1].toDouble(), 0.0001);
    
    set_dps = dps.get("1");
    assertEquals(2, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertTrue(Double.isNaN(set_dps[0].toDouble()));
    assertTrue(Double.isNaN(set_dps[1].toDouble()));

    ts += 60000;
    it.next();
    
    set_dps = dps.get("0");
    assertEquals(2, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertTrue(Double.isNaN(set_dps[0].toDouble()));
    assertTrue(Double.isNaN(set_dps[1].toDouble()));
    
    set_dps = dps.get("1");
    assertEquals(2, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(13, set_dps[0].toDouble(), 0.0001);
    assertEquals(16, set_dps[1].toDouble(), 0.0001);

    ts += 60000;
    it.next();
    
    set_dps = dps.get("0");
    assertEquals(2, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertTrue(Double.isNaN(set_dps[0].toDouble()));
    assertTrue(Double.isNaN(set_dps[1].toDouble()));
    
    set_dps = dps.get("1");
    assertEquals(2, set_dps.length);
    assertEquals(ts, set_dps[0].timestamp());
    assertEquals(ts, set_dps[1].timestamp());
    assertEquals(14, set_dps[0].toDouble(), 0.0001);
    assertEquals(17, set_dps[1].toDouble(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test (expected = IllegalDataException.class)
  public void noIntersectionUsingResultTags() throws Exception {
    threeDifE();
    queryAB_Dstar();
    new IntersectionIterator("it", iterators, false, false);
  }
  
  @Test
  public void intersectUsingQueryTags() throws Exception {
    threeDifE();
    queryAB_Dstar();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, true, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(3, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 1, 4, 7, 11, 14, 17 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(3, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(ts, set_dps[2].timestamp());
      assertEquals(values[0]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, set_dps[1].toDouble(), 0.0001);
      assertEquals(values[2]++, set_dps[2].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(3, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(ts, set_dps[2].timestamp());
      assertEquals(values[3]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[4]++, set_dps[1].toDouble(), 0.0001);
      assertEquals(values[5]++, set_dps[2].toDouble(), 0.0001);

      ts += 60000;
    }
  }
  
  @Test
  public void commonAggregatedTag() throws Exception {
    twoSeriesAggedE();
    queryAB_Dstar();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(1, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 2, 22 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[0], set_dps[0].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[1], set_dps[0].toDouble(), 0.0001);

      values[0] += 2;
      values[1] += 2;
      
      ts += 60000;
    }
  }
  
  @Test
  public void extraAggTagIgnored() throws Exception {
    twoSeriesAggedEandExtraTagK();
    queryAB_Dstar();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(1, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 2, 22 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[0], set_dps[0].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[1], set_dps[0].toDouble(), 0.0001);

      values[0] += 2;
      values[1] += 2;
      
      ts += 60000;
    }
  }
  
  @Test (expected = IllegalDataException.class)
  public void extraAggTagNoIntersection() throws Exception {
    twoSeriesAggedEandExtraTagK();
    queryAB_Dstar();
    new IntersectionIterator("it", iterators, false, true);
  }
  
  @Test (expected = IllegalDataException.class)
  public void onlyOneResultSet() throws Exception {
    threeSameENoB();
    queryAB_Dstar();
    new IntersectionIterator("it", iterators, false, true);
  }
  
  @Test (expected = IllegalDataException.class)
  public void oneAggedOneTaggedNoIntersection() throws Exception {
    oneAggedTheOtherTagged();
    queryAB_AggAll();
    new IntersectionIterator("it", iterators, false, true);
  }
  
  @Test
  public void oneAggedOneTaggedUseQueryTagsWoutQueryTags() throws Exception {
    oneAggedTheOtherTagged();
    queryAB_AggAll();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, true, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(1, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 2, 11 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[0], set_dps[0].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(values[1]++, set_dps[0].toDouble(), 0.0001);

      // the first set is agged
      values[0] += 2;
      
      ts += 60000;
    }
  }
  
  @Test
  public void singleSeries() throws Exception {
    oneExtraSameE();
    queryA_DD();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(1, dps.size());
    assertEquals(1, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double value = 1;
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(1, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(value++, set_dps[0].toDouble(), 0.0001);

      ts += 60000;
    }
  }
  
  @Test (expected = IllegalDataException.class)
  public void setAMissingE() throws Exception {
    threeAMissingE();
    queryAB_Dstar();
    new IntersectionIterator("it", iterators, false, false);
  }
  
  @Test
  public void setAMissingEQueryTags() throws Exception {
    threeAMissingE();
    queryAB_Dstar();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, true, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertTrue(it.hasNext());
    assertEquals(2, dps.size());
    assertEquals(3, it.getSeriesSize());
    
    long ts = 1431561600000L;
    double values[] = new double[] { 1, 4, 7, 11, 14, 17 };
    while (it.hasNext()) {
      it.next();
      
      DataPoint[] set_dps = dps.get("0");
      assertEquals(3, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(ts, set_dps[2].timestamp());
      assertEquals(values[0]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[1]++, set_dps[1].toDouble(), 0.0001);
      assertEquals(values[2]++, set_dps[2].toDouble(), 0.0001);
      
      set_dps = dps.get("1");
      assertEquals(3, set_dps.length);
      assertEquals(ts, set_dps[0].timestamp());
      assertEquals(ts, set_dps[1].timestamp());
      assertEquals(ts, set_dps[2].timestamp());
      assertEquals(values[3]++, set_dps[0].toDouble(), 0.0001);
      assertEquals(values[4]++, set_dps[1].toDouble(), 0.0001);
      assertEquals(values[5]++, set_dps[2].toDouble(), 0.0001);

      ts += 60000;
    }
  }
  
  @Test
  public void noData() throws Exception {
    setDataPointStorage();
    queryAB_Dstar();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    final Map<String, ExpressionDataPoint[]> dps = it.getResults();
    assertEquals(0, dps.size());
    assertFalse(it.hasNext());
    assertEquals(0, it.getSeriesSize());
  }
  
  @Test (expected = IllegalDataException.class)
  public void nextException() throws Exception {
    threeDisjointSameE();
    queryAB_Dstar();
    
    final IntersectionIterator it = new IntersectionIterator("it", iterators, false, false);
    it.next();
    it.next();
    it.next();
    it.next();
  }
 
  @Test
  public void flattenTags() throws Exception {
    final byte[] flat = IntersectionIterator.flattenTags(
        false, false, tags, agg_tags, sub);
    assertArrayEquals(MockBase.concatByteArrays(UID1, UID1, UID2, UID2), flat);
  }
  
  @Test
  public void flattenTagsWithAgg() throws Exception {
    final byte[] flat = IntersectionIterator.flattenTags(
        false, true, tags, agg_tags, sub);
    assertArrayEquals(
        MockBase.concatByteArrays(UID1, UID1, UID2, UID2, UID3), flat);
  }
  
  @Test
  public void flattenTagsQueryTags() throws Exception {
    final byte[] flat = IntersectionIterator.flattenTags(
        true, false, tags, agg_tags, sub);
    assertArrayEquals(MockBase.concatByteArrays(UID1, UID1), flat);
  }
  
  @Test
  public void flattenTagsQueryTagsWithAgg() throws Exception {
    final byte[] flat = IntersectionIterator.flattenTags(
        true, true, tags, agg_tags, sub);
    assertArrayEquals(MockBase.concatByteArrays(UID1, UID1, UID3), flat);
  }
  
  @Test
  public void flattenEmptyTags() throws Exception {
    tags.clear();
    final byte[] flat = IntersectionIterator.flattenTags(
        false, false, tags, agg_tags, sub);
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, flat);
  }
  
  @Test
  public void flattenEmptyTagsWithAggEmpty() throws Exception {
    agg_tags.clear();
    final byte[] flat = IntersectionIterator.flattenTags(
        false, true, tags, agg_tags, sub);
    assertArrayEquals(MockBase.concatByteArrays(UID1, UID1, UID2, UID2), flat);
  }
  
  // TODO - how will this play out if we choose a default agg of "none" and the
  // user hasn't asked for any filtering?
  @Test
  public void flattenTagsQueryTagsEmpty() throws Exception {
    query_tags.clear();
    final byte[] flat = IntersectionIterator.flattenTags(
        true, false, tags, agg_tags, sub);
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, flat);
  }
  
  @Test
  public void flattenTagsQueryTagsEmptyWithAgg() throws Exception {
    query_tags.clear();
    final byte[] flat = IntersectionIterator.flattenTags(
        true, true, tags, agg_tags, sub);
    assertArrayEquals(UID3, flat);
  }

  @Test (expected = NullPointerException.class)
  public void flattenTagsNullTags() throws Exception {
    IntersectionIterator.flattenTags(false, false, null, agg_tags, sub);
  }
  
  @Test
  public void flattenTagsNullAggTagsNotRequested() throws Exception {
    final byte[] flat = IntersectionIterator.flattenTags(
        false, false, tags, null, sub);
    assertArrayEquals(MockBase.concatByteArrays(UID1, UID1, UID2, UID2), flat);
  }
  
  @Test (expected = NullPointerException.class)
  public void flattenTagsNullAggTags() throws Exception {
    IntersectionIterator.flattenTags(false, true, tags, null, sub);
  }
  
  @Test
  public void flattenTagsNullSubNotRequested() throws Exception {
    final byte[] flat = IntersectionIterator.flattenTags(
        false, false, tags, agg_tags, null);
    assertArrayEquals(MockBase.concatByteArrays(UID1, UID1, UID2, UID2), flat);
  }
  
  @Test (expected = NullPointerException.class)
  public void flattenTagsNullSub() throws Exception {
    IntersectionIterator.flattenTags(true, false, tags, agg_tags, null);
  }

}
