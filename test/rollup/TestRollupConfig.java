// This file is part of OpenTSDB.
// Copyright (C) 2010-2015  The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class TestRollupConfig {
  private final static String rollup_table = "tsdb-rollup-10m";
  private final static String preagg_table = "tsdb-rollup-agg-10m";

  @Test
  public void ctor() throws Exception {
    final RollupConfig config = new RollupConfig();
    assertNotNull(config);
    assertTrue(config.forward_intervals.size() >= 1);
    assertEquals(config.forward_intervals.size() * 2, 
        config.reverse_intervals.size());
  }

  @Test
  public void getRollupIntervalString() {
    final RollupConfig config = new RollupConfig();
    final Map<String, RollupInterval> forward_intervals = 
        new HashMap<String, RollupInterval>();
    final RollupInterval rollup = new RollupInterval(
        rollup_table, preagg_table, "10m", "1d");
    forward_intervals.put(rollup.getStringInterval(), rollup);
    Whitebox.setInternalState(config, "forward_intervals", forward_intervals);
    
    final RollupInterval fetched = config.getRollupInterval("10m");
    assertTrue(rollup == fetched);
  }
  
  @Test (expected = NoSuchRollupForIntervalException.class)
  public void getRollupIntervalStringNoSuchRollup() {
    final RollupConfig config = new RollupConfig();
    final Map<String, RollupInterval> forward_intervals = 
        new HashMap<String, RollupInterval>();
    Whitebox.setInternalState(config, "forward_intervals", forward_intervals);
    
    config.getRollupInterval("10m");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getRollupIntervalStringNullString() {
    new RollupConfig().getRollupInterval((String)null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getRollupIntervalStringEmptyString() {
    new RollupConfig().getRollupInterval("");
  }
  
  @Test
  public void getRollupIntervalForTable() {
    final RollupConfig config = new RollupConfig();
    final Map<String, RollupInterval> reverse_intervals = 
        new HashMap<String, RollupInterval>();
    final RollupInterval rollup = new RollupInterval(
        rollup_table, preagg_table, "10m", "1d");
    reverse_intervals.put(rollup.getTemporalTableName(), rollup);
    reverse_intervals.put(rollup.getGroupbyTableName(), rollup);
    Whitebox.setInternalState(config, "reverse_intervals", reverse_intervals);
    
    RollupInterval fetched = config.getRollupIntervalForTable(rollup_table);
    assertTrue(rollup == fetched);
    fetched = config.getRollupIntervalForTable(preagg_table);
    assertTrue(rollup == fetched);
  }
  
  @Test (expected = NoSuchRollupForTableException.class)
  public void getRollupIntervalForTableNoSuchRollup() {
    final RollupConfig config = new RollupConfig();
    final Map<String, RollupInterval> reverse_intervals = 
        new HashMap<String, RollupInterval>();
    Whitebox.setInternalState(config, "reverse_intervals", reverse_intervals);
    
    config.getRollupIntervalForTable(rollup_table);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getRollupIntervalForTableNull() {
    new RollupConfig().getRollupIntervalForTable(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getRollupIntervalForTableEmpty() {
    new RollupConfig().getRollupIntervalForTable("");
  }
  
  // Does nothing effectively
  @Test
  public void validateAndCompileIntervalsEmptyList() throws Exception {
    final RollupConfig config = new RollupConfig();
    config.validateAndCompileIntervals(Collections.<RollupInterval>emptyList());
    assertTrue(config.forward_intervals.size() >= 1);
    assertEquals(config.forward_intervals.size() * 2, 
        config.reverse_intervals.size());
  }
  
  @Test (expected = NullPointerException.class)
  public void validateAndCompileIntervalsNullList() throws Exception {
    new RollupConfig().validateAndCompileIntervals(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateAndCompileIntervalsDuplicate() throws Exception {
    final List<RollupInterval> list = new ArrayList<RollupInterval>();
    list.add(new RollupInterval(rollup_table, preagg_table, "1h", "1d"));
    final RollupConfig config = new RollupConfig();
    config.validateAndCompileIntervals(list);
  }

}
