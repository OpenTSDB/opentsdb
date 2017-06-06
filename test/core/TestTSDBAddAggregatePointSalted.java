// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.storage.MockBase;

/**
 * Integration test that runs all of the tests in {@see TestTSDBAddAggregatePoint}
 * but with salting enabled just to verify nothing goes wrong with the extra
 * salt bytes.
 */
@RunWith(PowerMockRunner.class)
public class TestTSDBAddAggregatePointSalted extends TestTSDBAddAggregatePoint {

  @Before
  public void beforeLocal() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);
    
    agg_tag_key = config.getString("tsd.rollups.agg_tag_key");
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    final List<byte[]> families = new ArrayList<byte[]>();
    families.add(FAMILY);
    
    storage.addTable("tsdb-rollup-10m".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-10m".getBytes(), families);
    storage.addTable("tsdb-rollup-1h".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-1h".getBytes(), families);
    storage.addTable("tsdb-rollup-1d".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-1d".getBytes(), families);
    storage.addTable(AGG_TABLE, families);
    
    rollup_config = RollupConfig.builder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .addAggregationId("max", 2)
        .addAggregationId("min", 3)
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-10m")
            .setPreAggregationTable("tsdb-rollup-agg-10m")
            .setInterval("10m")
            .setRowSpan("1d"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1h")
            .setPreAggregationTable("tsdb-rollup-agg-1h")
            .setInterval("1h")
            .setRowSpan("1n"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1d")
            .setPreAggregationTable("tsdb-rollup-agg-1d")
            .setInterval("1d")
            .setRowSpan("1y"))
        .build();
    Whitebox.setInternalState(tsdb, "rollup_config", rollup_config);
    Whitebox.setInternalState(tsdb, "default_interval", 
        rollup_config.getRollupInterval("10m"));
    Whitebox.setInternalState(tsdb, "rollups_block_derived", true);
    Whitebox.setInternalState(tsdb, "agg_tag_key", 
        config.getString("tsd.rollups.agg_tag_key"));
    Whitebox.setInternalState(tsdb, "raw_agg_tag_value", 
        config.getString("tsd.rollups.raw_agg_tag_value"));
    setupGroupByTagValues();
    
    row = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_STRING);
  }
}
