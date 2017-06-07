// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;

public class TestTsdbQueryRollupSalted extends TestTsdbQueryRollup {

  @Before
  public void beforeLocal() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);
    
    storeLongTimeSeriesSeconds(false, false);
    final List<byte[]> families = new ArrayList<byte[]>();
    families.add(FAMILY);
    
    storage.addTable("tsdb-rollup-10m".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-10m".getBytes(), families);
    storage.addTable("tsdb-rollup-1h".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-1h".getBytes(), families);
    storage.addTable("tsdb-rollup-1d".getBytes(), families);
    storage.addTable("tsdb-rollup-agg-1d".getBytes(), families);
    
    query = new TsdbQuery(tsdb);
    tags2 = new HashMap<String, String>(1);
    tags2.put(TAGK_STRING, TAGV_B_STRING);
    
    rollup_config = RollupConfig.builder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .addAggregationId("max", 2)
        .addAggregationId("min", 3)
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-10m")
            .setPreAggregationTable("tsdb-rollup-agg-10m")
            .setInterval("10m")
            .setRowSpan("6h"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1h")
            .setPreAggregationTable("tsdb-rollup-agg-1h")
            .setInterval("1h")
            .setRowSpan("1d"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1d")
            .setPreAggregationTable("tsdb-rollup-agg-1d")
            .setInterval("1d")
            .setRowSpan("1n"))
        .build();
    Whitebox.setInternalState(tsdb, "rollup_config", rollup_config);
  }
  
}
