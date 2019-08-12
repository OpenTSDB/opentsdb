// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.hbase.async.RegionLocation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
      "ch.qos.*", "org.slf4j.*",
      "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, HBaseClient.class,
      CompactionQueue.class, GetRequest.class, PutRequest.class, KeyValue.class,
      Scanner.class, AtomicIncrementRequest.class, Const.class, })
public final class TestTSDBTableAvailability extends BaseTsdbTest {

  private TSDB createTSDB(Deferred<List<RegionLocation>> uid_regions,
                          Deferred<List<RegionLocation>> data_regions) {
    TSDB tsdb = new TSDB(mock(HBaseClient.class), config);
    when(tsdb.getClient()
         .locateRegions(config.getString("tsd.storage.hbase.uid_table"))
         ).thenReturn(uid_regions);
    when(tsdb.getClient()
         .locateRegions(config.getString("tsd.storage.hbase.data_table"))
         ).thenReturn(data_regions);
    return tsdb;
  }

  /** If locateRegions() throws an exception, availability is NONE */
  @Test
  public void failedToGetRegions() throws Exception {
    Deferred<List<RegionLocation>> d = new Deferred<List<RegionLocation>>();
    d.callback(new Exception());
    Deferred<List<RegionLocation>> d2 = new Deferred<List<RegionLocation>>();
    d2.callback(new Exception());
    TSDB tsdb = createTSDB(d, d2);
    assertEquals(tsdb.checkNecessaryTablesAvailability().join(),
                 TSDB.TableAvailability.NONE);
  }

  /** If locateRegions() returns empty list, availability is NONE */
  @Test
  public void noRegions() throws Exception {
    Deferred<List<RegionLocation>> d = new Deferred<List<RegionLocation>>();
    d.callback(new ArrayList<RegionLocation>());
    Deferred<List<RegionLocation>> d2 = new Deferred<List<RegionLocation>>();
    d2.callback(new ArrayList<RegionLocation>());
    TSDB tsdb = createTSDB(d, d2);
    assertEquals(tsdb.checkNecessaryTablesAvailability().join(),
                 TSDB.TableAvailability.NONE);
  }

    // If all returned regions return a result, availability is FULL

    // If a regions returned by locateRegions() throws exception, availability
    // is PARTIAL.

    // If one table returns PARTIAL and the other returns FULL, final result is
    // PARTIAL

    // If one table returns NONE and the other returns FULL, final result is
    // NONE

}
