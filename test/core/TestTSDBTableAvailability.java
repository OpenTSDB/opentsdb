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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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

  private TSDB createTSDB(List<RegionLocation> uid_regions,
                          List<RegionLocation> data_regions) {
    TSDB tsdb = new TSDB(mock(HBaseClient.class), config);
    String uid_table = config.getString("tsd.storage.hbase.uid_table");
    String data_table = config.getString("tsd.storage.hbase.data_table");

    Deferred<List<RegionLocation>> d = new Deferred<List<RegionLocation>>();
    d.callback(uid_regions);
    when(tsdb.getClient().locateRegions(uid_table)).thenReturn(d);

    Deferred<List<RegionLocation>> d2 = new Deferred<List<RegionLocation>>();
    d2.callback(data_regions);
    when(tsdb.getClient().locateRegions(data_table)).thenReturn(d2);
    return tsdb;
  }

  /** If locateRegions() throws an exception, availability is NONE */
  @Test
  public void failedToGetRegions() throws Exception {
    Deferred<List<RegionLocation>> d = new Deferred<List<RegionLocation>>();
    d.callback(new Exception());
    Deferred<List<RegionLocation>> d2 = new Deferred<List<RegionLocation>>();
    d2.callback(new Exception());
    TSDB tsdb = new TSDB(mock(HBaseClient.class), config);
    when(tsdb.getClient()
         .locateRegions(config.getString("tsd.storage.hbase.uid_table"))
         ).thenReturn(d);
    when(tsdb.getClient()
         .locateRegions(config.getString("tsd.storage.hbase.data_table"))
         ).thenReturn(d2);
    assertEquals(tsdb.checkNecessaryTablesAvailability().join(),
                 TSDB.TableAvailability.NONE);
  }

  /** If locateRegions() returns empty list, availability is NONE */
  @Test
  public void noRegions() throws Exception {
    TSDB tsdb = createTSDB(new ArrayList<RegionLocation>(),
                           new ArrayList<RegionLocation>());
    assertEquals(tsdb.checkNecessaryTablesAvailability().join(),
                 TSDB.TableAvailability.NONE);
  }

  /* If all returned regions return a result, availability is FULL. */
  @Test
  public void allRegionsAvailable() throws Exception {
    TSDB original = new TSDB(mock(HBaseClient.class), config);
    TSDB tsdb = PowerMockito.spy(original);
    ArrayList<Boolean> region_availability = new ArrayList<Boolean>();
    region_availability.add(true);

    Deferred<ArrayList<Boolean>> get_results = new Deferred<ArrayList<Boolean>>();
    get_results.callback(region_availability);
    Deferred<ArrayList<Boolean>> get_results2 = new Deferred<ArrayList<Boolean>>();
    get_results2.callback(region_availability);

    PowerMockito.doReturn(get_results).when(tsdb)
      .getTableRegionAvailability("tsd.storage.hbase.uid_table");
    PowerMockito.doReturn(get_results2).when(tsdb)
      .getTableRegionAvailability("tsd.storage.hbase.data_table");

    assertEquals(tsdb.checkNecessaryTablesAvailability().join(),
                 TSDB.TableAvailability.FULL);
  }

    // If one out of many regions returned by locateRegions() throws exception,
    // availability is PARTIAL.

    // If one table returns PARTIAL and the other returns FULL, final result is
    // PARTIAL

    // If one table returns NONE and the other returns FULL, final result is
    // NONE

}
