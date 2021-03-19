// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.tools;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.BaseTsdbTest.FakeTaskTimer;
import net.opentsdb.meta.Annotation;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Threads;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, KeyValue.class, Fsck.class,
  FsckOptions.class, Scanner.class, Annotation.class, Tags.class,
  HashedWheelTimer.class, Threads.class })
public class TestFsckWRollups {
  protected byte[] GLOBAL_ROW = 
      new byte[] {0, 0, 0, 0x52, (byte)0xC3, 0x5A, (byte)0x80};
  protected byte[] ROW = MockBase.stringToBytes("00000150E22700000001000001");
  protected byte[] ROW2 = MockBase.stringToBytes("00000150E23510000001000001");
  protected byte[] ROW3 = MockBase.stringToBytes("00000150E24320000001000001");
  protected byte[] BAD_KEY = { 0x00, 0x00, 0x01 };
  protected Config config;
  protected TSDB tsdb = null;
  protected HBaseClient client;
  protected UniqueId metrics;
  protected UniqueId tag_names;
  protected UniqueId tag_values;
  protected MockBase storage;
  protected FsckOptions options;
  protected FakeTaskTimer timer;
  protected final static List<byte[]> tags = new ArrayList<byte[]>(1);
  static {
    tags.add(new byte[] { 0, 0, 1, 0, 0, 1});
  }

  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    client = mock(HBaseClient.class);
    metrics = mock(UniqueId.class);
    tag_names = mock(UniqueId.class);
    tag_values = mock(UniqueId.class);
    options = mock(FsckOptions.class);
    timer = new FakeTaskTimer();
    
    PowerMockito.mockStatic(Threads.class);
    PowerMockito.when(Threads.newTimer(anyString())).thenReturn(timer);
    PowerMockito.when(Threads.newTimer(anyInt(), anyString())).thenReturn(timer);
    
    PowerMockito.whenNew(HashedWheelTimer.class).withNoArguments()
      .thenReturn(timer);
    PowerMockito.whenNew(HBaseClient.class).withAnyArguments()
      .thenReturn(client);
    
    config = new Config(false);
    tsdb = new TSDB(client, config);
    when(client.flush()).thenReturn(Deferred.fromResult(null));
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));
    
    when(options.fix()).thenReturn(false);
    when(options.compact()).thenReturn(false);
    when(options.resolveDupes()).thenReturn(false);
    when(options.lastWriteWins()).thenReturn(false);
    when(options.deleteOrphans()).thenReturn(false);
    when(options.deleteUnknownColumns()).thenReturn(false);
    when(options.deleteBadValues()).thenReturn(false);
    when(options.deleteBadRows()).thenReturn(false);
    when(options.deleteBadCompacts()).thenReturn(false);
    when(options.threads()).thenReturn(1);

    // replace the "real" field objects with mocks
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);
    
    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);
    
    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);
    
    // mock UniqueId
    when(metrics.getId("sys.cpu.user")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getName(new byte[] { 0, 0, 2 })).thenReturn("sys.cpu.nice");
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getName(new byte[] { 0, 0, 1 })).thenReturn("host");
    when(tag_names.getOrCreateId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getId("dc")).thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getName(new byte[] { 0, 0, 1 })).thenReturn("web01");
    when(tag_values.getOrCreateId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getName(new byte[] { 0, 0, 2 })).thenReturn("web02");
    when(tag_values.getOrCreateId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "metric"));

    PowerMockito.mockStatic(Tags.class);
    when(Tags.resolveIds((TSDB)any(), (ArrayList<byte[]>)any()))
      .thenReturn(null); // don't care

    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
    
    RollupConfig rollup_config = RollupConfig.builder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .addAggregationId("min", 2)
        .addAggregationId("max", 3)
        .addInterval(RollupInterval.builder()
            .setInterval("1m")
            .setRowSpan("24h")
            .setTable("tsdb-1m")
            .setPreAggregationTable("tsdb-preagg-1m"))
        .addInterval(RollupInterval.builder()
            .setInterval("1h")
            .setRowSpan("1d")
            .setTable("tsdb-1h")
            .setPreAggregationTable("tsdb-preagg-1h"))
        .build();
    Whitebox.setInternalState(tsdb, "rollup_config", rollup_config);
  }

  @Test
  public void oneBadOneGood() throws Exception {
    final byte[] qual1 = { 0x01, 0x00, 0x00 };
    final byte[] val1 =  { 4 };
    final byte[] qual2 = { (byte) 0x2, 0x00, 0x02 };
    final byte[] val2 = new byte[] { 0, 0, 0, 5 };
    storage.addColumn(ROW, qual1, val1);
    storage.addColumn(ROW, qual2, val2);
    
    final Fsck fsck = new Fsck(tsdb, options);
    fsck.runFullTable();
    assertEquals(2, fsck.kvs_processed.get());
    assertEquals(1, fsck.bad_values.get());
    assertEquals(1, fsck.totalErrors());
  }  
  
}
