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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;

import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  CompactionQueue.class})
public final class TestTSDB {
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private CompactionQueue compactionq = mock(CompactionQueue.class);
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    tsdb = new TSDB(config);
    
    // replace the "real" field objects with mocks
    Field cl = tsdb.getClass().getDeclaredField("client");
    cl.setAccessible(true);
    cl.set(tsdb, client);
    
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);
    
    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);
    
    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);
    
    Field cq = tsdb.getClass().getDeclaredField("compactionq");
    cq.setAccessible(true);
    cq.set(tsdb, compactionq);
  }
  
  @Test
  public void getClient() {
    assertNotNull(tsdb.getClient());
  }
  
  @Test
  public void getConfig() {
    assertNotNull(tsdb.getConfig());
  }
  
  @Test
  public void assignUidMetric() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        tsdb.assignUid("metric", "sys.cpu.1"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidMetricExists() {
    setupAssignUid();
    tsdb.assignUid("metric", "sys.cpu.0");
  }
  
  @Test
  public void assignUidTagk() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        tsdb.assignUid("tagk", "datacenter"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagkExists() {
    setupAssignUid();
    tsdb.assignUid("tagk", "host");
  }
  
  @Test
  public void assignUidTagv() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        tsdb.assignUid("tagv", "myserver"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagvExists() {
    setupAssignUid();
    tsdb.assignUid("tagv", "localhost");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidBadType() {
    setupAssignUid();
    tsdb.assignUid("nothere", "localhost");
  }
  
  @Test (expected = NullPointerException.class)
  public void assignUidNullType() {
    setupAssignUid();
    tsdb.assignUid(null, "localhost");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidNullName() {
    setupAssignUid();
    tsdb.assignUid("metric", null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidInvalidCharacter() {
    setupAssignUid();
    tsdb.assignUid("metric", "Not!A:Valid@Name");
  }
  
  private void setupAssignUid() {
    when(metrics.getId("sys.cpu.0")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getId("sys.cpu.1")).thenThrow(
        new NoSuchUniqueName("metric", "sys.cpu.1"));
    when(metrics.getOrCreateId("sys.cpu.1")).thenReturn(new byte[] { 0, 0, 2 });
    
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getId("datacenter")).thenThrow(
        new NoSuchUniqueName("tagk", "datacenter"));
    when(tag_names.getOrCreateId("datacenter")).thenReturn(new byte[] { 0, 0, 2 });
    
    when(tag_values.getId("localhost")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getId("myserver")).thenThrow(
        new NoSuchUniqueName("tagv", "myserver"));
    when(tag_values.getOrCreateId("myserver")).thenReturn(new byte[] { 0, 0, 2 });
  }
}
