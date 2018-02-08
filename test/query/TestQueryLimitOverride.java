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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.TimerTask;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.io.Files;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryLimitOverride.QueryLimitOverrideItem;
import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, HashedWheelTimer.class, QueryLimitOverride.class, 
  File.class, Files.class })
public class TestQueryLimitOverride {
  
  private TSDB tsdb;
  private Config config;
  private HashedWheelTimer timer;
  private File file;
  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    config = new Config(false);
    timer = mock(HashedWheelTimer.class);
    PowerMockito.when(tsdb.getTimer()).thenReturn(timer);
    when(tsdb.getConfig()).thenReturn(config);
    PowerMockito.mockStatic(Files.class);
    file = mock(File.class);
    PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(file);
    
    config.overrideConfig("tsd.query.limits.bytes.default", "42");
    config.overrideConfig("tsd.query.limits.data_points.default", "24");
    config.overrideConfig("tsd.query.limits.overrides.config", "/tmp/overrides.json");
  }

  @Test
  public void ctorNoFileConfigured() throws Exception {
    config.overrideConfig("tsd.query.limits.overrides.config", null);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    assertEquals(42, limits.getDefaultByteLimit());
    assertEquals(24, limits.getDefaultDataPointsLimit());
    assertEquals(0, limits.getLimits().size());
    verify(file, never()).exists();
    verify(timer, never())
      .newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }
  
  @Test
  public void ctorFileNoReload() throws Exception {
    config.overrideConfig("tsd.query.limits.overrides.interval", null);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    assertEquals(42, limits.getDefaultByteLimit());
    assertEquals(24, limits.getDefaultDataPointsLimit());
    verify(file, times(1)).exists();
    verify(timer, never())
      .newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }
  
  @Test
  public void ctorFileDoesntExistException() throws Exception {
    PowerMockito.when(Files.getFileExtension(anyString()))
      .thenThrow(new IllegalStateException("Boo!"));
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    verify(file, times(1)).exists();
    assertEquals(42, limits.getDefaultByteLimit());
    assertEquals(24, limits.getDefaultDataPointsLimit());
    assertEquals(0, limits.getLimits().size());
    verify(timer, times(1))
      .newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void ctorNegativeDefaultsLimit() throws Exception {
    config.overrideConfig("tsd.query.limits.bytes.default", "-42");
    config.overrideConfig("tsd.query.limits.data_points.default", "24");
    try {
      new QueryLimitOverride(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    config.overrideConfig("tsd.query.limits.bytes.default", "42");
    config.overrideConfig("tsd.query.limits.data_points.default", "-24");
    try {
      new QueryLimitOverride(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void ctorWithFile() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,\"dataPointsLimit\":16}]");
    when(file.exists()).thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    verify(file, times(1)).exists();
    assertEquals(42, limits.getDefaultByteLimit());
    assertEquals(24, limits.getDefaultDataPointsLimit());
    assertEquals(1, limits.getLimits().size());
    final QueryLimitOverrideItem item = limits.getLimits().iterator().next();
    assertEquals(24, item.getByteLimit());
    assertEquals(16, item.getDataPointsLimit());
    assertEquals(".*sys$", item.getRegex());
    assertTrue(item.matches("namespace.app.sys"));
    assertFalse(item.matches("namespace.app.sys.cpu"));
    verify(timer, times(1))
      .newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }
  
  @Test
  public void ctorWithFileException() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenThrow(new IOException("Boo!"));
    when(file.exists()).thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    verify(file, times(1)).exists();
    assertEquals(42, limits.getDefaultByteLimit());
    assertEquals(24, limits.getDefaultDataPointsLimit());
    assertEquals(0, limits.getLimits().size());
    verify(timer, times(1))
      .newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }
  
  @Test
  public void ctorWithFileBadJSON() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLim");
    when(file.exists()).thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    verify(file, times(1)).exists();
    assertEquals(42, limits.getDefaultByteLimit());
    assertEquals(24, limits.getDefaultDataPointsLimit());
    assertEquals(0, limits.getLimits().size());
    verify(timer, times(1))
      .newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }
  
  @Test
  public void ctorWithFileBadRegex() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sy(notclosed\",\"byteLimit\":24,"
          + "\"dataPointsLimit\":16}]");
    when(file.exists()).thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    verify(file, times(1)).exists();
    assertEquals(42, limits.getDefaultByteLimit());
    assertEquals(24, limits.getDefaultDataPointsLimit());
    assertEquals(0, limits.getLimits().size());
    verify(timer, times(1))
      .newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }
  
  @Test
  public void timerTaskEmptySet() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,"
          + "\"dataPointsLimit\":16}]");
    when(file.exists())
      .thenReturn(false)
      .thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    assertEquals(0, limits.getLimits().size());
    limits.run(null);
    assertEquals(1, limits.getLimits().size());
    final QueryLimitOverrideItem item = limits.getLimits().iterator().next();
    assertEquals(24, item.getByteLimit());
    assertEquals(16, item.getDataPointsLimit());
    assertEquals(".*sys$", item.getRegex());
  }
  
  @Test
  public void timerTaskSame() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,"
          + "\"dataPointsLimit\":16}]");
    when(file.exists())
      .thenReturn(true)
      .thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    assertEquals(1, limits.getLimits().size());
    final QueryLimitOverrideItem item = limits.getLimits().iterator().next();
    assertEquals(24, item.getByteLimit());
    assertEquals(16, item.getDataPointsLimit());
    assertEquals(".*sys$", item.getRegex());
    
    limits.run(null);
    assertEquals(1, limits.getLimits().size());
    // same obj/address
    assertSame(item, limits.getLimits().iterator().next());
  }
  
  @Test
  public void timerTaskDiffLimit() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,"
          + "\"dataPointsLimit\":16}]")
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":60,"
          + "\"dataPointsLimit\":16}]");
    when(file.exists())
      .thenReturn(true)
      .thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    assertEquals(1, limits.getLimits().size());
    QueryLimitOverrideItem item = limits.getLimits().iterator().next();
    assertEquals(24, item.getByteLimit());
    assertEquals(16, item.getDataPointsLimit());
    assertEquals(".*sys$", item.getRegex());
    
    limits.run(null);
    assertEquals(1, limits.getLimits().size());
    item = limits.getLimits().iterator().next();
    assertEquals(60, item.getByteLimit());
    assertEquals(16, item.getDataPointsLimit());
    assertEquals(".*sys$", item.getRegex());
  }
  
  @Test
  public void timerTaskCleared() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,"
          + "\"dataPointsLimit\":16}]")
      .thenReturn("[]");
    when(file.exists())
      .thenReturn(true)
      .thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    assertEquals(1, limits.getLimits().size());
    limits.run(null);
    assertEquals(0, limits.getLimits().size());
  }
  
  @Test
  public void timerTaskAddOne() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,"
          + "\"dataPointsLimit\":16}]")
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,"
          + "\"dataPointsLimit\":16},"
          + "{\"regex\":\".*if$\",\"byteLimit\":96,"
          + "\"dataPointsLimit\":32}]");
    when(file.exists())
      .thenReturn(true)
      .thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    assertEquals(1, limits.getLimits().size());
    limits.run(null);
    assertEquals(2, limits.getLimits().size());
  }
  
  @Test
  public void timerTaskRemoveOne() throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,"
          + "\"dataPointsLimit\":16},"
          + "{\"regex\":\".*if$\",\"byteLimit\":96,"
          + "\"dataPointsLimit\":32}]")
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,"
          + "\"dataPointsLimit\":16}]");
    when(file.exists())
      .thenReturn(true)
      .thenReturn(true);
    final QueryLimitOverride limits = new QueryLimitOverride(tsdb);
    assertEquals(2, limits.getLimits().size());
    limits.run(null);
    assertEquals(1, limits.getLimits().size());
  }
  
  /** @return an override object with a couple of items */
  public QueryLimitOverride getTestObject(final boolean with_file) throws Exception {
    PowerMockito.when(Files.class, "toString", any(File.class), any(Charset.class))
      .thenReturn("[{\"regex\":\".*sys$\",\"byteLimit\":24,\"dataPointsLimit\":16},"
          + "{\"regex\":\".*perf.*\",\"byteLimit\":84,\"dataPointsLimit\":42}]");
    when(file.exists()).thenReturn(true);
    return new QueryLimitOverride(tsdb); 
  }
}
