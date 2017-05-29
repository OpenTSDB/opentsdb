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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.io.Files;

import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, HistogramDataPointDecoderManager.class, 
  Files.class })
public class TestHistogramDataPointDecoderManager {

  private TSDB tsdb;
  private Config config;
  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    config = new Config(false);
    
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\": 0,"
        + "\"net.opentsdb.core.TestHistogramDataPointDecoderManager$MockDecoder\":1}");
    when(tsdb.getConfig()).thenReturn(config);
    PowerMockito.mockStatic(Files.class);
  }
  
  @Test
  public void ctor() throws Exception {
    HistogramDataPointDecoderManager manager = 
        new HistogramDataPointDecoderManager(tsdb);
    assertEquals(0, manager.getDecoder(SimpleHistogramDecoder.class));
    assertEquals(1, manager.getDecoder(MockDecoder.class));
    assertTrue(manager.getDecoder((byte) 0) instanceof SimpleHistogramDecoder);
    assertTrue(manager.getDecoder((byte) 1) instanceof MockDecoder);
    
    // bad JSON
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\": ");
    try {
      new HistogramDataPointDecoderManager(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    config.overrideConfig("tsd.core.histograms.config", "nosuchfile.json");
    when(Files.toString(any(File.class), eq(Const.UTF8_CHARSET)))
      .thenReturn("{\"net.opentsdb.core.SimpleHistogramDecoder\": 0}");
    manager = new HistogramDataPointDecoderManager(tsdb);
    assertEquals(0, manager.getDecoder(SimpleHistogramDecoder.class));
    assertTrue(manager.getDecoder((byte) 0) instanceof SimpleHistogramDecoder);
    
    when(Files.toString(any(File.class), eq(Const.UTF8_CHARSET)))
      .thenThrow(new IOException("Boo!"));
    try {
      new HistogramDataPointDecoderManager(tsdb);
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
    
    // no such plugin
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.NoSuchPlugin\":0}");
    try {
      new HistogramDataPointDecoderManager(tsdb);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // bad plugin
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.TestHistogramDataPointDecoderManager$MockDecoderBadly\":0}");
    try {
      new HistogramDataPointDecoderManager(tsdb);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void getDecoder() throws Exception {
    final HistogramDataPointDecoderManager manager = 
        new HistogramDataPointDecoderManager(tsdb);
    assertTrue(manager.getDecoder((byte) 0) instanceof SimpleHistogramDecoder);
    
    try {
      manager.getDecoder((byte) 43);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getDecoderClass() throws Exception {
    final HistogramDataPointDecoderManager manager = 
        new HistogramDataPointDecoderManager(tsdb);
    assertEquals(0, manager.getDecoder(SimpleHistogramDecoder.class));
    assertEquals(1, manager.getDecoder(MockDecoder.class));
    
    try {
      manager.getDecoder(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      manager.getDecoder(MockDecoderBadly.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  public static class MockDecoder extends HistogramDataPointDecoder {

    @Override
    public HistogramDataPoint decode(byte[] raw_data, long timestamp) {
      return null;
    }
    
  }
  
  static class MockDecoderBadly extends HistogramDataPointDecoder {

    // not allowed!
    public MockDecoderBadly(final long unwanted_param) { }
    
    @Override
    public HistogramDataPoint decode(byte[] raw_data, long timestamp) {
      return null;
    }
    
  }
}
