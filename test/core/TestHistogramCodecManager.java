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
@PrepareForTest({ TSDB.class, HistogramCodecManager.class, 
  Files.class })
public class TestHistogramCodecManager {

  private TSDB tsdb;
  private Config config;
  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    config = new Config(false);
    
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\": 0,"
        + "\"net.opentsdb.core.TestHistogramCodecManager$MockDecoder\":1}");
    when(tsdb.getConfig()).thenReturn(config);
    PowerMockito.mockStatic(Files.class);
  }
  
  @Test
  public void ctor() throws Exception {
    HistogramCodecManager manager = 
        new HistogramCodecManager(tsdb);
    assertEquals(0, manager.getCodec(SimpleHistogramDecoder.class));
    assertEquals(1, manager.getCodec(MockDecoder.class));
    HistogramDataPointCodec codec = manager.getCodec(0);
    assertEquals(0, codec.getId());
    assertTrue(codec instanceof SimpleHistogramDecoder);
    codec = manager.getCodec(1);
    assertEquals(1, codec.getId());
    assertTrue(codec instanceof MockDecoder);
    
    // bad JSON
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\": ");
    try {
      new HistogramCodecManager(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // id too small
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\":-1}s");
    try {
      new HistogramCodecManager(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // id too big
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\":256}s");
    try {
      new HistogramCodecManager(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // duplicate ID
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\": 42,"
        + "\"net.opentsdb.core.TestHistogramCodecManager$MockDecoder\":42}");
    try {
      new HistogramCodecManager(tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    config.overrideConfig("tsd.core.histograms.config", "nosuchfile.json");
    when(Files.toString(any(File.class), eq(Const.UTF8_CHARSET)))
      .thenReturn("{\"net.opentsdb.core.SimpleHistogramDecoder\": 0}");
    manager = new HistogramCodecManager(tsdb);
    assertEquals(0, manager.getCodec(SimpleHistogramDecoder.class));
    assertTrue(manager.getCodec(0) instanceof SimpleHistogramDecoder);
    
    when(Files.toString(any(File.class), eq(Const.UTF8_CHARSET)))
      .thenThrow(new IOException("Boo!"));
    try {
      new HistogramCodecManager(tsdb);
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
    
    // no such plugin
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.NoSuchPlugin\":0}");
    try {
      new HistogramCodecManager(tsdb);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // bad plugin
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.TestHistogramCodecManager$MockDecoderBadly\":0}");
    try {
      new HistogramCodecManager(tsdb);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void getDecoder() throws Exception {
    final HistogramCodecManager manager = 
        new HistogramCodecManager(tsdb);
    assertTrue(manager.getCodec(0) instanceof SimpleHistogramDecoder);
    
    try {
      manager.getCodec(43);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getDecoderClass() throws Exception {
    final HistogramCodecManager manager = 
        new HistogramCodecManager(tsdb);
    assertEquals(0, manager.getCodec(SimpleHistogramDecoder.class));
    assertEquals(1, manager.getCodec(MockDecoder.class));
    
    try {
      manager.getCodec(MockDecoderBadly.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  public static class MockDecoder extends HistogramDataPointCodec {

    @Override
    public Histogram decode(byte[] raw_data, boolean includes_id) {
      return null;
    }

    
    @Override
    public byte[] encode(Histogram data_point, boolean include_id) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  static class MockDecoderBadly extends HistogramDataPointCodec {

    // not allowed!
    public MockDecoderBadly(final long unwanted_param) { }
    
    @Override
    public Histogram decode(byte[] raw_data, boolean includes_id) {
      return null;
    }
    

    @Override
    public byte[] encode(Histogram data_point, boolean include_id) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}
