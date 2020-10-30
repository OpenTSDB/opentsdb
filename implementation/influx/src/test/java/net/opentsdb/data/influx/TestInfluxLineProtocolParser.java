// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data.influx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.common.Const;
import net.opentsdb.data.LowLevelMetricData.ValueFormat;
import net.opentsdb.utils.DateTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class })
public class TestInfluxLineProtocolParser {
  
  @Test
  public void singleLineTagsOneFieldTimestamp() throws Exception {
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsOneFieldTimestampBlankspace() throws Exception {
    String msg = " sys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200  ";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsOneFieldNoTimestamp() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.currentTimeMillis()).thenReturn(1234123456789L);
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1234123456789L, parser.timestamp().msEpoch());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsOneFieldNoTimestampBlankspace() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.currentTimeMillis()).thenReturn(1234123456789L);
    String msg = "  sys.if,tagKey=Value,tagk2=Value2 in=10.24  ";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1234123456789L, parser.timestamp().msEpoch());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineNoTagsTimestamp() throws Exception {
    String msg = "sys.if in=10.24 1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineNoTagsNoTimestamp() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.currentTimeMillis()).thenReturn(1234123456789L);
    String msg = "sys.if in=10.24";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1234123456789L, parser.timestamp().msEpoch());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsMultipleFieldsTimestamp() throws Exception {
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24,out=42i,err=.05 1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    // next field
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.out", new String(parser.metricBuffer(), 
        parser.metricStart(), 
        parser.metricLength(), 
        Const.UTF8_CHARSET));
    assertEquals(ValueFormat.INTEGER, parser.valueFormat());
    assertEquals(42, parser.longValue());
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.err", new String(parser.metricBuffer(), 
        parser.metricStart(), 
        parser.metricLength(), 
        Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(0.05, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsHashedDifferentOrder() throws Exception {
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24,out=42i,err=.05 1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.computeHashes(true);
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    long tags_hash = parser.tagsSetHash();
    long m1_hash = parser.metricHash();
    
    // next field
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.out", new String(parser.metricBuffer(), 
        parser.metricStart(), 
        parser.metricLength(), 
        Const.UTF8_CHARSET));
    assertEquals(ValueFormat.INTEGER, parser.valueFormat());
    assertEquals(42, parser.longValue());
    assertTags(parser);
    assertEquals(tags_hash, parser.tagsSetHash());
    long m2_hash = parser.metricHash();
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.err", new String(parser.metricBuffer(), 
        parser.metricStart(), 
        parser.metricLength(), 
        Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(0.05, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertEquals(tags_hash, parser.tagsSetHash());
    long m3_hash = parser.metricHash();
    
    assertFalse(parser.advance());
    
    // change tag order
    msg = "sys.if,tagk2=Value2,tagKey=Value in=10.24,out=42i,err=.05 1465839830100400200";
    parser = new InfluxLineProtocolParser();
    parser.computeHashes(true);
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    
    assertEquals(tags_hash, parser.tagsSetHash());
    assertEquals(m1_hash, parser.metricHash());
    
    assertTrue(parser.advance());
    assertEquals(tags_hash, parser.tagsSetHash());
    assertEquals(m2_hash, parser.metricHash());
    
    assertTrue(parser.advance());
    assertEquals(tags_hash, parser.tagsSetHash());
    assertEquals(m3_hash, parser.metricHash());
    
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineMalformed() throws Exception {
    // no value for field
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // no equals after field name.
    msg = "sys.if,tagKey=Value,tagk2=Value2 in";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // no fields
    msg = "sys.if,tagKey=Value,tagk2=Value2";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // no fields... but a timestamp??
    msg = "sys.if,tagKey=Value,tagk2=Value2 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // ends with tag value missing
    msg = "sys.if,tagKey=Value,tagk2=";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // whoops nulled tag value
    msg = "sys.if,tagKey=Value,tagk2= in=10.24 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // ends with tag value missing
    msg = "sys.if,tagKey=Value,tagk2";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // whoops nulled tag value
    msg = "sys.if,tagKey=Value,tagk2 in=10.24 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // ok, yeah, missing a key. Ew
    msg = "sys.if,tagKey=Value,=value2 in=10.24 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // just a measurement and timestamp
    msg = "sys.if 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // just a measurement?
    msg = "sys.if";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // shoulda beena comment
    msg = "This is a measurement from our Internet Scale app!";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsOneFieldTimestampTabs() throws Exception {
    // not to spec but hey, let's be kind.
    String msg = "sys.if,tagKey=Value,tagk2=Value2\tin=10.24\t1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedMetric() throws Exception {
    // not to spec but hey, let's be kind.
    String msg = "sys\\,\\ if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys, if.in", new String(parser.metricBuffer(), 
                                          parser.metricStart(), 
                                          parser.metricLength(), 
                                          Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedTagKeys() throws Exception {
    String msg = "sys.if,tag\\ Key=Value,tag\\=k\\,2=Value2 in=10.24 1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTrue(parser.advanceTagPair());
    assertEquals("tag Key", new String(parser.tagsBuffer(),
                                      parser.tagKeyStart(),
                                      parser.tagKeyLength(),
                                      Const.UTF8_CHARSET));
    assertEquals("Value", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueLength(),
                                     Const.UTF8_CHARSET));
    assertTrue(parser.advanceTagPair());
    assertEquals("tag=k,2", new String(parser.tagsBuffer(),
                                     parser.tagKeyStart(),
                                     parser.tagKeyLength(),
                                     Const.UTF8_CHARSET));
    assertEquals("Value2", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueLength(),
                                     Const.UTF8_CHARSET));
    assertFalse(parser.advanceTagPair());
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedTagValues() throws Exception {
    String msg = "sys.if,tagKey=Va\\ lue,tagk2=Va\\=lu\\,e2 in=10.24 1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTrue(parser.advanceTagPair());
    assertEquals("tagKey", new String(parser.tagsBuffer(),
                                      parser.tagKeyStart(),
                                      parser.tagKeyLength(),
                                      Const.UTF8_CHARSET));
    assertEquals("Va lue", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueLength(),
                                     Const.UTF8_CHARSET));
    assertTrue(parser.advanceTagPair());
    assertEquals("tagk2", new String(parser.tagsBuffer(),
                                     parser.tagKeyStart(),
                                     parser.tagKeyLength(),
                                     Const.UTF8_CHARSET));
    assertEquals("Va=lu,e2", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueLength(),
                                     Const.UTF8_CHARSET));
    assertFalse(parser.advanceTagPair());
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedTagKeysAndValues() throws Exception {
    String msg = "sys.if,tag\\ Key=V\\a\\ lue,ta\\=gk\\,2=Va\\=lu\\,e2 in=10.24 1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTrue(parser.advanceTagPair());
    assertEquals("tag Key", new String(parser.tagsBuffer(),
                                      parser.tagKeyStart(),
                                      parser.tagKeyLength(),
                                      Const.UTF8_CHARSET));
    assertEquals("V\\a lue", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueLength(),
                                     Const.UTF8_CHARSET));
    assertTrue(parser.advanceTagPair());
    assertEquals("ta=gk,2", new String(parser.tagsBuffer(),
                                     parser.tagKeyStart(),
                                     parser.tagKeyLength(),
                                     Const.UTF8_CHARSET));
    assertEquals("Va=lu,e2", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueLength(),
                                     Const.UTF8_CHARSET));
    assertFalse(parser.advanceTagPair());
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedField() throws Exception {
    // not to spec but hey, let's be kind.
    String msg = "sys.if,tagKey=Value,tagk2=Value2 i\\,\\ n=10.24,te\\=st=42 1465839830100400200";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.i, n", new String(parser.metricBuffer(), 
                                          parser.metricStart(), 
                                          parser.metricLength(), 
                                          Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertTrue(parser.advance());
    
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.te=st", new String(parser.metricBuffer(), 
                                          parser.metricStart(), 
                                          parser.metricLength(), 
                                          Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(42, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void newLinesBeforeAndAfter() throws Exception {
    String msg = "\n\nsys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200\n";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void newLinesBeforeAndAfterWithBlankSpace() throws Exception {
    String msg = "  \n  \nsys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200\n  \n";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void CommentsBeforeAndAfterWithBlankSpace() throws Exception {
    String msg = "# ignore this line\nsys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200\n# and this";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void twoLinesTagsTwoFieldsTimestamp() throws Exception {
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24,out=42i 1465839830100400200\n"
        + "sys.cpu,tagKey=Value,tagk2=Value2 sys=-1.234456e+78,user=41 1465839830100000000\n";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.out", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.INTEGER, parser.valueFormat());
    assertEquals(42, parser.longValue());
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100000000, parser.timestamp.nanos());
    assertEquals("sys.cpu.sys", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(-1.234456e+78, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100000000, parser.timestamp.nanos());
    assertEquals("sys.cpu.user", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(41, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertFalse(parser.advance());
  }
  
  @Test
  public void twoLinesTagsTwoFieldsTimestampCommentInBetween() throws Exception {
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24,out=42i 1465839830100400200\n"
        + "# comment\n"
        + "sys.cpu,tagKey=Value,tagk2=Value2 sys=-1.234456e+78,user=41 1465839830100000000\n";
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100400200, parser.timestamp.nanos());
    assertEquals("sys.if.out", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.INTEGER, parser.valueFormat());
    assertEquals(42, parser.longValue());
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100000000, parser.timestamp.nanos());
    assertEquals("sys.cpu.sys", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(-1.234456e+78, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830, parser.timestamp().epoch());
    assertEquals(100000000, parser.timestamp.nanos());
    assertEquals("sys.cpu.user", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricLength(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(41, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertFalse(parser.advance());
  }
  
  @Test
  public void exampleTelegrafPayload() throws Exception {
    String msg = "cpu,cpu=cpu12,host=localhost usage_guest_nice=0,usage_idle=42.4424424424307,usage_nice=0,usage_iowait=0,usage_irq=0,usage_guest=0,usage_user=52.652652652661025,usage_system=4.9049049049059965,usage_softirq=0,usage_steal=0 1603905190000000000\n" + 
        "cpu,cpu=cpu13,host=localhost usage_guest=0,usage_user=17.51751751752109,usage_nice=0,usage_irq=0,usage_softirq=0,usage_steal=0,usage_system=1.5015015015018647,usage_idle=80.98098098096254,usage_iowait=0,usage_guest_nice=0 1603905190000000000\n" + 
        "cpu,cpu=cpu14,host=localhost usage_user=52.80000000003587,usage_system=3.2000000000016917,usage_idle=43.99999999997381,usage_nice=0,usage_iowait=0,usage_guest=0,usage_guest_nice=0,usage_irq=0,usage_softirq=0,usage_steal=0 1603905190000000000\n" + 
        "cpu,cpu=cpu15,host=localhost usage_steal=0,usage_guest=0,usage_user=16.383616383624823,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_guest_nice=0,usage_system=1.6983016983026042,usage_idle=81.91808191809571,usage_nice=0 1603905190000000000\n" + 
        "cpu,cpu=cpu-total,host=localhost usage_idle=62.03125,usage_steal=0,usage_guest=0,usage_user=33.956249999996544,usage_system=4.0125000000000455,usage_nice=0,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_guest_nice=0 1603905190000000000\n" + 
        "system,host=localhost load5=2.732421875,load15=2.3037109375,n_cpus=16i,n_users=17i,load1=3.251953125 1603905190000000000\n" + 
        "system,host=localhost uptime=138648i 1603905190000000000\n" + 
        "system,host=localhost uptime_format=\"1 day, 14:30\" 1603905190000000000\n" + 
        "processes,host=localhost idle=0i,blocked=0i,zombies=2i,stopped=0i,running=3i,sleeping=573i,total=578i,unknown=0i 1603905190000000000\n" + 
        "swap,host=localhost total=0i,used=0i,free=0i,used_percent=0 1603905200000000000\n" + 
        "swap,host=localhost in=0i,out=0i 1603905200000000000\n" + 
        "mem,host=localhost total=34359738368i,active=14224404480i,free=28590080i,wired=3467272192i,available=14243762176i,used=20115976192i,used_percent=58.54519605636597,available_percent=41.45480394363403,inactive=14215172096i 1603905200000000000\n" + 
        "cpu,cpu=cpu0,host=localhost usage_idle=76.52347652347878,usage_irq=0,usage_user=15.684315684320985,usage_system=7.792207792209326,usage_nice=0,usage_iowait=0,usage_softirq=0,usage_steal=0,usage_guest=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu1,host=localhost usage_irq=0,usage_softirq=0,usage_steal=0,usage_guest=0,usage_user=0.8982035928144086,usage_system=2.1956087824358197,usage_nice=0,usage_idle=96.90618762477274,usage_iowait=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu2,host=localhost usage_irq=0,usage_steal=0,usage_user=12.899999999999636,usage_system=6.399999999998727,usage_nice=0,usage_iowait=0,usage_idle=80.69999999999709,usage_softirq=0,usage_guest=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu3,host=localhost usage_system=1.8018018018022375,usage_nice=0,usage_iowait=0,usage_irq=0,usage_steal=0,usage_guest_nice=0,usage_user=0.8008008008008047,usage_idle=97.39739739737648,usage_softirq=0,usage_guest=0 1603905200000000000\n" + 
        "cpu,cpu=cpu4,host=localhost usage_idle=82.90000000000873,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_steal=0,usage_user=11.899999999995998,usage_nice=0,usage_guest=0,usage_guest_nice=0,usage_system=5.199999999999818 1603905200000000000\n" + 
        "cpu,cpu=cpu5,host=localhost usage_guest=0,usage_user=0.8008008008002215,usage_system=1.7017017017006841,usage_idle=97.49749749749881,usage_nice=0,usage_irq=0,usage_softirq=0,usage_iowait=0,usage_steal=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu6,host=localhost usage_softirq=0,usage_steal=0,usage_guest_nice=0,usage_system=4.700000000000273,usage_nice=0,usage_iowait=0,usage_guest=0,usage_user=9.700000000002547,usage_idle=85.59999999997672,usage_irq=0 1603905200000000000\n" + 
        "cpu,cpu=cpu7,host=localhost usage_idle=97.5024975024962,usage_nice=0,usage_softirq=0,usage_guest_nice=0,usage_user=0.8991008991011198,usage_system=1.598401598402401,usage_iowait=0,usage_irq=0,usage_steal=0,usage_guest=0 1603905200000000000\n" + 
        "cpu,cpu=cpu8,host=localhost usage_user=8.291708291705877,usage_iowait=0,usage_softirq=0,usage_guest_nice=0,usage_system=3.896103896101831,usage_idle=87.81218781217866,usage_nice=0,usage_irq=0,usage_steal=0,usage_guest=0 1603905200000000000\n" + 
        "cpu,cpu=cpu9,host=localhost usage_user=0.7999999999998408,usage_system=1.4999999999999147,usage_idle=97.70000000004075,usage_irq=0,usage_guest=0,usage_nice=0,usage_iowait=0,usage_softirq=0,usage_steal=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu10,host=localhost usage_idle=86.88688688690752,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_guest=0,usage_user=9.509509509509629,usage_system=3.603603603604475,usage_nice=0,usage_steal=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu11,host=localhost usage_nice=0,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_user=0.8000000000004093,usage_system=1.2999999999999545,usage_idle=97.90000000000873,usage_steal=0,usage_guest=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu12,host=localhost usage_system=3.0969030969041724,usage_nice=0,usage_iowait=0,usage_irq=0,usage_steal=0,usage_guest=0,usage_user=7.092907092911168,usage_idle=89.81018981021646,usage_softirq=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu13,host=localhost usage_irq=0,usage_softirq=0,usage_guest=0,usage_user=0.7992007992010585,usage_idle=98.00199800202603,usage_nice=0,usage_iowait=0,usage_system=1.1988011988018716,usage_steal=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu14,host=localhost usage_steal=0,usage_guest=0,usage_guest_nice=0,usage_user=7.2927072927006895,usage_nice=0,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_system=3.1968031968007224,usage_idle=89.5104895104702 1603905200000000000\n" + 
        "cpu,cpu=cpu15,host=localhost usage_system=0.9999999999992155,usage_iowait=0,usage_softirq=0,usage_steal=0,usage_user=0.5999999999995862,usage_idle=98.39999999996624,usage_nice=0,usage_irq=0,usage_guest=0,usage_guest_nice=0 1603905200000000000\n" + 
        "cpu,cpu=cpu-total,host=localhost usage_user=5.548266166824163,usage_system=3.1365198375501255,usage_idle=91.31521399561434,usage_iowait=0,usage_irq=0,usage_nice=0,usage_softirq=0,usage_steal=0,usage_guest=0,usage_guest_nice=0 1603905200000000000\n" + 
        "disk,device=disk1s1,fstype=apfs,host=localhost,mode=ro,path=/ total=499963174912i,free=165971582976i,used=11227463680i,used_percent=6.336074539834346,inodes_total=4882452880i,inodes_free=4881964598i,inodes_used=488282i 1603905200000000000\n" + 
        "disk,device=disk1s5,fstype=apfs,host=localhost,mode=rw,path=/System/Volumes/Data used_percent=65.91184624243071,inodes_total=4882452880i,inodes_free=4880521999i,inodes_used=1930881i,total=499963174912i,free=165971582976i,used=320917745664i 1603905200000000000\n" + 
        "disk,device=disk1s4,fstype=apfs,host=localhost,mode=rw,path=/private/var/vm free=165971582976i,used=1073762304i,used_percent=0.6427969017635115,inodes_total=4882452880i,inodes_free=4882452879i,inodes_used=1i,total=499963174912i 1603905200000000000\n" + 
        "disk,device=disk1s3,fstype=apfs,host=localhost,mode=rw,path=/Volumes/Recovery used=528900096i,used_percent=0.3176567936870712,inodes_total=4882452880i,inodes_free=4882452830i,inodes_used=50i,total=499963174912i,free=165971582976i 1603905200000000000\n" + 
        "system,host=localhost load5=2.78955078125,load15=2.3291015625,n_cpus=16i,n_users=17i,load1=3.42724609375 1603905200000000000\n" + 
        "system,host=localhost uptime=138658i 1603905200000000000\n" + 
        "diskio,host=localhost,name=disk0 read_time=619658i,io_time=2627047i,weighted_io_time=0i,iops_in_progress=0i,reads=1684427i,writes=2318667i,read_bytes=30730891264i,write_bytes=43480100864i,merged_reads=0i,merged_writes=0i,write_time=2007388i 1603905200000000000\n" + 
        "system,host=localhost uptime_format=\"1 day, 14:30\" 1603905200000000000\n" + 
        "processes,host=localhost sleeping=575i,total=578i,unknown=0i,idle=0i,blocked=0i,zombies=2i,stopped=0i,running=1i 1603905200000000000\n" + 
        "swap,host=localhost total=0i,used=0i,free=0i,used_percent=0 1603905210000000000\n" + 
        "swap,host=localhost in=0i,out=0i 1603905210000000000\n" + 
        "mem,host=localhost wired=3471589376i,used=19570159616i,used_percent=56.956660747528076,active=13673021440i,free=1131094016i,inactive=13658484736i,total=34359738368i,available=14789578752i,available_percent=43.043339252471924 1603905210000000000\n" + 
        "cpu,cpu=cpu0,host=localhost usage_nice=0,usage_softirq=0,usage_steal=0,usage_guest_nice=0,usage_user=11.81181181180853,usage_system=6.506506506508557,usage_idle=81.68168168167381,usage_iowait=0,usage_irq=0,usage_guest=0 1603905210000000000\n" + 
        "cpu,cpu=cpu1,host=localhost usage_idle=99.89989989987956,usage_irq=0,usage_guest_nice=0,usage_user=0,usage_system=0.10010010009995657,usage_nice=0,usage_iowait=0,usage_softirq=0,usage_steal=0,usage_guest=0 1603905210000000000\n" + 
        "cpu,cpu=cpu2,host=localhost usage_user=9.799999999995634,usage_system=4.400000000000546,usage_iowait=0,usage_softirq=0,usage_guest_nice=0,usage_idle=85.80000000001746,usage_nice=0,usage_irq=0,usage_steal=0,usage_guest=0 1603905210000000000\n" + 
        "cpu,cpu=cpu3,host=localhost usage_guest_nice=0,usage_system=0.09999999999990905,usage_nice=0,usage_iowait=0,usage_steal=0,usage_guest=0,usage_user=0.09999999999990905,usage_idle=99.80000000003201,usage_irq=0,usage_softirq=0 1603905210000000000\n" + 
        "cpu,cpu=cpu4,host=localhost usage_user=7.492507492505967,usage_system=3.296703296701899,usage_iowait=0,usage_irq=0,usage_idle=89.21078921077397,usage_nice=0,usage_softirq=0,usage_steal=0,usage_guest=0,usage_guest_nice=0 1603905210000000000\n" + 
        "cpu,cpu=cpu5,host=localhost usage_idle=99.80000000003187,usage_irq=0,usage_guest_nice=0,usage_user=0.09999999999998181,usage_system=0.09999999999998181,usage_softirq=0,usage_steal=0,usage_guest=0,usage_nice=0,usage_iowait=0 1603905210000000000\n" + 
        "disk,device=disk1s1,fstype=apfs,host=localhost,mode=ro,path=/ inodes_free=4881964598i,inodes_used=488282i,total=499963174912i,free=165977980928i,used=11227463680i,used_percent=6.335845777671513,inodes_total=4882452880i 1603905210000000000\n" + 
        "disk,device=disk1s5,fstype=apfs,host=localhost,mode=rw,path=/System/Volumes/Data used=320911347712i,used_percent=65.91053219596807,inodes_total=4882452880i,inodes_free=4880521999i,inodes_used=1930881i,total=499963174912i,free=165977980928i 1603905210000000000\n" + 
        "cpu,cpu=cpu6,host=localhost usage_idle=93.49999999999018,usage_nice=0,usage_iowait=0,usage_softirq=0,usage_guest=0,usage_user=4.399999999992797,usage_system=2.0999999999988357,usage_guest_nice=0,usage_irq=0,usage_steal=0 1603905210000000000\n" + 
        "cpu,cpu=cpu7,host=localhost usage_idle=100,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_steal=0,usage_guest=0,usage_user=0,usage_system=0,usage_guest_nice=0,usage_nice=0 1603905210000000000\n" + 
        "disk,device=disk1s4,fstype=apfs,host=localhost,mode=rw,path=/private/var/vm inodes_used=1i,total=499963174912i,free=165977980928i,used=1073762304i,used_percent=0.6427722831414984,inodes_total=4882452880i,inodes_free=4882452879i 1603905210000000000\n" + 
        "cpu,cpu=cpu8,host=localhost usage_user=4.200000000000728,usage_nice=0,usage_iowait=0,usage_steal=0,usage_guest=0,usage_system=1.5000000000009095,usage_idle=94.30000000000291,usage_irq=0,usage_softirq=0,usage_guest_nice=0 1603905210000000000\n" + 
        "disk,device=disk1s3,fstype=apfs,host=localhost,mode=rw,path=/Volumes/Recovery free=165977980928i,used=528900096i,used_percent=0.31764458786767213,inodes_total=4882452880i,inodes_free=4882452830i,inodes_used=50i,total=499963174912i 1603905210000000000\n" + 
        "cpu,cpu=cpu9,host=localhost usage_idle=99.89999999997963,usage_nice=0,usage_iowait=0,usage_softirq=0,usage_guest=0,usage_user=0.09999999999990905,usage_system=0,usage_guest_nice=0,usage_irq=0,usage_steal=0 1603905210000000000\n" + 
        "cpu,cpu=cpu10,host=localhost usage_system=1.3999999999998636,usage_iowait=0,usage_steal=0,usage_guest=0,usage_user=3.000000000001819,usage_nice=0,usage_irq=0,usage_softirq=0,usage_guest_nice=0,usage_idle=95.59999999997672 1603905210000000000\n" + 
        "cpu,cpu=cpu11,host=localhost usage_user=0.09990009990006132,usage_system=0.09990009990020329,usage_iowait=0,usage_guest_nice=0,usage_steal=0,usage_guest=0,usage_idle=99.80019980023168,usage_nice=0,usage_irq=0,usage_softirq=0 1603905210000000000\n" + 
        "cpu,cpu=cpu12,host=localhost usage_nice=0,usage_irq=0,usage_steal=0,usage_guest_nice=0,usage_softirq=0,usage_guest=0,usage_user=3.0999999999971988,usage_system=0.6999999999999909,usage_idle=96.1999999999562,usage_iowait=0 1603905210000000000\n" + 
        "cpu,cpu=cpu13,host=localhost usage_irq=0,usage_steal=0,usage_guest=0,usage_guest_nice=0,usage_system=0.09999999999983629,usage_idle=99.8999999999797,usage_nice=0,usage_user=0,usage_iowait=0,usage_softirq=0 1603905210000000000\n" + 
        "cpu,cpu=cpu14,host=localhost usage_idle=97.497497497497,usage_nice=0,usage_irq=0,usage_steal=0,usage_guest_nice=0,usage_system=0.4004004004001179,usage_iowait=0,usage_softirq=0,usage_guest=0,usage_user=2.102102102102895 1603905210000000000\n" + 
        "cpu,cpu=cpu15,host=localhost usage_user=0,usage_irq=0,usage_softirq=0,usage_guest=0,usage_system=0.10010010010017173,usage_idle=99.89989989987949,usage_nice=0,usage_iowait=0,usage_steal=0,usage_guest_nice=0 1603905210000000000\n" + 
        "cpu,cpu=cpu-total,host=localhost usage_user=2.8944736184036377,usage_system=1.3065766441614355,usage_idle=95.79894973745084,usage_guest=0,usage_guest_nice=0,usage_nice=0,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_steal=0 1603905210000000000\n" + 
        "system,host=localhost n_cpus=16i,n_users=17i,load1=3.126953125,load5=2.74658203125,load15=2.3193359375 1603905210000000000\n" + 
        "system,host=localhost uptime=138668i 1603905210000000000\n" + 
        "system,host=localhost uptime_format=\"1 day, 14:31\" 1603905210000000000\n" + 
        "diskio,host=localhost,name=disk0 read_bytes=30768029696i,write_bytes=43498524672i,weighted_io_time=0i,merged_writes=0i,merged_reads=0i,reads=1684866i,writes=2318793i,read_time=619846i,write_time=2007495i,io_time=2627342i,iops_in_progress=0i 1603905210000000000\n" + 
        "processes,host=localhost total=577i,unknown=0i,idle=0i,blocked=0i,zombies=2i,stopped=0i,running=1i,sleeping=574i 1603905210000000000\n" + 
        "swap,host=localhost total=0i,used=0i,free=0i,used_percent=0 1603905220000000000\n" + 
        "swap,host=localhost in=0i,out=0i 1603905220000000000\n" + 
        "mem,host=localhost free=1042231296i,wired=3479437312i,used_percent=57.114291191101074,active=13720211456i,used=19624321024i,available_percent=42.885708808898926,inactive=13693186048i,total=34359738368i,available=14735417344i 1603905220000000000\n" + 
        "cpu,cpu=cpu0,host=localhost usage_softirq=0,usage_user=13.600000000015717,usage_iowait=0,usage_nice=0,usage_irq=0,usage_steal=0,usage_guest=0,usage_guest_nice=0,usage_system=6.100000000001164,usage_idle=80.30000000004678 1603905220000000000\n" + 
        "disk,device=disk1s1,fstype=apfs,host=localhost,mode=ro,path=/ used_percent=6.335847242165471,inodes_total=4882452880i,inodes_free=4881964598i,inodes_used=488282i,total=499963174912i,free=165977939968i,used=11227463680i 1603905220000000000\n" + 
        "disk,device=disk1s5,fstype=apfs,host=localhost,mode=rw,path=/System/Volumes/Data inodes_free=4880521997i,inodes_used=1930883i,total=499963174912i,free=165977939968i,used=320911388672i,used_percent=65.91054060855747,inodes_total=4882452880i 1603905220000000000\n" + 
        "cpu,cpu=cpu1,host=localhost usage_idle=99.79999999995925,usage_iowait=0,usage_guest=0,usage_user=0.09999999999990905,usage_nice=0,usage_irq=0,usage_softirq=0,usage_steal=0,usage_guest_nice=0,usage_system=0.10000000000019327 1603905220000000000\n" + 
        "disk,device=disk1s4,fstype=apfs,host=localhost,mode=rw,path=/private/var/vm inodes_total=4882452880i,inodes_free=4882452879i,inodes_used=1i,total=499963174912i,free=165977939968i,used=1073762304i,used_percent=0.6427724407451167 1603905220000000000\n" + 
        "cpu,cpu=cpu2,host=localhost usage_user=11.300000000001091,usage_iowait=0,usage_steal=0,usage_guest=0,usage_system=3.8999999999987267,usage_idle=84.79999999995925,usage_nice=0,usage_irq=0,usage_softirq=0,usage_guest_nice=0 1603905220000000000\n" + 
        "cpu,cpu=cpu3,host=localhost usage_nice=0,usage_irq=0,usage_steal=0,usage_guest=0,usage_user=0,usage_system=0.09999999999990905,usage_softirq=0,usage_guest_nice=0,usage_idle=99.89999999997963,usage_iowait=0 1603905220000000000\n" + 
        "disk,device=disk1s3,fstype=apfs,host=localhost,mode=rw,path=/Volumes/Recovery used=528900096i,used_percent=0.31764466600693847,inodes_total=4882452880i,inodes_free=4882452830i,inodes_used=50i,total=499963174912i,free=165977939968i 1603905220000000000\n" + 
        "cpu,cpu=cpu4,host=localhost usage_system=2.9999999999972715,usage_idle=89.80000000003201,usage_nice=0,usage_irq=0,usage_softirq=0,usage_guest=0,usage_guest_nice=0,usage_user=7.200000000002547,usage_steal=0,usage_iowait=0 1603905220000000000\n" + 
        "cpu,cpu=cpu5,host=localhost usage_system=0.10000000000019327,usage_idle=99.89999999997963,usage_nice=0,usage_guest=0,usage_user=0,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_steal=0,usage_guest_nice=0 1603905220000000000\n" + 
        "cpu,cpu=cpu6,host=localhost usage_system=2.2000000000018733,usage_nice=0,usage_steal=0,usage_guest=0,usage_guest_nice=0,usage_user=5.900000000005748,usage_idle=91.90000000001739,usage_iowait=0,usage_irq=0,usage_softirq=0 1603905220000000000\n" + 
        "cpu,cpu=cpu7,host=localhost usage_system=0.09990009990020329,usage_idle=99.80019980023168,usage_nice=0,usage_iowait=0,usage_irq=0,usage_softirq=0,usage_user=0.09990009990006132,usage_steal=0,usage_guest=0,usage_guest_nice=0 1603905220000000000\n" + 
        "cpu,cpu=cpu8,host=localhost usage_guest_nice=0,usage_system=1.5999999999985448,usage_idle=94.19999999998254,usage_nice=0,usage_softirq=0,usage_guest=0,usage_user=4.19999999999618,usage_iowait=0,usage_irq=0,usage_steal=0 1603905220000000000";
    
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    int read = 0;
    while (parser.advance()) {
      read++;
    }
    assertEquals(673, read);
  }
  
  void print(InfluxLineProtocolParser influx) {
    System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    //System.out.println(influx.hasNext());
    System.out.println("    Metric: " + new String(influx.metricBuffer(), influx.metricStart(), influx.metricLength()));
    System.out.println("    Tags: " + new String(influx.tagsBuffer()));
    System.out.println("    Timestamp: " + influx.timestamp);
    System.out.println("    Type: " + influx.valueFormat());
    if (influx.valueFormat() == ValueFormat.INTEGER) {
      System.out.println("    INT: " + influx.longValue());
    } else {
      System.out.println("    DOUBLE: " + influx.doubleValue());
    }
    System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
  }

  void assertTags(final InfluxLineProtocolParser parser) {
    assertTrue(parser.advanceTagPair());
    assertEquals("tagKey", new String(parser.tagsBuffer(),
                                      parser.tagKeyStart(),
                                      parser.tagKeyLength(),
                                      Const.UTF8_CHARSET));
    assertEquals("Value", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueLength(),
                                     Const.UTF8_CHARSET));
    assertTrue(parser.advanceTagPair());
    assertEquals("tagk2", new String(parser.tagsBuffer(),
                                     parser.tagKeyStart(),
                                     parser.tagKeyLength(),
                                     Const.UTF8_CHARSET));
    assertEquals("Value2", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueLength(),
                                     Const.UTF8_CHARSET));
    assertFalse(parser.advanceTagPair());
  }
}