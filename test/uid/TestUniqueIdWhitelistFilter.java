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
package net.opentsdb.uid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
               "ch.qos.*", "org.slf4j.*", "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, Config.class })
public class TestUniqueIdWhitelistFilter {

  private TSDB tsdb;
  private Config config;
  private UniqueIdWhitelistFilter filter;
  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    config = new Config(false);
    when(tsdb.getConfig()).thenReturn(config);
    filter = new UniqueIdWhitelistFilter();
    
    config.overrideConfig("tsd.uidfilter.whitelist.metric_patterns", ".*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagk_patterns", ".*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagv_patterns", ".*");
  }
  
  @Test
  public void ctor() throws Exception {
    assertNull(filter.metricPatterns());
    assertNull(filter.tagkPatterns());
    assertNull(filter.tagvPatterns());
  }
  
  @Test
  public void initalize() throws Exception {
    filter.initialize(tsdb);
    assertEquals(1, filter.metricPatterns().size());
    assertEquals(".*", filter.metricPatterns().get(0).pattern());
    assertEquals(1, filter.tagkPatterns().size());
    assertEquals(".*", filter.tagkPatterns().get(0).pattern());
    assertEquals(1, filter.tagvPatterns().size());
    assertEquals(".*", filter.tagvPatterns().get(0).pattern());
  }
  
  @Test
  public void initalizeMultiplePatterns() throws Exception {
    config.overrideConfig("tsd.uidfilter.whitelist.metric_patterns", ".*,^test.*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagk_patterns", ".*,^test.*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagv_patterns", ".*,^test.*");
    filter.initialize(tsdb);
    assertEquals(2, filter.metricPatterns().size());
    assertEquals(".*", filter.metricPatterns().get(0).pattern());
    assertEquals("^test.*", filter.metricPatterns().get(1).pattern());
    assertEquals(2, filter.tagkPatterns().size());
    assertEquals(".*", filter.tagkPatterns().get(0).pattern());
    assertEquals("^test.*", filter.tagkPatterns().get(1).pattern());
    assertEquals(2, filter.tagvPatterns().size());
    assertEquals(".*", filter.tagvPatterns().get(0).pattern());
    assertEquals("^test.*", filter.tagvPatterns().get(1).pattern());
  }
  
  @Test
  public void initalizeMultiplePatternsAlternateDelimiter() throws Exception {
    config.overrideConfig("tsd.uidfilter.whitelist.delimiter", "\\|");
    config.overrideConfig("tsd.uidfilter.whitelist.metric_patterns", ".*|^test.*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagk_patterns", ".*|^test.*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagv_patterns", ".*|^test.*");
    filter.initialize(tsdb);
    assertEquals(2, filter.metricPatterns().size());
    assertEquals(".*", filter.metricPatterns().get(0).pattern());
    assertEquals("^test.*", filter.metricPatterns().get(1).pattern());
    assertEquals(2, filter.tagkPatterns().size());
    assertEquals(".*", filter.tagkPatterns().get(0).pattern());
    assertEquals("^test.*", filter.tagkPatterns().get(1).pattern());
    assertEquals(2, filter.tagvPatterns().size());
    assertEquals(".*", filter.tagvPatterns().get(0).pattern());
    assertEquals("^test.*", filter.tagvPatterns().get(1).pattern());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initalizeBadRegex() throws Exception {
    config.overrideConfig("tsd.uidfilter.whitelist.metric_patterns", "grp[start");
    filter.initialize(tsdb);
  }

  @Test
  public void shutdown() throws Exception {
    assertNull(filter.shutdown().join());
  }
  
  @Test
  public void allowUIDAssignment() throws Exception {
    config.overrideConfig("tsd.uidfilter.whitelist.metric_patterns", "^test.*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagk_patterns", "^test.*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagv_patterns", "^test.*");
    filter.initialize(tsdb);
    assertTrue(filter.allowUIDAssignment(UniqueIdType.METRIC, "test_metric", 
        null, null).join());
    assertFalse(filter.allowUIDAssignment(UniqueIdType.METRIC, "metric", 
        null, null).join());
    assertTrue(filter.allowUIDAssignment(UniqueIdType.TAGK, "test_tagk", 
        null, null).join());
    assertFalse(filter.allowUIDAssignment(UniqueIdType.TAGK, "tagk", 
        null, null).join());
    assertTrue(filter.allowUIDAssignment(UniqueIdType.TAGV, "test_tagv", 
        null, null).join());
    assertFalse(filter.allowUIDAssignment(UniqueIdType.TAGV, "tagv", 
        null, null).join());
  }
  
  @Test
  public void allowUIDAssignmentMultiplePaterns() throws Exception {
    config.overrideConfig("tsd.uidfilter.whitelist.metric_patterns", ".*,^test.*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagk_patterns", ".*,^test.*");
    config.overrideConfig("tsd.uidfilter.whitelist.tagv_patterns", ".*,^test.*");
    filter.initialize(tsdb);
    assertTrue(filter.allowUIDAssignment(UniqueIdType.METRIC, "test_metric", 
        null, null).join());
    assertFalse(filter.allowUIDAssignment(UniqueIdType.METRIC, "metric", 
        null, null).join());
    assertTrue(filter.allowUIDAssignment(UniqueIdType.TAGK, "test_tagk", 
        null, null).join());
    assertFalse(filter.allowUIDAssignment(UniqueIdType.TAGK, "tagk", 
        null, null).join());
    assertTrue(filter.allowUIDAssignment(UniqueIdType.TAGV, "test_tagv", 
        null, null).join());
    assertFalse(filter.allowUIDAssignment(UniqueIdType.TAGV, "tagv", 
        null, null).join());
  }
  
  @Test
  public void fillterUIDAssignments() throws Exception {
    assertTrue(filter.fillterUIDAssignments());
  }
}
