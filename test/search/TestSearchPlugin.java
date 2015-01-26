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
package net.opentsdb.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Maps;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TsdbBuilder;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.stumbleupon.async.Callback;

import java.util.Map;

public final class TestSearchPlugin {
  private TSDB tsdb;
  private Config config;
  private SearchPlugin search;
  
  @Before
  public void before() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.search.DummySearchPlugin.hosts", "localhost");
    overrides.put("tsd.search.DummySearchPlugin.port", "42");
    config = new Config(false, overrides);

    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(new MemoryStore())
            .build();

    // setups a good default for the config
    search = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.search.DummySearchPlugin", SearchPlugin.class);
  }
  
  @Test
  public void initialize() throws Exception {
    search.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeMissingHost() throws Exception {
    tsdb = TsdbBuilder.createFromConfig(new Config(false))
            .withStore(new MemoryStore())
            .withSearchPlugin(search)
            .build();
    search.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeEmptyHost() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.search.DummySearchPlugin.hosts", "");
    config = new Config(false, overrides);
    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(new MemoryStore())
            .withSearchPlugin(search)
            .build();

    search.initialize(tsdb);
  }
  
  @Test (expected = NumberFormatException.class)
  public void initializeMissingPort() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.search.DummySearchPlugin.hosts", "localhost");
    config = new Config(false, overrides);
    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(new MemoryStore())
            .withSearchPlugin(search)
            .build();

    search.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeInvalidPort() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.search.DummySearchPlugin.port", "no number");
    config = new Config(false, overrides);
    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(new MemoryStore())
            .withSearchPlugin(search)
            .build();

    search.initialize(tsdb);
  }
  
  @Test
  public void shutdown() throws Exception  {
    assertNotNull(search.shutdown());
  }
  
  @Test
  public void version() throws Exception  {
    assertEquals("2.0.0", search.version());
  }
  
  @Test
  public void indexTSMeta() throws Exception  {
    assertNotNull(search.indexTSMeta(new TSMeta()));
  }
  
  @Test
  public void indexTSMetaNull() throws Exception  {
    assertNotNull(search.indexTSMeta(null));
  }
  
  @Test
  public void indexTSMetaNullErrBack() throws Exception  {
    assertNotNull(search.indexTSMeta(null).addErrback(new Errback()));
  }
  
  @Test
  public void deleteTSMeta() throws Exception  {
    assertNotNull(search.deleteTSMeta("hello"));
  }
  
  @Test
  public void deleteTSMetaNull() throws Exception  {
    assertNotNull(search.deleteTSMeta(null));
  }
  
  @Test
  public void deleteTSMetaNullErrBack() throws Exception  {
    assertNotNull(search.deleteTSMeta(null).addErrback(new Errback()));
  }
  
  @Test
  public void indexUIDMetaNull() throws Exception  {
    assertNotNull(search.indexUIDMeta(null));
  }
  
  @Test
  public void IndexUIDMetaNullErrBack() throws Exception  {
    assertNotNull(search.indexUIDMeta(null).addErrback(new Errback()));
  }
  
  @Test
  public void deleteUIDMetaNull() throws Exception  {
    assertNotNull(search.deleteUIDMeta(null));
  }
  
  @Test
  public void deleteUIDMetaNullErrBack() throws Exception  {
    assertNotNull(search.deleteUIDMeta(null).addErrback(new Errback()));
  }
  
  @Test
  public void indexAnnotation() throws Exception {
    assertNotNull(search.indexAnnotation(new Annotation()));
  }
  
  @Test
  public void indexAnnotationNull() throws Exception {
    assertNotNull(search.indexAnnotation(null));
  }
  
  @Test
  public void indexAnnotationNullErrBack() throws Exception {
    assertNotNull(search.indexAnnotation(null).addErrback(new Errback()));
  }
  
  @Test
  public void deleteAnnotation() throws Exception {
    assertNotNull(search.deleteAnnotation(new Annotation()));
  }
  
  @Test
  public void deleteAnnotationNull() throws Exception {
    assertNotNull(search.deleteAnnotation(null));
  }
  
  @Test
  public void deleteAnnotationNullErrBack() throws Exception {
    assertNotNull(search.deleteAnnotation(null).addErrback(new Errback()));
  }
  
  /**
   * Helper Deferred Errback handler just to make sure the dummy plugin (and
   * hopefully implementers) use errbacks for exceptions in the proper spots
   */
  @Ignore
  final class Errback implements Callback<Object, Exception> {
    public Object call(final Exception e) {
      assertNotNull(e);
      return new Object();
    }
  }
}
