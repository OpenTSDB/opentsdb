// This file is part of OpenTSDB.
// Copyright (C) 2013-2017 The OpenTSDB Authors.
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
package net.opentsdb.servlet.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.MetaQuery;
import net.opentsdb.query.filter.AnyFieldRegexFactory;
import net.opentsdb.query.filter.ChainFilterFactory;
import net.opentsdb.query.filter.QueryFilterFactory;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the Query RPC class that handles parsing user queries for
 * timeseries data and returning that data
 * <b>Note:</b> Testing query validation and such should be done in the
 * core.TestTSQuery and TestTSSubQuery classes
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DefaultTSDB.class, Config.class,
        Deferred.class, DeferredGroupException.class })
public final class TestMetaRpc {
  private static TSDB tsdb;
  private Configuration config;
  private MetaRpc rpc;
  private ServletConfig servlet_config;
  private ServletContext context;
  private HttpServletRequest request;
  private AsyncContext async;
  private Map<String, String> headers;
  private ObjectMapper mapper;

  @BeforeClass
  public static void beforeClass() {
    tsdb = MockTSDBDefault.getMockTSDB();

    ((MockTSDB) tsdb).registry.registerPlugin(QueryFilterFactory.class, ChainFilterFactory.TYPE, new ChainFilterFactory());
    ((MockTSDB) tsdb).registry.registerPlugin(QueryFilterFactory.class, AnyFieldRegexFactory.TYPE, new AnyFieldRegexFactory());
  }

  @Before
  public void before() throws Exception {

    config = UnitTestConfiguration.getConfiguration();
    rpc = new MetaRpc();
    servlet_config = mock(ServletConfig.class);
    context = mock(ServletContext.class);
    request = mock(HttpServletRequest.class);
    async = mock(AsyncContext.class);
    headers = Maps.newHashMap();
    mapper = new ObjectMapper();

    when(servlet_config.getServletContext()).thenReturn(context);
  }

  @Test
  public void stubTest() {
    assertTrue(true);
  }

  @Test
  public void parseQueryWithAllAggregate() throws Exception {
    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\",\"filter\":{\"type\":\"Chain\",\"filters\":[" +
            "{\"type\":\"AnyFieldRegex\",\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\",\"filter\":\"cpu\"}]}," +
            "\"aggregate_by\":\"all\"}";
    JsonNode node = mapper.readTree(request);

    MetaQuery query = MetaQuery.parse(tsdb, mapper, node).build();

    assertNotNull(query);
    assertEquals(0, query.from());
    assertEquals(10, query.to());
    assertEquals("Test-Namespace", query.namespace());
    assertEquals("Chain" , query.filters().getType());
    assertEquals(MetaQuery.AggregationField.ALL, query.aggregate_by());
    assertNull(query.aggregation_field());
  }

  @Test
  public void parseQueryWithMetricsAggregate() throws Exception {
    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\",\"filter\":{\"type\":\"Chain\",\"filters\":[" +
            "{\"type\":\"AnyFieldRegex\",\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\",\"filter\":\"cpu\"}]}," +
            "\"aggregate_by\":\"metrics\"}";
    JsonNode node = mapper.readTree(request);

    MetaQuery query = MetaQuery.parse(tsdb, mapper, node).build();

    assertNotNull(query);
    assertEquals(0, query.from());
    assertEquals(10, query.to());
    assertEquals("Test-Namespace", query.namespace());
    assertEquals("Chain" , query.filters().getType());
    assertEquals(MetaQuery.AggregationField.METRICS, query.aggregate_by());
    assertNull(query.aggregation_field());
  }

  @Test
  public void parseQueryWithtagKeysAggregate() throws Exception {
    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\",\"filter\":{\"type\":\"Chain\",\"filters\":[" +
            "{\"type\":\"AnyFieldRegex\",\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\",\"filter\":\"cpu\"}]}," +
            "\"aggregate_by\":\"tag_keys\"}";
    JsonNode node = mapper.readTree(request);

    MetaQuery query = MetaQuery.parse(tsdb, mapper, node).build();

    assertNotNull(query);
    assertEquals(0, query.from());
    assertEquals(10, query.to());
    assertEquals("Test-Namespace", query.namespace());
    assertEquals("Chain" , query.filters().getType());
    assertEquals(MetaQuery.AggregationField.TAGS_KEYS, query.aggregate_by());
    assertNull(query.aggregation_field());
  }

  @Test
  public void parseQueryWithtagValuesAggregate() throws Exception {
    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\",\"filter\":{\"type\":\"chain\",\"filters\":[" +
            "{\"type\":\"AnyFieldRegex\",\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\",\"filter\":\"cpu\"}]}" +
            ",\"aggregate_by\":\"tag_values\",\"tag_key\":\"host\"}";

    JsonNode node = mapper.readTree(request);

    MetaQuery query = MetaQuery.parse(tsdb, mapper, node).build();

    assertNotNull(query);
    assertEquals(0, query.from());
    assertEquals(10, query.to());
    assertEquals("Test-Namespace", query.namespace());
    assertEquals("Chain" , query.filters().getType());
    assertEquals(MetaQuery.AggregationField.TAGS_VALUES, query.aggregate_by());
    assertEquals("host", query.aggregation_field());
  }
}
