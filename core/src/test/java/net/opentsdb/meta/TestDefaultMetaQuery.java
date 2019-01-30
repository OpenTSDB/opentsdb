//// This file is part of OpenTSDB.
//// Copyright (C) 2018 The OpenTSDB Authors.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package net.opentsdb.meta;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.common.collect.Maps;
//import com.stumbleupon.async.Deferred;
//import com.stumbleupon.async.DeferredGroupException;
//import net.opentsdb.configuration.Configuration;
//import net.opentsdb.configuration.UnitTestConfiguration;
//import net.opentsdb.core.DefaultTSDB;
//import net.opentsdb.core.MockTSDB;
//import net.opentsdb.core.MockTSDBDefault;
//import net.opentsdb.core.TSDB;
//import net.opentsdb.meta.MetaQuery;
//import net.opentsdb.meta.MetaQuery.QueryType;
//import net.opentsdb.query.filter.AnyFieldRegexFactory;
//import net.opentsdb.query.filter.ChainFilterFactory;
//import net.opentsdb.query.filter.QueryFilterFactory;
//import net.opentsdb.utils.Config;
//import net.opentsdb.utils.JSON;
//
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import java.util.Map;
//
//import static org.junit.Assert.*;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//public class TestDefaultMetaQuery {
//  private static TSDB TSDB;
//
//  @BeforeClass
//  public static void beforeClass() {
//    TSDB = MockTSDBDefault.getMockTSDB();
//  }
//
//  @Test
//  public void parseQueryWithAllAggregate() throws Exception {
//    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\","
//        + "\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":"
//        + "\"AnyFieldRegex\",\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\","
//        + "\"filter\":\"cpu\"}]},\"type\":\"TIMESERIES\"}";
//    JsonNode node = JSON.getMapper().readTree(request);
//
//    MetaQuery query = DefaultMetaQuery.parse(TSDB, JSON.getMapper(), node).build();
//
//    assertNotNull(query);
//    assertEquals(0, query.from());
//    assertEquals(10, query.to());
//    assertEquals("Test-Namespace", query.namespace());
//    assertEquals("Chain" , query.filter().getType());
//    assertEquals(QueryType.TIMESERIES, query.type());
//  }
//
//  @Test
//  public void parseQueryWithMetricsAggregate() throws Exception {
//    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\","
//        + "\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":"
//        + "\"AnyFieldRegex\",\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\","
//        + "\"filter\":\"cpu\"}]},\"type\":\"METRICS\"}";
//    JsonNode node = JSON.getMapper().readTree(request);
//
//    MetaQuery query = DefaultMetaQuery.parse(TSDB, JSON.getMapper(), node).build();
//
//    assertNotNull(query);
//    assertEquals(0, query.from());
//    assertEquals(10, query.to());
//    assertEquals("Test-Namespace", query.namespace());
//    assertEquals("Chain" , query.filter().getType());
//    assertEquals(QueryType.METRICS, query.type());
//  }
//
//  @Test
//  public void parseQueryWithtagKeysAggregate() throws Exception {
//    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\","
//        + "\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":\"AnyFieldRegex\","
//        + "\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\",\"filter\":"
//        + "\"cpu\"}]},\"type\":\"TAG_KEYS\"}";
//    JsonNode node = JSON.getMapper().readTree(request);
//
//    MetaQuery query = DefaultMetaQuery.parse(TSDB, JSON.getMapper(), node).build();
//
//    assertNotNull(query);
//    assertEquals(0, query.from());
//    assertEquals(10, query.to());
//    assertEquals("Test-Namespace", query.namespace());
//    assertEquals("Chain" , query.filter().getType());
//    assertEquals(QueryType.TAG_KEYS, query.type());
//  }
//
//  @Test
//  public void parseQueryWithtagValuesAggregate() throws Exception {
//    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\","
//        + "\"filter\":{\"type\":\"chain\",\"filters\":[{\"type\":\"AnyFieldRegex\","
//        + "\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\",\"filter\":"
//        + "\"cpu\"}]},\"type\":\"TAG_VALUES\"}";
//    JsonNode node = JSON.getMapper().readTree(request);
//
//    MetaQuery query = DefaultMetaQuery.parse(TSDB, JSON.getMapper(), node).build();
//
//    assertNotNull(query);
//    assertEquals(0, query.from());
//    assertEquals(10, query.to());
//    assertEquals("Test-Namespace", query.namespace());
//    assertEquals("Chain" , query.filter().getType());
//    assertEquals(QueryType.TAG_VALUES, query.type());
//  }
//
//}
