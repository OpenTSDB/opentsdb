// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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
package net.opentsdb.meta;

import com.fasterxml.jackson.databind.JsonNode;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestDefaultMetaQuery {
  private static TSDB TSDB;

  @BeforeClass
  public static void beforeClass() {
    TSDB = MockTSDBDefault.getMockTSDB();
  }

  @Test
  public void parseQueryWithAllAggregate() throws Exception {
    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\", \"id\": \"1\","
        + "\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":"
        + "\"AnyFieldRegex\",\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\","
        + "\"filter\":\"cpu\"}]},\"type\":\"TIMESERIES\"}";
    JsonNode node = JSON.getMapper().readTree(request);

    MetaQuery query = DefaultMetaQuery.parse(TSDB, JSON.getMapper(), node,
      BatchMetaQuery.QueryType.TIMESERIES)
      .build();

    assertNotNull(query);
    assertEquals("Test-Namespace", query.namespace());
    assertEquals("Chain" , query.filter().getType());
  }

  @Test
  public void parseQueryWithMetricsAggregate() throws Exception {
    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\", \"id\": \"1\","
        + "\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":"
        + "\"AnyFieldRegex\",\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\","
        + "\"filter\":\"cpu\"}]},\"type\":\"METRICS\"}";
    JsonNode node = JSON.getMapper().readTree(request);

    MetaQuery query = DefaultMetaQuery.parse(TSDB, JSON.getMapper(), node,
      BatchMetaQuery.QueryType.METRICS).build();

    assertNotNull(query);
    assertEquals("Test-Namespace", query.namespace());
    assertEquals("Chain" , query.filter().getType());
  }

  @Test
  public void parseQueryWithtagKeysAggregate() throws Exception {
    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\", \"id\": \"1\","
        + "\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":\"AnyFieldRegex\","
        + "\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\",\"filter\":"
        + "\"cpu\"}]},\"type\":\"TAG_KEYS\"}";
    JsonNode node = JSON.getMapper().readTree(request);

    MetaQuery query = DefaultMetaQuery.parse(TSDB, JSON.getMapper(), node,
      BatchMetaQuery.QueryType.TAG_KEYS)
      .build();

    assertNotNull(query);
    assertEquals("Test-Namespace", query.namespace());
    assertEquals("Chain" , query.filter().getType());
  }

  @Test
  public void parseQueryWithtagValuesAggregate() throws Exception {
    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\", \"id\": \"1\","
        + "\"filter\":{\"type\":\"chain\",\"filters\":[{\"type\":\"AnyFieldRegex\","
        + "\"filter\":\"sys|bf\"},{\"type\":\"AnyFieldRegex\",\"filter\":"
        + "\"cpu\"}]},\"type\":\"TAG_VALUES\"}";
    JsonNode node = JSON.getMapper().readTree(request);

    MetaQuery query = DefaultMetaQuery.parse(TSDB, JSON.getMapper(), node,
      BatchMetaQuery.QueryType.TAG_VALUES)
      .build();

    assertNotNull(query);
    assertEquals("Test-Namespace", query.namespace());
    assertEquals("Chain" , query.filter().getType());
  }

  @Test
  public void parseQueryWithNamespaceAggregateNoFilters() throws Exception {
    String request = "{\"from\":0,\"to\":10,\"namespace\":\"Test-Namespace\", \"id\": \"1\","
      + "\"type\":\"NAMESPACES\"}";
    JsonNode node = JSON.getMapper().readTree(request);

    MetaQuery query = DefaultMetaQuery.parse(TSDB, JSON.getMapper(), node,
      BatchMetaQuery.QueryType.NAMESPACES)
      .build();

    assertNotNull(query);
    assertEquals("Test-Namespace", query.namespace());
  }

}
