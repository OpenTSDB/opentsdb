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
package net.opentsdb.meta;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.utils.UniqueKeyPair;

/**
 * Basic meta query result parser.
 * TODO - improve this for memory performance.
 * 
 * @since 3.0
 */
public class HttpMetaResult implements MetaDataStorageResult {

  private final JsonNode result;
  private final long total_hits;
  
  HttpMetaResult(final JsonNode result) {
    this.result = result;
    total_hits = result.get("totalHits").asLong();
    // TODO - some validation
    
  }
  
  @Override
  public String id() {
    return result.get("id").asText();
  }

  @Override
  public long totalHits() {
    return total_hits;
  }

  @Override
  public MetaResult result() {
    return total_hits > 0 ? MetaResult.DATA : MetaResult.NO_DATA;
  }

  @Override
  public Throwable exception() {
    return null;
  }

  @Override
  public Collection<String> namespaces() {
    List<String> namespaces = Lists.newArrayList();
    JsonNode ns = result.get("namespaces");
    for (final JsonNode namespace : ns) {
      namespaces.add(namespace.asText());
    }
    return namespaces;
  }

  @Override
  public Collection<TimeSeriesId> timeSeries() {
    final JsonNode json_timeseries = result.get("timeseries");
    if (json_timeseries == null || json_timeseries.size() <= 0) {
      return Collections.emptyList();
    }
    
    // TODO - view over JSON instead of loading a whole set.
    List<TimeSeriesId> ids = Lists.newArrayList();
    for (final JsonNode id : json_timeseries) {
      final BaseTimeSeriesStringId.Builder builder = BaseTimeSeriesStringId.newBuilder()
          .setMetric(id.get("metric").asText());
      final Iterator<Entry<String, JsonNode>> iterator = id.get("tags").fields();
      while (iterator.hasNext()) {
        final Entry<String, JsonNode> pair = iterator.next();
        builder.addTags(pair.getKey(), pair.getValue().asText());
      }
      ids.add(builder.build());
    }
    return ids;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public Collection<UniqueKeyPair<String, Long>> metrics() {
    final JsonNode json_metrics = result.get("metrics");
    if (json_metrics == null || json_metrics.size() <= 0) {
      return Collections.emptyList();
    }
    
    // TODO - view over JSON instead of loading a whole set.
    List<UniqueKeyPair<String, Long>> ids = Lists.newArrayList();
    for (final JsonNode metric : json_metrics) {
      ids.add(new UniqueKeyPair<String, Long>(
          metric.get("name").asText(),
          metric.get("count").asLong()));
    }
    return ids;
  }

  @Override
  public Map<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>> tags() {
    final JsonNode json_tkvs = result.get("tagKeysAndValues");
    if (json_tkvs == null || json_tkvs.size() <= 0) {
      return Collections.emptyMap();
    }
    
    // TODO - view over JSON instead of loading a whole set. This is the ugliest
    // of them
    Map<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>> map
      = Maps.newHashMap();
    final Iterator<Entry<String, JsonNode>> iterator = json_tkvs.fields();
    while (iterator.hasNext()) {
      final Entry<String, JsonNode> pair = iterator.next();
      UniqueKeyPair<String, Long> key = new UniqueKeyPair<String, Long>(
          pair.getKey(),
          pair.getValue().get("hits").asLong());
      
      List<UniqueKeyPair<String, Long>> values = Lists.newArrayList();
      for (final JsonNode value : pair.getValue().get("values")) {
        values.add(new UniqueKeyPair<String, Long>(
            value.get("name").asText(),
            value.get("count").asLong()));
      }
      map.put(key, values);
    }
    
    return map;
  }

  @Override
  public Collection<UniqueKeyPair<String, Long>> tagKeysOrValues() {
    JsonNode json_tkvs = result.get("tagKeys");
    if (json_tkvs == null || json_tkvs.size() <= 0) {
      json_tkvs = result.get("tagValues");
    }
    if (json_tkvs == null || json_tkvs.size() <= 0) {
      return Collections.emptyList();
    }
    
    // TODO - view over JSON instead of loading a whole set.
    List<UniqueKeyPair<String, Long>> ids = Lists.newArrayList();
    for (final JsonNode korv : json_tkvs) {
      ids.add(new UniqueKeyPair<String, Long>(
          korv.get("name").asText(),
          korv.get("count").asLong()));
    }
    return ids;
  }
  
}
