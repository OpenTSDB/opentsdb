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
package net.opentsdb.tsd;

import java.util.Iterator;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesStringId;

/**
 * An interface for parsing results from the query runner and emitting additional
 * metrics like the number of series and values.
 * 
 * @since 3.0
 */
public interface ResultParser {

  /**
   * Parses the temporary file containing the results.
   * @param path The path to the temp file.
   * @param config The query config.
   * @param tags Tags for the query to report with the metrics.
   */
  public void parse(final String path, final QueryConfig config, final String[] tags);
  
  public static String[] parseIdToLatestDpTags(final JsonNode node, 
                                               final String[] tags) {
    final BaseTimeSeriesStringId.Builder builder =
        BaseTimeSeriesStringId.newBuilder();
    JsonNode temp = node.get("metric");
    if (temp != null && !temp.isNull()) {
      builder.setMetric(temp.asText());
    }
    temp = node.get("tags");
    if (temp != null && !temp.isNull()) {
      final Iterator<Entry<String, JsonNode>> iterator = temp.fields();
      while (iterator.hasNext()) {
        final Entry<String, JsonNode> entry = iterator.next();
        String tag = entry.getValue().asText();
        builder.addTags(entry.getKey(), tag);
      }
    }

    temp = node.get("hits");
    if (temp != null && !temp.isNull()) {
      builder.setHits(temp.asLong());
    }

    temp = node.get("aggregateTags");
    if (temp != null && !temp.isNull()) {
      for (final JsonNode tag : temp) {
        builder.addAggregatedTag(tag.asText());
      }
    }
    final TimeSeriesStringId id = builder.build();
    int length = id.tags() != null ? id.tags().size() : 0;
    length += 2; // metric = X
    length += tags.length * 2; 
    final String[] new_tags = new String[length];
    int i = 0;
    for (; i < tags.length; i++) {
      new_tags[i] = tags[i];
    }
    new_tags[i++] = "metricName";
    new_tags[i++] = id.metric();
    for (final Entry<String, String> entry : id.tags().entrySet()) {
      new_tags[i++] = entry.getKey();
      new_tags[i++] = entry.getValue();
    }
    
    // TODO - agg tags maybe just as a sorted comma separated list
    // TODO - disjoint tags
    
    return new_tags;
  }
}
