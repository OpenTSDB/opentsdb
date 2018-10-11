// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.interpolation.types.numeric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfigParser;
import net.opentsdb.query.pojo.FillPolicy;

/**
 * Parser that will return a scalar or plain interpolator config.
 * 
 * @since 3.0
 */
public class NumericInterpolatorParser implements QueryInterpolatorConfigParser {

  @Override
  public QueryInterpolatorConfig parse(final ObjectMapper mapper, 
                                       final TSDB tsdb,
                                       final JsonNode node) {
    try {
      final JsonNode fill_node = node.get("fillPolicy");
      if (fill_node != null && !fill_node.isNull()) {
        final FillPolicy fill = mapper.treeToValue(fill_node, FillPolicy.class);
        if (fill == FillPolicy.SCALAR) {
          return ScalarNumericInterpolatorConfig.parse(mapper, tsdb, node);
        }
      }
      
      return mapper.treeToValue(node, NumericInterpolatorConfig.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse the config", e);
    }
  }

}
