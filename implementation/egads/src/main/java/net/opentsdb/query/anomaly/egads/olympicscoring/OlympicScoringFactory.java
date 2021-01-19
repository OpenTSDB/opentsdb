// This file is part of OpenTSDB.
// Copyright (C) 2019-2021  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly.egads.olympicscoring;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.anomaly.BaseAnomalyFactory;
import net.opentsdb.query.anomaly.egads.olympicscoring.OlympicScoringConfig.Builder;

/**
 * Class that returns OlympicScoringNodes.
 * <b>NOTE:</b> If a query comes in with the PREDICT or EVALUATE mode set, the
 * {@link SetupGraph} method will check the cache for the state of the prediction.
 *
 * @since 3.0
 */
public class OlympicScoringFactory extends BaseAnomalyFactory<
    OlympicScoringConfig, OlympicScoringNode> {

  public static final String TYPE = "OlympicScoring";

  @Override
  public OlympicScoringNode newNode(final QueryPipelineContext context,
                                    final OlympicScoringConfig config) {
    return new OlympicScoringNode(this, context, config);
  }
  
  @Override
  public OlympicScoringConfig parseConfig(final ObjectMapper mapper, 
                                          final TSDB tsdb,
                                          final JsonNode node) {
    Builder builder = new Builder();
    Builder.parseConfig(mapper, tsdb, node, builder);
    
    JsonNode n = node.get("baselineQuery");
    if (n != null && !n.isNull()) {
      builder.setBaselineQuery(SemanticQuery.parse(tsdb, n).build());
    }
     
    n = node.get("baselinePeriod");
    if (n != null && !n.isNull()) {
      builder.setBaselinePeriod(n.asText());
    }
    
    n = node.get("baselineNumPeriods");
    if (n != null && !n.isNull()) {
      builder.setBaselineNumPeriods(n.asInt());
    }
    
    n = node.get("baselineAggregator");
    if (n != null && !n.isNull()) {
      builder.setBaselineAggregator(n.asText());
    }
    
    n = node.get("excludeMax");
    if (n != null && !n.isNull()) {
      builder.setExcludeMax(n.asInt());
    }
    
    n = node.get("excludeMin");
    if (n != null && !n.isNull()) {
      builder.setExcludeMin(n.asInt());
    }
    
    return builder.build();
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    type = TYPE;
    return super.initialize(tsdb, id);
  }

  @Override
  public String type() {
    return TYPE;
  }

}
