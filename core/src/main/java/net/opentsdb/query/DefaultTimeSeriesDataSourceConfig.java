// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
package net.opentsdb.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig;
import net.opentsdb.utils.Pair;

import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@JsonInclude(Include.NON_DEFAULT)
@JsonDeserialize(builder = DefaultTimeSeriesDataSourceConfig.Builder.class)
public class DefaultTimeSeriesDataSourceConfig
    extends BaseTimeSeriesDataSourceConfig<
        DefaultTimeSeriesDataSourceConfig.Builder, DefaultTimeSeriesDataSourceConfig> {

  public static final String TYPE = "TimeSeriesDataSourceConfig";

  protected DefaultTimeSeriesDataSourceConfig(final Builder builder) {
    super(builder);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Builder toBuilder() {
    Builder builder = new Builder();
    cloneBuilder(this, builder);
    return builder;
  }

  public static DefaultTimeSeriesDataSourceConfig parseConfig(
      final ObjectMapper mapper, final TSDB tsdb, final JsonNode node) {
    final Builder builder = new Builder();
    parseConfig(mapper, tsdb, node, builder);
    return builder.build();
  }

  @Override
  public int compareTo(final DefaultTimeSeriesDataSourceConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HashCode buildHashCode() {
    if (cached_hash != null) {
      return cached_hash;
    }
    cached_hash = super.buildHashCode();
    return cached_hash;
  }
  
  public static void setupTimeShift(
      final TimeSeriesDataSourceConfig<DefaultTimeSeriesDataSourceConfig.Builder, DefaultTimeSeriesDataSourceConfig> config,
      final QueryPlanner planner) {
    for (final QueryNodeConfig upstream : planner.configGraph().predecessors(config)) {
      if (upstream instanceof TimeShiftConfig) {
        return;
      }
    }
    
    final Set<QueryNodeConfig> predecessors = planner.configGraph().predecessors(config);
    final BaseQueryNodeConfig shift_config =
            TimeShiftConfig.newBuilder()
                .setTimeshiftInterval(config.getTimeShiftInterval())
                .setId(config.getId() + "-timeShift")
                .build();


    planner.replace(config, shift_config);
    
    final Pair<Boolean, TemporalAmount> amounts = config.timeShifts();
    TimeSeriesDataSourceConfig.Builder rebuilt_builder =
        config.toBuilder().setTimeShifts(amounts).setId(config.getId());
    ((BaseTimeSeriesDataSourceConfig.Builder) rebuilt_builder).setHasBeenSetup(true);
    QueryNodeConfig rebuilt = rebuilt_builder.build();
    planner.addEdge(shift_config, rebuilt);
  }

  /**
   * Handles cloning a sub-graph of HA node configs (group bys, downsamples etc) for each time shift
   * offset that we have to query.
   *
   * @param config The non-null parent config.
   * @param planner The planner.
   * @param merger The merger.
   */
  public static void setupTimeShiftMultiNode(
      final TimeSeriesDataSourceConfig config,
      final QueryPlanner planner,
      final MergerConfig merger) {
    if (config.timeShifts() == null) {
      return;
    }

    final Set<QueryNodeConfig> shift_predecessors = planner.configGraph().predecessors(merger);
    final TimeShiftConfig shift_config =
            TimeShiftConfig.newBuilder()
                .setTimeshiftInterval(config.getTimeShiftInterval())
                .setId(merger.getId() + "-timeShift")
                .build();

    for (final QueryNodeConfig predecessor : shift_predecessors) {
      planner.addEdge(predecessor, shift_config);
    }
    
    // now for each time shift we have to duplicate the sub-graph from the
    // merger to the destinations. *sigh*.
    // TODO - make this cleaner some day. This is SUPER ugly. For now we do it
    // so that we have the proper timeouts and distribute the query load.
    String shift_id = config.timeShifts().getValue() + "-timeShift";
    final MergerConfig merger_shift = (MergerConfig) merger.toBuilder().setId(shift_id).build();
    planner.addEdge(shift_config, merger_shift);

    final Set<QueryNodeConfig> successors =
        Sets.newHashSet(planner.configGraph().successors(merger));
    for (final QueryNodeConfig successor : successors) {
      recursiveTimeShift(
          planner, merger_shift, merger_shift, successor, config.timeShifts(), shift_id);
    }
    // Remove the original from the planner. We'll only have the time shift query
    ((DefaultQueryPlanner) planner).sinkFilters().remove(merger.id);
    planner.removeNode(merger);
  }

  /**
   * Recursive walker for sub-graphs to create time shift offsets.
   *
   * @param planner The non-null planner.
   * @param parent the NEW time shifted parent to link to.
   * @param config The old config to clone.
   * @param shifts The shifts to pass down.
   * @param shift_id The shift ID to append to IDs.
   */
  private static void recursiveTimeShift(
      final QueryPlanner planner,
      final QueryNodeConfig parent,
      final QueryNodeConfig new_parent,
      final QueryNodeConfig config,
      final Pair<Boolean, TemporalAmount> shifts,
      final String shift_id) {
    final QueryNodeConfig shift;
    final String timeshift_id = config.getId() + "-" + shift_id;
    if (config instanceof TimeSeriesDataSourceConfig) {
      // for the shift to happen properly we need to rename the shift node and
      // send that to the query target.
      final Pair<Boolean, TemporalAmount> amounts = shifts;
      TimeSeriesDataSourceConfig<Builder, DefaultTimeSeriesDataSourceConfig> tsDataSourceConfig =
          (TimeSeriesDataSourceConfig<Builder, DefaultTimeSeriesDataSourceConfig>) config;
      TimeSeriesDataSourceConfig.Builder shift_builder =
          tsDataSourceConfig.toBuilder().setTimeShifts(amounts).setId(timeshift_id);

      rebuildPushDownNodesForTimeShift(tsDataSourceConfig, shift_builder, timeshift_id);
      shift = shift_builder.build();
    } else {
      shift = config.toBuilder().setId(timeshift_id).build();
    }

    planner.addEdge(new_parent, shift);
    final Set<QueryNodeConfig> successors =
        Sets.newHashSet(planner.configGraph().successors(config));
    for (final QueryNodeConfig successor : successors) {
      recursiveTimeShift(planner, config, shift, successor, shifts, shift_id);
    }
  }

  /**
   * Rebuilds the pushdowns for timeshift query. We have to clone the original pushdowns, change
   * sources whereever necessary (to match with the timeshift id)
   *
   * @param original the original query
   * @param time_shift_config_builder config builder for the new timeshift query
   * @param timeshift_id the timeshift id
   */
  protected static void rebuildPushDownNodesForTimeShift(
      TimeSeriesDataSourceConfig<Builder, DefaultTimeSeriesDataSourceConfig> original,
      TimeSeriesDataSourceConfig.Builder time_shift_config_builder,
      String timeshift_id) {
    List<QueryNodeConfig> pushdowns = (original).getPushDownNodes();
    if (!pushdowns.isEmpty()) {
      List<QueryNodeConfig> pushdown_clone = new ArrayList<>();
      for (QueryNodeConfig pushdown : pushdowns) { // cloning the pushdowns
        pushdown_clone.add(
            pushdown
                .toBuilder()
                .setSources((ArrayList) ((ArrayList) pushdown.getSources()).clone())
                .build());
      }

      for (QueryNodeConfig pushdown : pushdown_clone) {
        if (pushdown.getSources().contains(original.getId())) {
          pushdown.getSources().remove(original.getId());
          pushdown.getSources().add(timeshift_id);
        }
      }

      (time_shift_config_builder).setPushDownNodes(pushdown_clone);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder
      extends BaseTimeSeriesDataSourceConfig.Builder<Builder, DefaultTimeSeriesDataSourceConfig> {

    @Override
    public DefaultTimeSeriesDataSourceConfig build() {
      return new DefaultTimeSeriesDataSourceConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }

  }
}
