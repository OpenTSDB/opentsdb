// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.query.router;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeConfigOptions;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.idconverter.ByteToStringIdConverterConfig;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.plan.QueryPlanner.TimeAdjustments;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utilities to help with pushdowns and routing.
 *
 * Note: UT coverage in the router factory tests.
 *
 * @since 3.0
 */
public class RoutingUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RoutingUtils.class);

  private RoutingUtils() {
    // you shall not instantiate me!
  }

  /**
   * Handles inserting the two or more data sources for an HA or split query.
   * This is responsible for renaming nodes, pushingdown the proper nodes and
   * linking those that couldn't be pushed down. Also adds an ID converter if
   * needed.
   * @param context The non-null context we're working with.
   * @param mergerConfig The non-null merger config to link to or update.
   * @param needsIdConverter Whether or not one or more sources need byte IDs
   *                         converted to strings.
   * @param pushDowns The list of pushdowns. The list can be empty or it must
   *                  be the same size as newSources.
   * @param newSources The non-null list of two or more sources to link to the
   *                   merger.
   * @param planner The non-null query planner.
   */
  public static void rebuildGraph(
          final QueryPipelineContext context,
          MergerConfig mergerConfig,
          final boolean needsIdConverter,
          final List<List<QueryNodeConfig>> pushDowns,
          final List<TimeSeriesDataSourceConfig.Builder> newSources,
          final QueryPlanner planner) {
    int max_pushdowns = Integer.MIN_VALUE;
    int min_pushdowns = Integer.MAX_VALUE;
    int max_index = -1;

    for (int i = 0; i < newSources.size(); i++) {
      final TimeSeriesDataSourceConfig.Builder builder = newSources.get(i);
      final List<QueryNodeConfig> pds = builder.pushDowns();
      if (pds == null || pds.isEmpty()) {
        min_pushdowns = 0;
        continue;
      }

      if (pds.size() > max_pushdowns) {
        max_pushdowns = pds.size();
        max_index = i;
      }

      if (pds.size() < min_pushdowns) {
        min_pushdowns = pds.size();
      }
    }

    ByteToStringIdConverterConfig.Builder converterBuilder = needsIdConverter ?
            ByteToStringIdConverterConfig.newBuilder()
                    .setId("IdConverter_" + mergerConfig.getId()): null;
    ByteToStringIdConverterConfig converterConfig = converterBuilder != null ?
            converterBuilder.build() : null;

    // initial add and we'll replace it with Ids later.
    if (converterConfig != null) {
      planner.configGraph().addNode(converterConfig);
    }

    if (max_pushdowns > 0 && pushDowns.get(max_index).size() > 0) {
      final List<String> data_sources =
              Lists.newArrayListWithExpectedSize(newSources.size());
      for (final TimeSeriesDataSourceConfig.Builder source : newSources) {
        data_sources.add(source.id());
      }

      final List<QueryNodeConfig> max = pushDowns.get(max_index);
      final Map<String, QueryNodeConfig> new_push_downs =
              Maps.newHashMapWithExpectedSize(max.size());
      QueryNodeConfig last = null;
      for (int i = min_pushdowns; i < max_pushdowns; i++) {
        final QueryNodeConfig push = max.get(i);
        if (last == null) {
          last = push.toBuilder()
                  .setId(mergerConfig.getId() + "_" + push.getId())
                  // TODO .setPushDown(false)
                  .build();
          new_push_downs.put(push.getId(), last);
        } else {
          QueryNodeConfig new_node = push.toBuilder()
                  .setId(mergerConfig.getId() + "_" + push.getId())
                  // TODO .setPushDown(false)
                  .build();
          new_push_downs.put(push.getId(), new_node);
          planner.addEdge(new_node, last);
          last = new_node;
        }
      }
      if (last != null) {
        planner.addEdge(mergerConfig, last);
      }

      QueryNodeConfig predecessor = max.get(max.size() - 1);
      Set<QueryNodeConfig> predecessors = Sets.newHashSet(
              planner.configGraph().predecessors(predecessor));
      for (final QueryNodeConfig pred : predecessors) {
        planner.addEdge(pred, mergerConfig);
        planner.removeEdge(pred, predecessor);
      }

      // re-link
      planner.removeEdge(max.get(0), mergerConfig);

      // we can shuffle the graph
      for (int i = 0; i < newSources.size(); i++) {
        final List<QueryNodeConfig> source_push_downs = pushDowns.get(i);
        if (source_push_downs.isEmpty()) {
          // no push down, just link it in
          planner.addEdge(new_push_downs.get(
                  max.get(0).getId()), newSources.get(i).build());
          continue;
        }

        // We need to rename the sources for these nodes if they pull from the
        // source.
        final String pushdown_id = newSources.get(i).id();
        List<QueryNodeConfig> renamed_pushdowns =
                Lists.newArrayListWithExpectedSize(source_push_downs.size());
        for (final QueryNodeConfig pd : source_push_downs) {
          if (pd.getSources().contains(mergerConfig.getId())) {
            renamed_pushdowns.add(
                    pd.toBuilder()
                            .setSources(Lists.newArrayList(pushdown_id))
                            .build());
          } else {
            renamed_pushdowns.add(pd);
          }
        }
        newSources.get(i).setPushDownNodes(renamed_pushdowns);
        final Collection<String> sinks = pushDownSinks(renamed_pushdowns);
        if (sinks.size() == 1) {
          final String new_merger_id = sinks.iterator().next();
          if (!mergerConfig.getId().equals(new_merger_id)) {
            // only do this once or we get an exception.
            MergerConfig new_merger = mergerConfig.toBuilder()
                    .setId(sinks.iterator().next())
                    .build();
            planner.replace(mergerConfig, new_merger);
            mergerConfig = new_merger;
          }
        }
        final TimeSeriesDataSourceConfig new_source = (TimeSeriesDataSourceConfig)
                newSources.get(i).build();
        if (source_push_downs.size() == max_pushdowns) {
          planner.addEdge(converterConfig != null ? converterConfig : mergerConfig,
                          new_source);
        } else {
          final QueryNodeConfig mx = new_push_downs.get(
                  max.get(source_push_downs.size()).getId());
          planner.addEdge(mx, new_source);
        }

        if (context.query().isTraceEnabled()) {
          context.queryContext().logTrace("Adding pushdown source: "
                  + new_source.getSourceId());
        }
      }
    }

    for (int i = 0; i < newSources.size(); i++) {
      // TODO - this would be better off in the planner I think.
      final TimeSeriesDataSourceConfig.Builder builder =
              newSources.get(i);

      // this is a _fun_ bit. We need to make sure the merger see's unique IDs
      // from the nodes that feed it. IF we have pushdowns, we need to rename the
      // SOURCE node and use the original source node ID for the last pushdown
      // ID. E.g. if we have a graph with a group by pushdown to two sources:
      // merger -> gb -> ha_m1_s1
      //        -> gb -> ha_m1_s2
      // we need results:
      // merger -> gb_ha_m1_s1:m1 -> ha_m1_s1:m1
      //        -> gb_ha_m1_s2:m1 -> ha_m1_s1:m1
      if (builder.pushDowns() != null && !builder.pushDowns().isEmpty()) {
        for (int x = 0; x < builder.pushDowns().size(); x++) {
           QueryNodeConfig.Builder pdBuilder = ((QueryNodeConfig) builder.pushDowns()
                   .get(x)).toBuilder();

//           if (x + 1 == builder.pushDowns().size()) {
//
//             final String pushdownId = ((QueryNodeConfig) builder.pushDowns().get(x)).getId() +
//                     "_" + builder.id();
//             pdBuilder.setResultIds(Lists.newArrayList(
//                     new DefaultQueryResultId(
//                            pushdownId,
//                            builder.dataSource())));
//             pdBuilder.setId(pushdownId);
//           } else {
//             pdBuilder.setResultIds(Lists.newArrayList(
//                     new DefaultQueryResultId(
//                             ((QueryNodeConfig) builder.pushDowns().get(x)).getId(),
//                             builder.dataSource())));
//           }

          pdBuilder.setResultIds(Lists.newArrayList(
                     new DefaultQueryResultId(
                             ((QueryNodeConfig) builder.pushDowns().get(x)).getId(),
                             builder.dataSource())));

           if (x == 0) {
             pdBuilder.setSources(Lists.newArrayList(builder.id()));
           } else {
             pdBuilder.setSources(Lists.newArrayList(
                           ((QueryNodeConfig) builder.pushDowns().get(x - 1)).getId()));
           }
          builder.pushDowns().set(x, pdBuilder.build());
        }
      }

      final TimeSeriesDataSourceConfig rebuilt =
              (TimeSeriesDataSourceConfig) newSources.get(i).build();

      if (!planner.configGraph().nodes().contains(rebuilt)) {
        planner.addEdge(converterConfig != null ? converterConfig : mergerConfig,
                rebuilt);
      } else {
        // already in the graph.
        planner.replace(rebuilt, rebuilt);
        if (planner.configGraph().predecessors(rebuilt).isEmpty()) {
          planner.addEdge(converterConfig != null ? converterConfig : mergerConfig,
                  rebuilt);
        }
      }

      if (converterConfig != null) {
        // edge case in that a node may have been pushed between the merger and
        // the data source in which case we need to link the ID converter in the
        // right spot.
        // TODO - check for nodes in between that NEED the converted values in
        // which case we should insert a new converter.
        Set<QueryNodeConfig> predecessors = planner.configGraph().predecessors(rebuilt);
        QueryNodeConfig prev = rebuilt;
        while (!predecessors.isEmpty()) {
          QueryNodeConfig predecessor = predecessors.iterator().next();
          if (predecessor instanceof ByteToStringIdConverterConfig) {
            break;
          }

          if (predecessor == mergerConfig) {
            // ok to do equality here.
            planner.removeEdge(mergerConfig, prev);
            planner.addEdge(converterConfig, prev);
            break;
          }

          prev = predecessor;
          predecessors = planner.configGraph().predecessors(predecessor);
        }

        // TODO - we could route those that don't require conversions directly to
        // the merger but for now the convert will pass them through as a no-op.
        converterBuilder.addResultId((QueryResultId) rebuilt.resultIds().get(0));
        final TimeSeriesDataSourceFactory factory = context.tsdb().getRegistry()
                .getPlugin(TimeSeriesDataSourceFactory.class, rebuilt.getSourceId());
        converterBuilder.addDataSourceFactory(rebuilt.getSourceId(), factory);
      }
    }

    if (converterBuilder != null) {
      ByteToStringIdConverterConfig newConfig = converterBuilder.build();
      planner.replace(converterConfig, newConfig);
      planner.addEdge(mergerConfig, newConfig);
    }
  }

  /**
   * Finds the potential pushdown list for each of the sources.
   * @param config The non-null original config (to walk up the graph from)
   * @param sources The list of non-null and non-empty sources that would
   *                satisfy the query.
   * @param planner The non-null planner.
   * @return A non-null but possibly empty list.
   */
  public static List<List<QueryNodeConfig>> potentialPushDowns(
          final TimeSeriesDataSourceConfig config,
          final List<TimeRouterConfigEntry> sources,
          final QueryPlanner planner) {
    Set<QueryNodeConfig> predecessors = planner.configGraph().predecessors(config);
    if (!predecessors.isEmpty() && predecessors.size() == 1) {
      QueryNodeConfig predecessor = predecessors.iterator().next();
      final List<List<QueryNodeConfig>> push_downs = Lists.newArrayList();
      for (int i = 0; i < sources.size(); i++) {
        final List<QueryNodeConfig> source_push_downs = Lists.newArrayList();
        canPushDown(predecessor,
                sources.get(i).factory(),
                source_push_downs,
                planner);
        push_downs.add(source_push_downs);
      }

      return push_downs;
    }
    return Collections.emptyList();
  }

  /**
   * Finds the potential pushdown list for each of the sources.
   * @param config The non-null original config (to walk up the graph from)
   * @param factories The list of non-null and non-empty factories that would
   *                  satisfy the query.
   * @param planner The non-null planner.
   * @return A non-null but possibly empty list.
   */
  public static List<List<QueryNodeConfig>> getPotentialPushDowns(
          final TimeSeriesDataSourceConfig config,
          final List<TimeSeriesDataSourceFactory> factories,
          final QueryPlanner planner) {
    Set<QueryNodeConfig> predecessors = planner.configGraph().predecessors(config);
    if (!predecessors.isEmpty() && predecessors.size() == 1) {
      QueryNodeConfig predecessor = predecessors.iterator().next();
      final List<List<QueryNodeConfig>> push_downs = Lists.newArrayList();
      for (int i = 0; i < factories.size(); i++) {
        final List<QueryNodeConfig> source_push_downs = Lists.newArrayList();
        canPushDown(predecessor,
                factories.get(i),
                source_push_downs,
                planner);
        push_downs.add(source_push_downs);
      }

      return push_downs;
    }
    return Collections.emptyList();
  }

  /**
   * Walks the pushdown list to find out if any nodes have a time adjustment that
   * must be applied to the config.
   * @param context The non-null context to work with.
   * @param pushdowns The non-null list of pushdowns.
   * @param startTimestamp The original time source start time stamp.
   * @param endTimestamp The original time source end time stamp.
   * @return A non-null list of adjustments the same size as the pushdown list.
   * Entries may be null.
   */
  public static List<TimeAdjustments> pushdownAdjustments(
          final QueryPipelineContext context,
          final List<List<QueryNodeConfig>> pushdowns,
          final TimeStamp startTimestamp,
          final TimeStamp endTimestamp) {
    final List<TimeAdjustments> results = Lists.newArrayListWithExpectedSize(
            pushdowns.size());
    // pushdowns are ordered from closest to source to furthest from source.
    for (int i = 0; i < pushdowns.size(); i++) {
      final List<QueryNodeConfig> pds = pushdowns.get(i);
      if (pds == null || pds.isEmpty()) {
        results.add(null);
        continue;
      }

      TimeAdjustments adjustments = new TimeAdjustments();
      for (int x = pds.size() - 1; x >= 0; x--) {
        final QueryNodeConfig config = pds.get(x);
        if (config instanceof DownsampleConfig) {
          if (adjustments.downsampleInterval == null) {
            adjustments.downsampleInterval =
                    ((DownsampleConfig) config).getInterval();
            if (adjustments.downsampleInterval.equals(DownsampleConfig.AUTO)) {
              final long deltaSeconds =
                      endTimestamp.epoch() - startTimestamp.epoch();
              adjustments.downsampleInterval =
                      DownsampleFactory.getAutoInterval(context.tsdb(),
                              deltaSeconds,
                              ((DownsampleConfig) config).getMinInterval());
            } else if (((DownsampleConfig) config).getRunAll()) {
              adjustments.downsampleInterval = null;
            }
          } else {
            // in this case we get the LARGEST interval
            final long existing = DateTime.parseDuration(
                    adjustments.downsampleInterval) / 1000;
            final long next = ((DownsampleConfig) config).interval().get(
                    ChronoUnit.SECONDS);
            if (next > existing) {
              adjustments.downsampleInterval =
                      ((DownsampleConfig) config).getInterval();
            }
          }
          continue;
        }

        final Integer paddingIntervals = (Integer) config.nodeOption(
                QueryNodeConfigOptions.PREVIOUS_INTERVALS);
        if (paddingIntervals != null) {
          if (paddingIntervals > adjustments.previousIntervals) {
            adjustments.previousIntervals = paddingIntervals;
          }
        }

        final String paddingWindow = (String) config.nodeOption(
                QueryNodeConfigOptions.PADDING_WINDOW);
        if (paddingWindow != null) {
          if (adjustments == null) {
            adjustments = new TimeAdjustments();
          }

          if (adjustments.windowInterval == null) {
            adjustments.windowInterval = paddingWindow;
          } else {
            // find the largest.
            long extant = DateTime.parseDuration(adjustments.windowInterval);
            long next = DateTime.parseDuration(paddingWindow);
            if (next > extant) {
              adjustments.windowInterval = paddingWindow;
            }
          }

          if (adjustments.downsampleInterval != null) {
            // validation that the sliding window is >= ds interval.
            long ds = DateTime.parseDuration(adjustments.downsampleInterval);
            long window = DateTime.parseDuration(adjustments.windowInterval);
            if (window < ds) {
              throw new QueryExecutionException("The window of config "
                      + config.getId() + " is smaller than the first " +
                      "downsample interval of " + adjustments.downsampleInterval
                      + ". The window must be larger than the interval and it " +
                      "should be a multiple of the interval.", 400);
            }
          }
        }
      }
      results.add(adjustments);
    }
    return results;
  }

  /**
   * Recursive helper to determine what nodes can be pushed down.
   * @param current The current node to examine.
   * @param factory The source factory.
   * @param push_downs The list of push downs to populate in order.
   * @param planner The planner.
   */
  protected static void canPushDown(final QueryNodeConfig current,
                                    final TimeSeriesDataSourceFactory factory,
                                    final List<QueryNodeConfig> push_downs,
                                    final QueryPlanner planner) {
    if (factory.supportsPushdown(current.getClass()) && current.pushDown()) {
      push_downs.add(current);
      final Set<QueryNodeConfig> predecessors =
              planner.configGraph().predecessors(current);
      if (predecessors.isEmpty() || predecessors.size() > 1) {
        return;
      }

      canPushDown(predecessors.iterator().next(), factory, push_downs, planner);
    }
  }

  /**
   * Determines if any of the pushdowns are sink nodes. Lets the merger rename
   * itself.
   * @param push_down_nodes The list of pushdowns.
   * @return A non-null list of nodes, can be empty.
   */
  protected static Collection<String> pushDownSinks(
          final List<QueryNodeConfig> push_down_nodes) {
    if (push_down_nodes == null || push_down_nodes.isEmpty()) {
      return Collections.emptyList();
    }

    // naive two pass for now, can make it more efficient some day
    final Set<String> nodes = Sets.newHashSet();
    for (final QueryNodeConfig node : push_down_nodes) {
      nodes.add(node.getId());
    }

    for (final QueryNodeConfig node : push_down_nodes) {
      // wtf? why do we need to cast this sucker?
      for (final Object source : node.getSources()) {
        nodes.remove((String) source);
      }
    }
    return nodes;
  }

}
