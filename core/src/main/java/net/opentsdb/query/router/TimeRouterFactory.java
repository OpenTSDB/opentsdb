// This file is part of OpenTSDB.
// Copyright (C) 2017-2021  The OpenTSDB Authors.
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

import java.time.temporal.TemporalAmount;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.plan.QueryPlanner.TimeAdjustments;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.router.TimeRouterConfigEntry.ConfigSorter;
import net.opentsdb.query.router.TimeRouterConfigEntry.MatchType;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A node that manages slicing and routing queries across multiple sources
 * over configurable time ranges. For example, if the time series storage 
 * system has a 24h in-memory cache (e.g. aura metrics) and
 * there's a long-term storage layer, the router will send queries for
 * the current 24h of data to the cache and longer and/or older queries 
 * to the long-term store.
 * <p>
 * For hot/warm/cold storage, queries can be split to execute across all of the
 * appropriate components for wide time ranges.
 * <p> 
 * Additionally the long-term layer can be sharded across tables or data 
 * stores. For example, if a TSD runs out of UIDs and users need more, 
 * they can create a new set of tables with a larger UID size. The 
 * router will then split queries at the appropriate date and merge the 
 * results.
 * </p>
 * <b>Important considerations:</b>
 * <ul>
 *   <li>Queries may have sliding windows (or equivalent) in which case we need
 *   to make sure the sources are queried so that we fetch the amount of data
 *   required to satisfy the LARGEST window.</li>
 *   <li>Queries may have a rate type of node wherein we need to make sure that
 *   there is a value BEFORE the query time range so that we don't just skip
 *   the first result. That means we also need to account for the overlaps in
 *   the sources as well. E.g. source A and B partake in a query. A should have
 *   enough data to calculate a rate that overlaps or at least butts up against
 *   B</li>
 *   <li>Some nodes may have a rollup config that would satsify a downsampler
 *   and others may not. Therefore we may be able to pushdown a DS to some but
 *   not others. </li>
 *   <li>If there is overlap between sources and the most recent source is
 *   a relatively small portion of the older sources fetch, it is worth it to
 *   simply query the older source and not involve the newer (unless we try
 *   streaming again in the future). Therefore we should just from the older
 *   source.</li>
 *   <li>The logic for split overlaps is roughly:
 *   <ul>
 *     <li>Window size will take precedence as we want both sources to have
 *     the same data for merging. If the more recent source has less data, the
 *     values will be way off and plots would look odd.</li>
 *     <li>If the interval is greater than the window, we fail the query.
 *     See the TODO below as in some situations this may be ok.</li>
 *     <li>Rates will still seek to the end of the older store even if a
 *     downsample is applied as we don't know when an actual value would be
 *     present for the DS.</li>
 *   </ul>
 *   </li>
 * </ul>
 *
 * <b>CAVEATS / TODOs</b>
 * <ul>
 *   <li>TODO - if there is significant overlap and the more recent store is
 *   much faster than the previous, we may still want to split the query.</li>
 *   <li>It does NOT support duplicate sources that have the same time configs.</li>
 *   <li>TODO - Some dowmsamples intervals may evade the overlap failures (or
 *   bad data) by dropping the downsample pushdown. We have to check for intervals
 *   that overlap the store times and see which stores support downsamples and what
 *   don't.</li>
 *   <li>TODO - Rates can't skip overlaps unless we have a special merger that
 *   will keep the previous value when processing.</li>
 *   <li>TODO - A rate overlap config. If there is significant overlap, we'd
 *   likely waste a fair bit of time on fetching overlap data we don't need.</li>
 *   <li>TODO - In the case of windows and downsamples where the downsample
 *   interval is gt than the window, we may be able to remove the pushdown and
 *   still process the query accurately.</li>
 *   <li>TODO - Figure out a config for rates to find a previous value before
 *   the query time.</li>
 * </ul>
 * @since 3.0
 */
public class TimeRouterFactory extends BaseTSDBPlugin implements 
    TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig, TimeSeriesDataSource> {
  private static final Logger LOG =
          LoggerFactory.getLogger(TimeRouterFactory.class);

  public static final String TYPE = "TimeNamespaceRouter";

  public static final String KEY_PREFIX = "tsd.query.tnrouter.";
  public static final String CONFIG_KEY = "config";
  public static final String SPLIT_KEY = "split.enable";
  public static final String FAIL_OVERLAP_KEY = "split.overlap.invalid.fail";
  public static final String OVERLAP_INTERVAL_KEY = "split.overlap.intervals";
  public static final String OVERLAP_DURATION_KEY = "split.overlap.duration";
  public static final String DEFAULT_QUERY_TIMEOUT_KEY = "default.timeout";
  // TODO - need millis in the future.
  public static final String DEFAULT_INTERVAL = "1s";
  
  /** Type for parsing the config. */
  public static final TypeReference<List<TimeRouterConfigEntry>> TYPE_REF = 
      new TypeReference<List<TimeRouterConfigEntry>>() { };
  
  /** The parsed config. */
  protected volatile List<TimeRouterConfigEntry> config;

  @Override
  public TimeSeriesDataSourceConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public boolean supportsQuery(final QueryPipelineContext context, 
                               final TimeSeriesDataSourceConfig config) {
    // TODO - ugg. We may _not_ support the query really. We won't know unless
    // we have the whole graph. We could do some minum checks here to see if
    // we at least have data for the time range but we'd be doing some duplicate
    // work.
    return true;
  }
  
  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final TimeSeriesDataSourceConfig config,
                         final QueryPlanner planner) {
    final TimeAdjustments adjustments = planner.getAdjustments(config);
    final TimeSeriesDataSourceConfig.Builder builder =
            DefaultQueryPlanner.setTimeOverrides(context, config, adjustments);

    if (tsdb.getConfig().getBoolean(getConfigKey(SPLIT_KEY))) {
      setupGraphSplit(context, config, builder, planner, adjustments);
      return;
    }

    final List<TimeRouterConfigEntry> config_ref = this.config;
    List<TimeRouterConfigEntry> sources = Lists.newArrayList();
    final int now = (int) (DateTime.currentTimeMillis() / 1000);
    for (final TimeRouterConfigEntry entry : config_ref) {
      final MatchType match = entry.match(context, builder, now);
      if (match == MatchType.NONE) {
        continue;
      } else if (match == MatchType.FULL) {
        sources.add(entry);
        break;
      } else {
        continue;
      }
    }

    if (sources.isEmpty()) {
      throw new QueryExecutionException("No data source found for "
              + "config: " + config, 400);
    } else if (sources.size() > 1) {
      throw new QueryExecutionException("Too many data sources found for "
              + "config: " + config + ". Sources: " + sources, 400);
    }

    if (context.query().isDebugEnabled()) {
      context.queryContext().logTrace("Potential data sources: " + sources.get(0));
    }

    builder.setSourceId(sources.get(0).getSourceId());
    TimeSeriesDataSourceConfig rebuilt = (TimeSeriesDataSourceConfig) builder.build();
    planner.replace(config, rebuilt);
    if (context.query().isTraceEnabled()) {
      context.queryContext().logTrace("Only one route available: "
              + rebuilt.getSourceId());
    }
  }

  /**
   * Tons of context, edge cases and optimizations here. Beware!
   * @param context
   * @param config
   * @param builder
   * @param planner
   * @param adjustments
   */
  void setupGraphSplit(final QueryPipelineContext context,
                       final TimeSeriesDataSourceConfig config,
                       final TimeSeriesDataSourceConfig.Builder builder,
                       final QueryPlanner planner,
                       final TimeAdjustments adjustments) {
    // shadow copy to allow for live updates
    final List<TimeRouterConfigEntry> config_ref = this.config;
    final boolean failInvalidOverlaps = tsdb.getConfig().getBoolean(
            getConfigKey(FAIL_OVERLAP_KEY));
    final int overlapIntervals = tsdb.getConfig().getInt(
            getConfigKey(OVERLAP_INTERVAL_KEY));
    final String overlapDurationString = tsdb.getConfig().getString(
            getConfigKey(OVERLAP_DURATION_KEY));
    final long overlapDuration = overlapDurationString == null ? 0 :
            DateTime.parseDuration(overlapDurationString) / 1000;

    boolean needsIdConverter = false;
    List<TimeRouterConfigEntry> sources = Lists.newArrayList();
    final int now = (int) (DateTime.currentTimeMillis() / 1000);
    for (final TimeRouterConfigEntry entry : config_ref) {
      final MatchType match = entry.match(context, builder, now);
      if (match == MatchType.NONE) {
        if (context.query().isTraceEnabled()) {
          context.queryContext().logTrace("No match on entry " + entry);
        }
        continue;
      } else if (match == MatchType.FULL) {
        if (context.query().isTraceEnabled()) {
          context.queryContext().logTrace("Entry " + entry
                  + " requires a full query, no match for split.");
        }
        sources.clear();
        sources.add(entry);
        break;
      }

      if (entry.factory().idType() != Const.TS_STRING_ID) {
        needsIdConverter = true;
      }

      if (adjustments != null && adjustments.downsampleInterval == null) {
        sources.add(entry);
      } else {
        if (entry.factory() == null) {
          if (context.query().isWarnEnabled()) {
            context.queryContext().logWarn("Entry " + entry
                    + " did not return a factory reference.");
          }
          LOG.warn("Unable to find query node factory for {}", entry.getSourceId());
          continue;
        }

        if (entry.factory().rollupConfig() == null) {
          if (context.query().isTraceEnabled()) {
            context.queryContext().logTrace("Entry " + entry
                    + " did not have a rollup config and the query involved an " +
                    "interval. Assuming \"raw\" data.");
          }
          sources.add(entry);
          continue;
        }

        String stringInterval = DEFAULT_INTERVAL;
        if (adjustments != null && adjustments.downsampleInterval != null) {
          stringInterval = adjustments.downsampleInterval;
        }
        long intervalMs = DateTime.parseDuration(stringInterval);

        final List<RollupInterval> possibleIntervals = entry.factory().rollupConfig()
                .getRollupIntervals(intervalMs, stringInterval, false);
        if (possibleIntervals == null || possibleIntervals.isEmpty()) {
          final RollupInterval defaultInterval =
                  entry.factory().rollupConfig().getDefaultInterval();
          if (defaultInterval == null) {
            if (context.query().isTraceEnabled()) {
              context.queryContext().logTrace("Entry " + entry
                      + " did not have an interval to match " + stringInterval);
            }
            continue;
          }
          // it's likely a raw data table so fall through and add it.
        }

        sources.add(entry);
      }
    }

    if (sources.isEmpty()) {
      // Can't handle this query.
      throw new QueryExecutionException("No data source found for "
              + "config: " + config, 406);
    }

    if (context.query().isDebugEnabled()) {
      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < sources.size(); i++) {
        if (i >= 0 && buf.length() > 0) {
          buf.append(", ");
        }
        buf.append(sources.get(i).getSourceId());
      }
      context.queryContext().logDebug("Potential data sources: " + buf.toString());
    }

    if (sources.size() == 1) {
      builder.setSourceId(sources.get(0).getSourceId());
      TimeSeriesDataSourceConfig rebuilt = (TimeSeriesDataSourceConfig) builder.build();
      planner.replace(config, rebuilt);
      if (context.query().isTraceEnabled()) {
        context.queryContext().logTrace("Only one route available: "
                + rebuilt.getSourceId());
      }
      return;
    }
    List<List<QueryNodeConfig>> pushdowns = Lists.newArrayList();
    if(config.getPushDownNodes() != null && ! config.getPushDownNodes().isEmpty()) {
      for (int i = 0; i < sources.size(); i++) {
        pushdowns.add(Lists.newArrayList(config.getPushDownNodes()));
      }
    } else {
      pushdowns = RoutingUtils.potentialPushDowns(config, sources, planner);
    }

    final List<TimeAdjustments> pushdownAdjustments =
            RoutingUtils.pushdownAdjustments(context,
                    pushdowns,
                    builder.startOverrideTimeStamp(),
                    builder.endOverrideTimeStamp());
    final long compareStart;
    final long compareEnd;
    if (config.timeShifts() != null) {
      TimeStamp ts = builder.startOverrideTimeStamp().getCopy();
      ts.subtract((TemporalAmount) config.timeShifts().getValue());
      compareStart = ts.epoch();

      ts = builder.endOverrideTimeStamp().getCopy();
      ts.subtract((TemporalAmount) config.timeShifts().getValue());
      compareEnd = ts.epoch();
    } else {
      compareStart = builder.startOverrideTimeStamp().epoch();
      compareEnd = builder.endOverrideTimeStamp().epoch();
    }
    // the actual query times to use when setting timestamps.
    final long queryStart = builder.startOverrideTimeStamp().epoch();
    final long queryEnd = builder.endOverrideTimeStamp().epoch();
    final long windowInterval = adjustments != null &&
                                adjustments.windowInterval != null ?
            DateTime.parseDuration(adjustments.windowInterval) / 1000 : 0;
    final int previousIntervals = adjustments != null ? adjustments.previousIntervals : 0;

    long[] timestamps = null;

    // We loop like this because we may have some messy configs where a source does
    // not overlap with the previous source. BUT there could be an overlap with next.
    // Or a source may violate requirements so we drop it and try again. Typical
    // optimization loop and it shouldn't happen too often as there are likely to
    // be a few entries in any given config.
    List<Integer> drops = null;
    while (drops == null || drops.size() > 0) {
      if (drops == null) {
        drops = Lists.newArrayList();
      } else {
        for (int i : drops) {
          sources.remove(i);
          if (!pushdowns.isEmpty()) {
            pushdowns.remove(i);
            pushdownAdjustments.remove(i);
          }
        }
        drops.clear();
      }

      timestamps = new long[sources.size() * 2];

      // walk back from the oldest store to the newest since, if we have windows,
      // we need to account for overlap if at all possible.
      for (int i = sources.size() - 1; i >= 0; i--) {
        final TimeRouterConfigEntry entry = sources.get(i);
        final long storeStart = entry.getStart(now);
        final long storeEnd = entry.getEnd(now);
        final int tsStartIdx = i * 2;
        final int tsEndIdx = (i * 2) + 1;

        final TimeAdjustments pushdownAdjustment =
                !pushdownAdjustments.isEmpty() ? pushdownAdjustments.get(i) : null;
        final long localDsInterval = pushdownAdjustment == null ||
                pushdownAdjustment.downsampleInterval == null ? 0 :
                DateTime.parseDuration(pushdownAdjustment.downsampleInterval) / 1000;;
        final String stringDsInterval = pushdownAdjustment == null ||
                pushdownAdjustment.downsampleInterval == null ? null :
                pushdownAdjustment.downsampleInterval;
        final long localWindowInterval = pushdownAdjustment == null ||
                pushdownAdjustment.windowInterval == null ?  windowInterval:
                DateTime.parseDuration(pushdownAdjustment.windowInterval) / 1000;
        // TODO - which here? We want the overlap I'm thinking, regardless of the
        // pushdown.
        final long localIntervals = pushdownAdjustment == null ? previousIntervals :
                pushdownAdjustment.previousIntervals;

        // check for overlap
        TimeRouterConfigEntry next = null;
        long nextStart = 0;
        long delta = 0;
        if (i - 1 >= 0) {
          next = sources.get(i - 1);
          nextStart = next.getStart(now);
          delta = nextStart - storeEnd;
          if (delta > 0) {
            throw new QueryExecutionException("Unable to execute query. Gap " +
                    "between configurations " + entry + " and " + next + " of " +
                    delta + " seconds.", 400);
          }
          delta = -delta; // want a positive value now;
        }

        // The start time is the easy part. Always the store or query start.
        if (i + 1 >= sources.size()) {
          if (compareStart < storeStart) {
            if (context.query().isDebugEnabled()) {
              context.queryContext().logDebug("Dropping a source as store " +
                      "start [" + storeStart + "] is less than the required start " +
                      "timestamp [" + compareStart + "]: " + entry);
            }
            drops.add(i);
            break;
          }
          timestamps[tsStartIdx] = queryStart;
        } else {
          timestamps[tsStartIdx] = storeStart;
        }

        // The end time is where it's tricky as we need to try and compute an
        // overlap.
        if (i - 1 >= 0) {
          if (localWindowInterval > 0) {
            if (localWindowInterval > delta) {
              if (failInvalidOverlaps) {
                // not enough window overlap and configured to fail
                throw new QueryExecutionException("Unable to satisfy required " +
                        "window overlap of [" + localWindowInterval + "] from an " +
                        "interval of [" + delta + "] between " + entry + " and " +
                        next, 400);
              } else {
                context.queryContext().logWarn("Entry was unable to satisfy the " +
                        "required window overlap of [" + localWindowInterval +
                        " from an interval of [" + delta + "] between " + entry +
                        " and " + next);
              }
              // otherwise fall through and we'll try our best.
            }

            // overlap the greatest of the configured overlap or the window.
            final long overlap = Math.max(localWindowInterval, overlapDuration);
            if (overlap > storeEnd - nextStart) {
              // failure is caught above
              timestamps[tsEndIdx] = storeEnd;
            } else {
              timestamps[tsEndIdx] = nextStart + overlap;
            }
          } else if (localDsInterval > 0) {
            if (delta > 0) {
              if (delta == localDsInterval) {
                timestamps[tsEndIdx] = nextStart + delta;
              } else if (storeEnd - nextStart < localDsInterval && failInvalidOverlaps) {
                if (context.query().isDebugEnabled()) {
                  context.queryContext().logDebug("Dropping a source as the " +
                          "overlap [" + delta + "] is smaller than the " +
                          "required downsample interval [" + localDsInterval +
                          "]: " + entry);
                }
                drops.add(i);
                break;
              } else {
                // compute the overlap
                long overlap = Math.max(localIntervals, overlapIntervals);
                if (overlap < 1) {
                  overlap = 1;
                }
                overlap *= localDsInterval;
                while (overlap > storeEnd - nextStart) {
                  overlap -= localDsInterval;
                }

                if (overlap <= 0 && failInvalidOverlaps) {
                  if (context.query().isDebugEnabled()) {
                    context.queryContext().logDebug("Dropping a source as the " +
                            "overlap [" + delta + "] is smaller than the " +
                            "required downsample interval [" + localDsInterval +
                            "]: " + entry);
                  }
                  drops.add(i);
                  break;
                } else if (overlap <= 0) {
                  context.queryContext().logInfo("A An entry did not have any " +
                          "overlap for a downsample interval of [" + localDsInterval +
                          "]: " + entry);
                }
                timestamps[tsEndIdx] = nextStart + overlap;
              }
            } else {
              if (failInvalidOverlaps) {
                if (context.query().isDebugEnabled()) {
                  context.queryContext().logDebug("Dropping a source as there " +
                          "is no overlap for the required downsample interval [" +
                          localDsInterval + "]: " + entry);
                }
                drops.add(i);
                break;
              } else {
                context.queryContext().logInfo("A An entry did not have any " +
                        "overlap for a downsample interval of [" + localDsInterval +
                        "]: " + entry);
              }
              timestamps[tsEndIdx] = nextStart;
            }
          } else if (localIntervals > 0) {
            // TODO - So in this situation we have a rate and we DON'T have a
            // downsample interval so we're not sure where the next data point
            // may be. We _could_ just read all of the current store and possibly
            // have a ton of overlap. OR we could use the default overlap. Which
            // is better? For now, go for broke.
            if (storeEnd == next.getStart(now) && failInvalidOverlaps) {
              if (context.query().isDebugEnabled()) {
                context.queryContext().logDebug("Dropping a source as there is" +
                        " no overlap for the required rate or previous intervals: " +
                        entry);
              }
              drops.add(i);
              break;
            } else if (storeEnd == next.getStart(now)) {
              context.queryContext().logWarn("A rate or window requiring [" +
                      localIntervals + "] intervals was present but there is no " +
                      "overlap for entry: " + entry);
            }
            timestamps[tsEndIdx] = storeEnd;
          } else if (overlapDuration > 0 && storeEnd > next.getStart(now)) {
            timestamps[tsEndIdx] = next.getStart(now) + overlapDuration;
          } else {
            // butt-joint
            timestamps[tsEndIdx] = next.getStart(now);
          }
        } else {
          // This is the most recent source.
          if (storeEnd < compareEnd) {
            // edge case, we could have an end future and if we're looking at
            // the most recent store AND it has a 0 relative end, let it flow
            if (!entry.isEndRelative() || entry.getEndString() != null) {
              if (context.query().isDebugEnabled()) {
                context.queryContext().logDebug("Dropping a source as store " +
                        "end [" + storeEnd + "] is less than the required end " +
                        "timestamp [" + compareEnd + "]: " + entry);
              }
              drops.add(i);
              break;
            }
          }
          timestamps[tsEndIdx] = queryEnd;
        }

        // Now check and see if it makes more sense to just query one source
        // instead of two because the amount fetched from the newer source is
        // just a small amount of the query or the query itself is so small.
        if (i < sources.size() - 1) {
          // easy check, overlap is >= than the current query time.
          final long end = timestamps[tsEndIdx];
          final long prevEnd = timestamps[tsEndIdx + 2];
          final TimeRouterConfigEntry prev = sources.get(i + 1);
          if (prevEnd >= end || end < prev.getEnd(now)) {
            if (context.query().isDebugEnabled()) {
              context.queryContext().logDebug("Dropping a source as the overlap " +
                      "is small enough that we can query a single source: " + entry);
            }
            drops.add(i);
            break;
          }
        }
      }
    }

    if (sources.isEmpty()) {
      throw new QueryExecutionException("Unable to find any set of sources " +
              "to satisfy the query", 400);
    }

    // Make sure the timestamps are covering the full query range. We could have
    // dropped the head or tail of the split sources and only cover a partial
    // range.
    if (timestamps[timestamps.length - 2] > context.query().startTime().epoch()) {
      throw new QueryExecutionException("The final start timestamp [" +
              timestamps[timestamps.length - 2] + " was less than the query " +
              "start timestamp [" + context.query().startTime().epoch() + "]", 400);
    }

    // Yay! After all of that we have a good query so we need to setup the new
    // sources with unique Ids, perform the actual pushdowns and rebuild the
    // graph.
    final List<TimeSeriesDataSourceConfig.Builder> newSources =
            Lists.newArrayListWithExpectedSize(sources.size());
    final List<String> ids = Lists.newArrayListWithCapacity(newSources.size());
    final List<String> timeouts = Lists.newArrayListWithCapacity(newSources.size());
    for (int i = 0; i < sources.size(); i++) {
      int tsIdx = i * 2;
      final TimeSeriesDataSourceConfig.Builder localBuilder =
              (TimeSeriesDataSourceConfig.Builder) config.toBuilder();
      final TimeRouterConfigEntry entry = sources.get(i);
      final String dataSource = config.getId() + "_" + entry.getSourceId();
      localBuilder.setSourceId(entry.getSourceId())
              .setDataSource(dataSource)
              .setStartTimeStamp(new SecondTimeStamp(timestamps[tsIdx]))
              .setEndTimeStamp(new SecondTimeStamp(timestamps[tsIdx + 1]))
              .setId(dataSource)
              .setResultIds(Lists.newArrayList(new DefaultQueryResultId(
                      dataSource, dataSource)));

      if (pushdowns != null && !pushdowns.isEmpty()) {
        final List<QueryNodeConfig> localPushdowns = pushdowns.get(i);
        if (localPushdowns != null) {
          localBuilder.setPushDownNodes(localPushdowns);
        }
      }

      newSources.add(localBuilder);
      ids.add(dataSource);
      String timeout = sources.get(i).getTimeout();
      if (Strings.isNullOrEmpty(timeout)) {
        timeout = tsdb.getConfig().getString(getConfigKey(DEFAULT_QUERY_TIMEOUT_KEY));
      }
      timeouts.add(timeout);
    }

    MergerConfig.Builder mergerBuilder = (MergerConfig.Builder) MergerConfig.newBuilder()
            // TODO - want to make this configurable.
            .setAggregator("max")
            .setMode(MergerConfig.MergeMode.SPLIT)
            .setTimeouts(timeouts)
            .setSortedDataSources(ids)
            .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                    .setFillPolicy(FillPolicy.NONE)
                    .setRealFillPolicy(FillWithRealPolicy.NONE)
                    .setDataType(NumericType.TYPE.toString())
                    .build())
            //.setDataSource(config.getId())
            .setDataSource(config.getDataSource() == null ? config.getId() : config.getDataSource())
            .setFirstDataTimestamp(newSources.get(newSources.size() - 1).startOverrideTimeStamp())
            .setId(config.getId());
    if (adjustments != null && adjustments.downsampleInterval != null) {
      long interval = DateTime.parseDuration(adjustments.downsampleInterval);
      long delta = newSources.get(0).endOverrideTimeStamp().msEpoch() -
              newSources.get(newSources.size() - 1).startOverrideTimeStamp().msEpoch();
      mergerBuilder.setAggregatorArraySize((int) (delta / interval))
                   .setAggregatorInterval(adjustments.downsampleInterval);
    }
    MergerConfig merger = mergerBuilder.build();
    planner.replace(config, merger);

    if(config.getPushDownNodes().isEmpty()) {
      RoutingUtils.rebuildGraph(context, merger, needsIdConverter, pushdowns, newSources, planner);
    } else {
      List<List<QueryNodeConfig>> pds = Lists.newArrayList();
      for (int i = 0; i < sources.size(); i++) {
        pds.add(Collections.emptyList());
      }
      RoutingUtils.rebuildGraph(context, merger, needsIdConverter, pds, newSources, planner);
    }
  }

  @Override
  public TimeSeriesDataSource newNode(QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TimeSeriesDataSource newNode(final QueryPipelineContext context,
                           final TimeSeriesDataSourceConfig config) {
    throw new UnsupportedOperationException("This node should have been "
        + "removed from the graph.");
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    // TODO - need to make sure downstream returns identical types.
    return Const.TS_STRING_ID;
  }

  @Override
  public boolean supportsPushdown(
      final Class<? extends QueryNodeConfig> operation) {
    // TODO figure out what to do here.
    return true;
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(
      final TimeSeriesByteId id,
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException(
        "This node should have been removed from the graph."));
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(
      final List<String> join_keys,
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException(
        "This node should have been removed from the graph."));
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(
      final List<String> join_metrics,
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException(
        "This node should have been removed from the graph."));
  }

  @Override
  public RollupConfig rollupConfig() {
    throw new UnsupportedOperationException(
            "This node should have been removed from the graph.");
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    registerConfigs(tsdb);
    // TODO - validate config.
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  /** A callback for the sources that updates default_sources. */
  class SettingsCallback implements ConfigurationCallback<Object> {

    @SuppressWarnings("unchecked")
    @Override
    public void update(final String key, final Object value) {
      if (key.equals(getConfigKey(CONFIG_KEY))) {
        if (value == null) {
          LOG.error("Config " + getConfigKey(CONFIG_KEY)
                  + " value was null for router " + id);
          return;
        }

        final List<TimeRouterConfigEntry> config =
                (List<TimeRouterConfigEntry>) value;
        Collections.sort(config, new ConfigSorter());
        for (TimeRouterConfigEntry entry : config) {
          entry.factory = tsdb.getRegistry().getPlugin(
                  TimeSeriesDataSourceFactory.class, entry.getSourceId());
          if (entry.factory == null) {
            throw new IllegalArgumentException("No factory found for source: "
                    + (Strings.isNullOrEmpty(entry.getSourceId()) ?
                    "Default" : entry.getSourceId()));
          }
        }
        TimeRouterFactory.this.config = config;
      }
    }
    
  }
  
  @VisibleForTesting
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(CONFIG_KEY))) {
      tsdb.getConfig().register(
          ConfigurationEntrySchema.newBuilder()
          .setKey(getConfigKey(CONFIG_KEY))
          .setType(TYPE_REF)
          .setDescription("The JSON or YAML config for the router.")
          .isDynamic()
          .isNullable()
          .setSource(getClass().getName())
          .build());
    }
    tsdb.getConfig().bind(getConfigKey(CONFIG_KEY), new SettingsCallback());

    if (!tsdb.getConfig().hasProperty(getConfigKey(SPLIT_KEY))) {
      tsdb.getConfig().register(getConfigKey(SPLIT_KEY), false, true,
              "Whether or not to split queries across backends for" +
                      " hot/warm/cold storage of the given data type.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(FAIL_OVERLAP_KEY))) {
      tsdb.getConfig().register(getConfigKey(FAIL_OVERLAP_KEY), false, true,
              "Whether or not to fail split queries when there isn't " +
                      "enough overlap between sources to fully satisfy queries " +
                      "with nodes like sliding windows or rates.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(OVERLAP_INTERVAL_KEY))) {
      tsdb.getConfig().register(getConfigKey(OVERLAP_INTERVAL_KEY), 5, true,
              "For split queries with compute windows and/or " +
                      "downsampling, a maximum number of intervals to attempt " +
                      "to fetch for overlapping of data. Note that this is a " +
                      "best-effort value in that if a smaller number of overlaps " +
                      "are available for a query, only the available overlaps " +
                      "will be processed.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(OVERLAP_DURATION_KEY))) {
      tsdb.getConfig().register(getConfigKey(OVERLAP_DURATION_KEY), "5m", true,
              "For split queries that can overlap, the maximum " +
                      "amount of time they should overlap.");
    }

    if (!tsdb.getConfig().hasProperty(getConfigKey(DEFAULT_QUERY_TIMEOUT_KEY))) {
      tsdb.getConfig().register(getConfigKey(DEFAULT_QUERY_TIMEOUT_KEY), "1m", true,
              "A default timeout for queries to sources.");
    }
  }

  /**
   * Helper to build the config key with a factory id.
   * @param suffix The non-null and non-empty config suffix.
   * @return The key containing the id.
   */
  @VisibleForTesting
  String getConfigKey(final String suffix) {
    if (id == null || id == TYPE) { // yes, same addy here.
      return KEY_PREFIX + suffix;
    } else {
      return KEY_PREFIX + id + "." + suffix;
    }
  }

  @VisibleForTesting
  protected void setConfig(final List<TimeRouterConfigEntry> config) {
    this.config = config;
    Collections.sort(config, new ConfigSorter());
  }
}
