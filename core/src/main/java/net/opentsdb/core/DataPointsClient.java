package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.plugins.PluginError;
import net.opentsdb.plugins.RTPublisher;
import net.opentsdb.stats.StopTimerCallback;
import net.opentsdb.storage.TsdbStore;
import com.typesafe.config.Config;
import net.opentsdb.uid.TimeseriesId;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.stats.Metrics.name;

@Singleton
public class DataPointsClient {
  private final TsdbStore store;
  private final Config config;
  private final UniqueIdClient uniqueIdClient;
  private final MetaClient metaClient;
  private final RTPublisher realtimePublisher;

  private final Timer addDataPointTimer;

  /**
   * Validates the given metric and tags.
   * @throws IllegalArgumentException if any of the arguments aren't valid.
   */
  static void checkMetricAndTags(final String metric, final Map<String, String> tags) {
    if (tags.size() <= 0) {
      throw new IllegalArgumentException("Need at least one tags (metric="
          + metric + ", tags=" + tags + ')');
    } else if (tags.size() > Const.MAX_NUM_TAGS) {
      throw new IllegalArgumentException("Too many tags: " + tags.size()
          + " maximum allowed: " + Const.MAX_NUM_TAGS + ", tags: " + tags);
    }

    UniqueIdClient.validateUidName("metric name", metric);
    for (final Map.Entry<String, String> tag : tags.entrySet()) {
      UniqueIdClient.validateUidName("tag name", tag.getKey());
      UniqueIdClient.validateUidName("tag value", tag.getValue());
    }
  }

  /**
   * Validates that the timestamp is within valid bounds.
   * @throws IllegalArgumentException if the timestamp isn't within
   * bounds.
   */
  static long checkTimestamp(long timestamp) {
    checkArgument(timestamp >= 0, "The timestamp must be positive but was %s", timestamp);
    checkArgument((timestamp & Const.SECOND_MASK) == 0 || timestamp <= Const.MAX_MS_TIMESTAMP,
            "The timestamp was too large (%s)", timestamp);

    return timestamp;
  }

  @Inject
  public DataPointsClient(final TsdbStore store,
                          final Config config,
                          final UniqueIdClient uniqueIdClient,
                          final MetaClient metaClient,
                          final RTPublisher realtimePublisher,
                          final MetricRegistry metricRegistry) {
    this.store = checkNotNull(store);
    this.config = checkNotNull(config);
    this.uniqueIdClient = checkNotNull(uniqueIdClient);
    this.metaClient = checkNotNull(metaClient);
    this.realtimePublisher = checkNotNull(realtimePublisher);

    this.addDataPointTimer = metricRegistry.timer(name("add_data_point"));
  }

  /**
   * Adds a single floating-point value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the value is NaN or infinite.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   */
  public Deferred<Void> addPoint(final String metric,
                                   final long timestamp,
                                   final float value,
                                   final Map<String, String> tags) {
    checkTimestamp(timestamp);
    checkMetricAndTags(metric, tags);

    class RowKeyCB implements Callback<Deferred<Void>, TimeseriesId> {
      @Override
      public Deferred<Void> call(final TimeseriesId tsuid) throws Exception {
        Deferred<Void> result = store.addPoint(tsuid, timestamp, value);

        realtimePublisher.publishDataPoint(metric, timestamp, value, tags, tsuid)
            .addErrback(new PluginError(realtimePublisher));

        return result;
      }
    }

    final Timer.Context time = addDataPointTimer.time();

    return uniqueIdClient.getTSUID(metric, tags)
        .addCallbackDeferring(new RowKeyCB())
        .addBoth(new StopTimerCallback<Void>(time));
  }

  /**
   * Adds a double precision floating-point value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the value is NaN or infinite.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   * @since 1.2
   */
  public Deferred<Void> addPoint(final String metric,
                                   final long timestamp,
                                   final double value,
                                   final Map<String, String> tags) {
    checkTimestamp(timestamp);
    checkMetricAndTags(metric, tags);

    class RowKeyCB implements Callback<Deferred<Void>, TimeseriesId> {
      @Override
      public Deferred<Void> call(final TimeseriesId tsuid) throws Exception {
        Deferred<Void> result = store.addPoint(tsuid, timestamp, value);

        realtimePublisher.publishDataPoint(metric, timestamp, value, tags, tsuid)
            .addErrback(new PluginError(realtimePublisher));

        return result;
      }
    }

    final Timer.Context time = addDataPointTimer.time();

    return uniqueIdClient.getTSUID(metric, tags)
        .addCallbackDeferring(new RowKeyCB())
        .addBoth(new StopTimerCallback<Void>(time));
  }

  /**
   * Adds a single integer value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   */
  public Deferred<Void> addPoint(final String metric,
                                   final long timestamp,
                                   final long value,
                                   final Map<String, String> tags) {
    checkTimestamp(timestamp);
    checkMetricAndTags(metric, tags);

    class RowKeyCB implements Callback<Deferred<Void>, TimeseriesId> {
      @Override
      public Deferred<Void> call(final TimeseriesId tsuid) throws Exception {
        Deferred<Void> result = store.addPoint(tsuid, timestamp, value);

        realtimePublisher.publishDataPoint(metric, timestamp, value, tags, tsuid)
            .addErrback(new PluginError(realtimePublisher));

        return result;
      }
    }

    final Timer.Context time = addDataPointTimer.time();

    return uniqueIdClient.getTSUID(metric, tags)
        .addCallbackDeferring(new RowKeyCB())
        .addBoth(new StopTimerCallback<Void>(time));
  }

  /**
   * Executes the query asynchronously
   * @return The data points matched by this query.
   * <p>
   * Each element in the non-{@code null} but possibly empty array returned
   * corresponds to one time series for which some data points have been
   * matched by the query.
   * @since 1.2
   * @param query
   */
  // TODO
  public Deferred<DataPoints[]> executeQuery(Object query) {
    //return store.executeQuery(query);
    return null;
  }
}
