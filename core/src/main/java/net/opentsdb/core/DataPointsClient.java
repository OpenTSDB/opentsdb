package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.addCallback;
import static net.opentsdb.stats.Metrics.name;

import net.opentsdb.plugins.PluginError;
import net.opentsdb.plugins.RTPublisher;
import net.opentsdb.stats.StopTimerCallback;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.TimeseriesId;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.Config;

import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DataPointsClient {
  private final TsdbStore store;
  private final Config config;
  private final IdClient idClient;
  private final MetaClient metaClient;
  private final RTPublisher realtimePublisher;

  private final Timer addDataPointTimer;

  @Inject
  public DataPointsClient(final TsdbStore store,
                          final Config config,
                          final IdClient idClient,
                          final MetaClient metaClient,
                          final RTPublisher realtimePublisher,
                          final MetricRegistry metricRegistry) {
    this.store = checkNotNull(store);
    this.config = checkNotNull(config);
    this.idClient = checkNotNull(idClient);
    this.metaClient = checkNotNull(metaClient);
    this.realtimePublisher = checkNotNull(realtimePublisher);

    this.addDataPointTimer = metricRegistry.timer(name("add_data_point"));
  }

  /**
   * Validates the given metric and tags.
   *
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

    IdClient.validateUidName("metric name", metric);
    for (final Map.Entry<String, String> tag : tags.entrySet()) {
      IdClient.validateUidName("tag name", tag.getKey());
      IdClient.validateUidName("tag value", tag.getValue());
    }
  }

  /**
   * Validates that the timestamp is within valid bounds.
   *
   * @throws IllegalArgumentException if the timestamp isn't within bounds.
   */
  static long checkTimestamp(long timestamp) {
    checkArgument(timestamp >= 0, "The timestamp must be positive and greater than zero but was %s", timestamp);

    return timestamp;
  }

  /**
   * Adds a single floating-point value data point in the TSDB.
   *
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A future that indicates the completion of the request or an error. timestamp added or 0
   * for the first timestamp, or if the difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains illegal characters.
   * @throws IllegalArgumentException if the value is NaN or infinite.
   * @throws IllegalArgumentException if the tags list is empty or one of the elements contains
   * illegal characters.
   */
  public ListenableFuture<Void> addPoint(final String metric,
                                         final long timestamp,
                                         final float value,
                                         final Map<String, String> tags) {
    checkTimestamp(timestamp);
    checkMetricAndTags(metric, tags);

    class RowKeyCB implements AsyncFunction<TimeseriesId, Void> {
      @Override
      public ListenableFuture<Void> apply(final TimeseriesId tsuid) {
        ListenableFuture<Void> result = store.addPoint(tsuid, timestamp, value);

        addCallback(realtimePublisher.publishDataPoint(metric, timestamp, value, tags, tsuid),
            new PluginError(realtimePublisher));

        return result;
      }
    }

    final Timer.Context time = addDataPointTimer.time();

    final ListenableFuture<Void> addPointComplete = Futures.transform(
        idClient.getTSUID(metric, tags), new RowKeyCB());

    StopTimerCallback.stopOn(time, addPointComplete);

    return addPointComplete;
  }

  /**
   * Adds a double precision floating-point value data point in the TSDB.
   *
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A future that indicates the completion of the request or an error.
   * @throws IllegalArgumentException if the timestamp is less than or equal to the previous
   * timestamp added or 0 for the first timestamp, or if the difference with the previous timestamp
   * is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains illegal characters.
   * @throws IllegalArgumentException if the value is NaN or infinite.
   * @throws IllegalArgumentException if the tags list is empty or one of the elements contains
   * illegal characters.
   * @since 1.2
   */
  public ListenableFuture<Void> addPoint(final String metric,
                                         final long timestamp,
                                         final double value,
                                         final Map<String, String> tags) {
    checkTimestamp(timestamp);
    checkMetricAndTags(metric, tags);

    class RowKeyCB implements AsyncFunction<TimeseriesId, Void> {
      @Override
      public ListenableFuture<Void> apply(final TimeseriesId tsuid) {
        ListenableFuture<Void> result = store.addPoint(tsuid, timestamp, value);

        addCallback(realtimePublisher.publishDataPoint(metric, timestamp, value, tags, tsuid),
            new PluginError(realtimePublisher));

        return result;
      }
    }

    final Timer.Context time = addDataPointTimer.time();

    final ListenableFuture<Void> addPointComplete = Futures.transform(
        idClient.getTSUID(metric, tags), new RowKeyCB());

    StopTimerCallback.stopOn(time, addPointComplete);

    return addPointComplete;
  }

  /**
   * Adds a single integer value data point in the TSDB.
   *
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A future that indicates the completion of the request or an error.
   * @throws IllegalArgumentException if the timestamp is less than or equal to the previous
   * timestamp added or 0 for the first timestamp, or if the difference with the previous timestamp
   * is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains illegal characters.
   * @throws IllegalArgumentException if the tags list is empty or one of the elements contains
   * illegal characters.
   */
  public ListenableFuture<Void> addPoint(final String metric,
                                         final long timestamp,
                                         final long value,
                                         final Map<String, String> tags) {
    checkTimestamp(timestamp);
    checkMetricAndTags(metric, tags);

    class RowKeyCB implements AsyncFunction<TimeseriesId, Void> {
      @Override
      public ListenableFuture<Void> apply(final TimeseriesId tsuid) {
        ListenableFuture<Void> result = store.addPoint(tsuid, timestamp, value);

        addCallback(realtimePublisher.publishDataPoint(metric, timestamp, value, tags, tsuid),
            new PluginError(realtimePublisher));

        return result;
      }
    }

    final Timer.Context time = addDataPointTimer.time();

    final ListenableFuture<Void> addPointComplete = Futures.transform(
        idClient.getTSUID(metric, tags), new RowKeyCB());

    StopTimerCallback.stopOn(time, addPointComplete);

    return addPointComplete;
  }

  /**
   * Executes the query asynchronously
   *
   * @param query
   * @return The data points matched by this query. Each element in the non-{@code null} but
   * possibly empty array returned corresponds to one time series for which some data points have
   * been matched by the query.
   * @since 1.2
   */
  // TODO
  public ListenableFuture<DataPoints[]> executeQuery(Object query) {
    //return store.executeQuery(query);
    return null;
  }
}
