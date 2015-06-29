package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.addCallback;
import static net.opentsdb.stats.Metrics.name;

import net.opentsdb.plugins.PluginError;
import net.opentsdb.plugins.RealTimePublisher;
import net.opentsdb.stats.StopTimerCallback;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.TimeSeriesId;
import net.opentsdb.utils.InvalidConfigException;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Strings;
import com.google.common.primitives.SignedBytes;
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
  private final LabelClient labelClient;
  private final RealTimePublisher publisher;

  private final Timer addDataPointTimer;
  private final byte maxTags;

  /**
   * Create a new instance using the given non-null arguments to configure itself.
   */
  @Inject
  public DataPointsClient(final TsdbStore store,
                          final LabelClient labelClient,
                          final RealTimePublisher realTimePublisher,
                          final MetricRegistry metricRegistry,
                          final Config config) {
    this.store = checkNotNull(store);
    this.labelClient = checkNotNull(labelClient);
    this.publisher = checkNotNull(realTimePublisher);

    this.addDataPointTimer = metricRegistry.timer(name("add_data_point"));

    // The config library unfortunately doesn't have any API to get any smaller primitive type than
    // ints so we have to do a little dance to make sure the value is not too extreme since it can
    // have a significant impact on query performance.
    final int configMaxTags = config.getInt("tsdb.core.max_tags");

    if (configMaxTags > Byte.MAX_VALUE) {
      throw new InvalidConfigException(config.getValue("tsdb.core.max_tags"),
          "The number of maximum allowed tags must not be larger than " + Byte.MAX_VALUE);
    }

    if (configMaxTags < 1) {
      throw new InvalidConfigException(config.getValue("tsdb.core.max_tags"),
          "At least one tag must be allowed");
    }

    this.maxTags = SignedBytes.checkedCast(configMaxTags);
  }

  /**
   * Validates the given metric and tags.
   *
   * @throws IllegalArgumentException if any of the arguments aren't valid.
   */
  private void checkMetricAndTags(final String metric, final Map<String, String> tags) {
    checkArgument(!Strings.isNullOrEmpty(metric), "Missing metric name", metric, tags);
    checkArgument(!tags.isEmpty(), "At least one tag is required", metric, tags);
    checkArgument(tags.size() <= maxTags,
        "No more than %s tags are allowed but there are %s",
        maxTags, tags.size(), metric, tags);

    LabelClient.validateLabelName("metric name", metric);
    for (final Map.Entry<String, String> tag : tags.entrySet()) {
      LabelClient.validateLabelName("tag name", tag.getKey());
      LabelClient.validateLabelName("tag value", tag.getValue());
    }
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
    Timestamp.checkStartTime(timestamp);
    checkMetricAndTags(metric, tags);

    class AddPointFunction implements AsyncFunction<TimeSeriesId, Void> {
      @Override
      public ListenableFuture<Void> apply(final TimeSeriesId timeSeriesId) {
        ListenableFuture<Void> result = store.addPoint(timeSeriesId, timestamp, value);

        addCallback(publisher.publishDataPoint(metric, timestamp, value, tags, timeSeriesId),
            new PluginError(publisher));

        return result;
      }
    }

    final Timer.Context time = addDataPointTimer.time();

    final ListenableFuture<Void> addPointComplete = Futures.transform(
        labelClient.getTimeSeriesId(metric, tags), new AddPointFunction());

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
    Timestamp.checkStartTime(timestamp);
    checkMetricAndTags(metric, tags);

    class AddPointFunction implements AsyncFunction<TimeSeriesId, Void> {
      @Override
      public ListenableFuture<Void> apply(final TimeSeriesId timeSeriesId) {
        ListenableFuture<Void> result = store.addPoint(timeSeriesId, timestamp, value);

        addCallback(publisher.publishDataPoint(metric, timestamp, value, tags, timeSeriesId),
            new PluginError(publisher));

        return result;
      }
    }

    final Timer.Context time = addDataPointTimer.time();

    final ListenableFuture<Void> addPointComplete = Futures.transform(
        labelClient.getTimeSeriesId(metric, tags), new AddPointFunction());

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
    Timestamp.checkStartTime(timestamp);
    checkMetricAndTags(metric, tags);

    class AddPointFunction implements AsyncFunction<TimeSeriesId, Void> {
      @Override
      public ListenableFuture<Void> apply(final TimeSeriesId timeSeriesId) {
        ListenableFuture<Void> result = store.addPoint(timeSeriesId, timestamp, value);

        addCallback(publisher.publishDataPoint(metric, timestamp, value, tags, timeSeriesId),
            new PluginError(publisher));

        return result;
      }
    }

    final Timer.Context time = addDataPointTimer.time();

    final ListenableFuture<Void> addPointComplete = Futures.transform(
        labelClient.getTimeSeriesId(metric, tags), new AddPointFunction());

    StopTimerCallback.stopOn(time, addPointComplete);

    return addPointComplete;
  }

  /**
   * Executes the query asynchronously
   *
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
