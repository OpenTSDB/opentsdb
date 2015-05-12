package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.SignedBytes;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.stats.StopTimerCallback;
import net.opentsdb.storage.TsdbStore;
import com.typesafe.config.Config;
import net.opentsdb.uid.TimeseriesId;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
  public Deferred<DataPoints[]> executeQuery(Query query) {
    return store.executeQuery(query)
            .addCallback(new GroupByAndAggregateCB(query));
  }

  /**
   * Callback that should be attached the the output of
   * {@link net.opentsdb.storage.TsdbStore#executeQuery} to group and sort the results.
   */
  private class GroupByAndAggregateCB implements
          Callback<DataPoints[], ImmutableList<DataPoints>> {

    private final Query query;

    public GroupByAndAggregateCB(final Query query) {
      this.query = query;
    }

    /**
     * Creates the {@link SpanGroup}s to form the final results of this query.
     *
     * @param dps The {@link Span}s found for this query ({@link TSDB#findSpans}).
     *            Can be {@code null}, in which case the array returned will be empty.
     * @return A possibly empty array of {@link SpanGroup}s built according to
     * any 'GROUP BY' formulated in this query.
     */
    @Override
    public DataPoints[] call(final ImmutableList<DataPoints> dps) {
      if (dps.isEmpty()) {
        // Result is empty so return an empty array
        return new DataPoints[0];
      }

      TreeMultimap<String, DataPoints> spans2 = TreeMultimap.create();

      for (DataPoints dp : dps) {
        List<String> tsuids = dp.getTSUIDs();
        spans2.put(tsuids.get(0), dp);
      }

      Set<Span> spans = Sets.newTreeSet();
      for (String tsuid : spans2.keySet()) {
        spans.add(new Span(ImmutableSortedSet.copyOf(spans2.get(tsuid))));
      }

      final List<byte[]> group_bys = query.getGroupBys();
      if (group_bys == null) {
        // We haven't been asked to find groups, so let's put all the spans
        // together in the same group.
        final SpanGroup group = SpanGroup.create(query, spans);
        return new SpanGroup[]{group};
      }

      // Maps group value IDs to the SpanGroup for those values. Say we've
      // been asked to group by two things: foo=* bar=* Then the keys in this
      // map will contain all the value IDs combinations we've seen. If the
      // name IDs for `foo' and `bar' are respectively [0, 0, 7] and [0, 0, 2]
      // then we'll have group_bys=[[0, 0, 2], [0, 0, 7]] (notice it's sorted
      // by ID, so bar is first) and say we find foo=LOL bar=OMG as well as
      // foo=LOL bar=WTF and that the IDs of the tag values are:
      // LOL=[0, 0, 1] OMG=[0, 0, 4] WTF=[0, 0, 3]
      // then the map will have two keys:
      // - one for the LOL-OMG combination: [0, 0, 1, 0, 0, 4] and,
      // - one for the LOL-WTF combination: [0, 0, 1, 0, 0, 3].
      final TreeMap<byte[], SpanGroup> groups = new TreeMap<>(SignedBytes.lexicographicalComparator());
      final byte[] group = new byte[group_bys.size() * Const.TAG_VALUE_WIDTH];

      for (final Span span : spans) {
        final Map<byte[], byte[]> dp_tags = span.tags();

        int i = 0;
        // TODO(tsuna): The following loop has a quadratic behavior. We can
        // make it much better since both the row key and group_bys are sorted.
        for (final byte[] tag_id : group_bys) {
          final byte[] value_id = dp_tags.get(tag_id);

          if (value_id == null) {
            throw new IllegalDataException("The " + span + " did not contain a " +
                    "value for the tag key " + Arrays.toString(tag_id));
          }

          System.arraycopy(value_id, 0, group, i, Const.TAG_VALUE_WIDTH);
          i += Const.TAG_VALUE_WIDTH;
        }

        //LOG.info("Span belongs to group " + Arrays.toString(group) + ": " + Arrays.toString(row));
        SpanGroup thegroup = groups.get(group);
        if (thegroup == null) {
          // Copy the array because we're going to keep `group' and overwrite
          // its contents. So we want the collection to have an immutable copy.
          final byte[] group_copy = Arrays.copyOf(group, group.length);

          thegroup = SpanGroup.create(query, null);
          groups.put(group_copy, thegroup);
        }
        thegroup.add(span);
      }

      //for (final Map.Entry<byte[], SpanGroup> entry : groups) {
      // LOG.info("group for " + Arrays.toString(entry.getKey()) + ": " + entry.getValue());
      //}
      return groups.values().toArray(new SpanGroup[groups.size()]);
    }
  }
}
