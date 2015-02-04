package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import net.opentsdb.uid.UidResolver;
import net.opentsdb.uid.UniqueId;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Map.Entry;
import static net.opentsdb.core.DataPointsClient.checkTimestamp;
import static net.opentsdb.uid.UniqueIdType.TAGV;
import static org.hbase.async.Bytes.ByteMap;

public class QueryBuilder {
  /**
   * The TSDB we belong to.
   */
  private final TSDB tsdb;

  /**
   * Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int).
   */
  private Optional<Long> start_time = Optional.absent();

  /**
   * End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int).
   */
  private Optional<Long> end_time = Optional.absent();

  /**
   * ID of the metric being looked up.
   */
  private Deferred<byte[]> metric;

  /**
   * Tags of the metrics being looked up.
   * Each tag is a byte array holding the ID of both the name and value
   * of the tag.
   * Invariant: an element cannot be both in this array and in group_bys.
   */
  private Deferred<ArrayList<byte[]>> tags;

  /**
   * If true, use rate of change instead of actual values.
   */
  private boolean rate;

  /**
   * Specifies the various options for rate calculations
   */
  private RateOptions rate_options;

  /**
   * Aggregator function to use.
   */
  private Aggregator aggregator;

  /**
   * Downsampling function to use, if any (can be {@code null}).
   * If this is non-null, {@code sample_interval_ms} must be strictly positive.
   */
  private Aggregator downsampler;

  /**
   * Minimum time interval (in milliseconds) wanted between each data point.
   */
  private long sample_interval_ms;

  /**
   * Tags by which we must group the results.
   * Each element is a tag ID.
   * Invariant: an element cannot be both in this array and in {@code tags}.
   */
  private List<Deferred<byte[]>> group_bys_deferreds;
  private Map<String, byte[]> group_bys;

  /**
   * Values we may be grouping on.
   * For certain elements in {@code group_bys}, we may have a specific list of
   * values IDs we're looking for.  Those IDs are stored in this map.  The key
   * is an element of {@code group_bys} (so a tag name ID) and the values are
   * tag value IDs (at least two).
   */
  private List<Deferred<ArrayList<byte[]>>> group_by_values_deferreds;
  private Map<String, ArrayList<byte[]>> group_by_values;

  /**
   * Optional list of TSUIDs to fetch and aggregate instead of a metric
   */
  private List<String> tsuids;

  public QueryBuilder(final TSDB tsdb) {
    this.tsdb = checkNotNull(tsdb);
  }

  /**
   * Set the start time of this query builder. This will set the end time to
   * the current time in milliseconds.
   */
  public QueryBuilder withStartTime(final long start_time) {
    return withStartAndEndTime(start_time, Optional.<Long>absent());
  }

  /**
   * Set the start and end time of this query builder.
   */
  public QueryBuilder withStartAndEndTime(final long start_time, final long end_time) {
    return withStartAndEndTime(start_time, Optional.of(end_time));
  }

  /**
   * Set the start and end time of this query builder.
   */
  public QueryBuilder withStartAndEndTime(final long start_time,
                                          final Optional<Long> end_time) {
    checkTimestamp(start_time);

    if (end_time.isPresent()) {
      checkTimestamp(end_time.get());

      if (start_time >= end_time.get()) {
        throw new IllegalArgumentException("new start time (" + start_time
                + ") is greater than or equal to end time: " + end_time.get());
      }
    }

    this.start_time = Optional.of(start_time);
    this.end_time = end_time;

    return this;
  }

  /**
   * Set the metric this query builder should search for. You should call
   * this or {@link #withTSUIDS(java.util.List)}, not both.
   */
  public QueryBuilder withMetric(final String metric) {
    this.metric = tsdb.getUniqueIdClient().metrics.getId(checkNotNull(metric));
    return this;
  }

  /**
   * Set the tags this query should search for.
   */
  public QueryBuilder withTags(final Map<String, String> tags) {
    findGroupBys(checkNotNull(tags));
    this.tags = tsdb.getUniqueIdClient().getAllTags(tags);
    return this;
  }

  /**
   * Set the aggregator the created query will use.
   */
  public QueryBuilder withAggregator(final Aggregator function) {
    this.aggregator = checkNotNull(function);
    return this;
  }

  /**
   * Let this builder know whether to calculate rates or not;
   */
  public QueryBuilder shouldCalculateRate(final boolean rate) {
    return shouldCalculateRate(rate, new RateOptions());
  }

  /**
   * Let this builder know whether to calculate rates or not and with the
   * specified options.
   */
  public QueryBuilder shouldCalculateRate(final boolean rate,
                                              final RateOptions rate_options) {
    this.rate = rate;
    this.rate_options = checkNotNull(rate_options);
    return this;
  }

  /**
   * Tell the builder to build a query that searches for the specified TSUIDS
   * . You should call this or {@link #withMetric(String)}, not both.
   */
  public QueryBuilder withTSUIDS(final List<String> tsuids) {
    checkArgument(!tsuids.isEmpty(), "Empty or missing TSUID list not allowed");

    String first_metric = "";
    for (final String tsuid : tsuids) {
      if (first_metric.isEmpty()) {
        first_metric = tsuid.substring(0, Const.METRICS_WIDTH * 2)
                .toUpperCase();
        continue;
      }

      final String metric = tsuid.substring(0, Const.METRICS_WIDTH * 2)
              .toUpperCase();
      if (!first_metric.equals(metric)) {
        throw new IllegalArgumentException(
                "One or more TSUIDs did not share the same metric");
      }
    }

    // the metric will be set with the scanner is configured
    this.tsuids = tsuids;

    return this;
  }

  /**
   * Downsamples the results by specifying a fixed interval between points.
   * <p>
   * Technically, downsampling means reducing the sampling interval.  Here
   * the idea is similar.  Instead of returning every single data point that
   * matched the query, we want one data point per fixed time interval.  The
   * way we get this one data point is by aggregating all the data points of
   * that interval together using an {@link Aggregator}.  This enables you
   * to compute things like the 5-minute average or 10 minute 99th percentile.
   * @param interval Number of seconds wanted between each data point.
   * @param downsampler Aggregation function to use to group data points
   * within an interval.
   * @throws IllegalArgumentException if the interval is not greater than 0
   * @throws NullPointerException     if the aggregation function is null
   */
  public QueryBuilder downsample(final long interval, final Aggregator downsampler) {
    checkArgument(interval > 0, "interval must be greater than zero but was %s",
            interval);

    this.downsampler = checkNotNull(downsampler);
    this.sample_interval_ms = interval;
    return this;
  }

  /**
   * Extracts all the tags we must use to group results.
   * <ul>
   * <li>If a tag has the form {@code name=*} then we'll create one
   * group per value we find for that tag.</li>
   * <li>If a tag has the form {@code name={v1,v2,..,vN}} then we'll
   * create {@code N} groups.</li>
   * </ul>
   * In the both cases above, {@code name} will be stored in the
   * {@code group_bys} attribute.  In the second case specifically,
   * the {@code N} values would be stored in {@code group_by_values},
   * the key in this map being {@code name}.
   *
   * @param tags The tags from which to extract the 'GROUP BY's.
   *             Each tag that represents a 'GROUP BY' will be removed from the map
   *             passed in argument.
   */
  private void findGroupBys(final Map<String, String> tags) {
    // Callbacks used later.
    class GroupBysCB implements Callback<Object, byte[]> {
      private final String tagk;

      private GroupBysCB(final String tagk) {
        this.tagk = tagk;
      }

      @Override
      public Object call(byte[] tagk_id) throws Exception {
        group_bys.put(tagk, tagk_id);
        return null;
      }
    }

    class GroupByValuesCB implements Callback<Object, ArrayList<byte[]>> {
      private final String tagk;

      public GroupByValuesCB(String tagk) {
        this.tagk = tagk;
      }

      @Override
      public Object call(ArrayList<byte[]> tagv_ids) throws Exception {
        group_by_values.put(tagk, tagv_ids);
        return null;
      }
    }

    // Method code starting here.
    if (group_bys_deferreds == null) {
      group_bys_deferreds = Lists.newArrayList();
      group_bys = Maps.newConcurrentMap();
    }
    if (group_by_values_deferreds == null) {
      group_by_values_deferreds = Lists.newArrayList();
      group_by_values = Maps.newConcurrentMap();
    }

    final Iterator<Entry<String, String>> i = tags.entrySet().iterator();
    final Splitter tagv_splitter = Splitter.on('|');
    final UidResolver uidResolver = new UidResolver(tsdb);

    while (i.hasNext()) {
      final Entry<String, String> tag = i.next();
      final String tagvalue = tag.getValue();
      final String tagk = tag.getKey();

      // 'GROUP BY' with any value OR Multiple possible values.
      if ("*".equals(tagvalue) || tagvalue.indexOf('|', 1) >= 0) {
        Deferred<byte[]> tagk_id = tsdb.getUniqueIdClient().tag_names.getId(tagk);
        group_bys_deferreds.add(tagk_id);
        tagk_id.addCallback(new GroupBysCB(tagk));

        i.remove();

        if (tagvalue.charAt(0) == '*') {
          continue;  // For a 'GROUP BY' with any value, we're done.
        }

        // 'GROUP BY' with specific values.  Need to split the values
        // to group on and store their IDs in group_by_values.
        Iterable<String> tagvs = tagv_splitter.split(tagvalue);

        Deferred<ArrayList<byte[]>> tagv_ids = uidResolver.resolve(tagvs, TAGV);
        group_by_values_deferreds.add(tagv_ids);
        tagv_ids.addCallback(new GroupByValuesCB(tagk));
      }
    }
  }

  /**
   * Build the query with the options given to this builder. If {@link
   * #tsuids} has been set using {@link #withTSUIDS(java.util.List)} then
   * TSUIDS will be used for this query. Otherwise the {@link #metric} and
   * {@link #tags} will be used.
   */
  public Deferred<Query> createQuery() {
    checkState(start_time.isPresent(), "A start time must be provided");
    checkState(metric != null || tsuids != null,
            "Either a metric or TSUIDS must be privoded");
    if (tags == null) {
      tags = Deferred.fromResult(Lists.<byte[]>newArrayList());
      group_bys = Maps.newTreeMap();
      group_bys_deferreds = ImmutableList.of(Deferred.fromResult(new byte[]{}));
      group_by_values = Maps.newTreeMap();
      group_by_values_deferreds = ImmutableList.of(Deferred.fromResult(Lists.<byte[]>newArrayList()));
    }
    if (tsuids != null) {
      return Deferred.fromResult(createFromTSUIDS());
    } else {
      return createFromMetricsAndTags();
    }
  }

  private Query createFromTSUIDS() {
    final String tsuid = tsuids.get(0);
    final String metric_uid = tsuid.substring(0, Const.METRICS_WIDTH * 2);
    final byte[] metric_id = UniqueId.stringToUid(metric_uid);

    return new Query(metric_id, tsuids, start_time.get(), end_time,
            aggregator, downsampler, sample_interval_ms, rate,
            rate_options);
  }

  private Deferred<Query> createFromMetricsAndTags() {
    class GroupByValuesCB implements Callback<Query,ArrayList<ArrayList<byte[]>>> {
      private final byte[] metric_id;
      private final ArrayList<byte[]> tag_ids;

      public GroupByValuesCB(final byte[] metric_id,
                             final ArrayList<byte[]> tag_ids) {
        this.metric_id = metric_id;
        this.tag_ids = tag_ids;
      }

      @Override
      public Query call(ArrayList<ArrayList<byte[]>> arg) throws Exception {
        List<byte[]> group_by = Lists.newArrayList();
        group_by.addAll(group_bys.values());

        ByteMap<ArrayList<byte[]>> group_by_vals = new ByteMap<ArrayList<byte[]>>();
        for (Entry<String, ArrayList<byte[]>> group_by_val_entry : group_by_values.entrySet()) {
          byte[] tagk_id = group_bys.get(group_by_val_entry.getKey());
          group_by_vals.put(tagk_id, group_by_val_entry.getValue());
        }

        return new Query(metric_id, tag_ids, start_time.get(), end_time,
                aggregator, downsampler, sample_interval_ms, rate,
                rate_options, group_by, group_by_vals);
      }
    }

    class GroupBysCB implements Callback<Deferred<Query>, ArrayList<byte[]>> {
      private final byte[] metric_id;
      private final ArrayList<byte[]> tag_ids;

      public GroupBysCB(final byte[] metric_id,
                        final ArrayList<byte[]> tag_ids) {
        this.metric_id = metric_id;
        this.tag_ids = tag_ids;
      }

      @Override
      public Deferred<Query> call(ArrayList<byte[]> group_bys_tagk_ids) {
        return Deferred.group(group_by_values_deferreds)
                .addCallback(new GroupByValuesCB(metric_id, tag_ids));
      }
    }

    class TagsCB implements Callback<Deferred<Query>, ArrayList<byte[]>> {
      private final byte[] metric_id;

      public TagsCB(byte[] metric_id) {
        this.metric_id = metric_id;
      }

      @Override
      public Deferred<Query> call(ArrayList<byte[]> tag_ids) {
        return Deferred.group(group_bys_deferreds)
                .addCallbackDeferring(new GroupBysCB(metric_id, tag_ids));
      }
    }

    class MetricCB implements Callback<Deferred<Query>, byte[]> {
      @Override
      public Deferred<Query> call(byte[] metric_id) {
        return tags.addCallbackDeferring(new TagsCB(metric_id));
      }
    }

    return metric.addCallbackDeferring(new MetricCB());
  }
}
