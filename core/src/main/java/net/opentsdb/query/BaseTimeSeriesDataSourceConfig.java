//This file is part of OpenTSDB.
//Copyright (C) 2017-2021  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.base.Objects;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.QueryFilterFactory;
import net.opentsdb.utils.Comparators;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;

import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A simple base config class for {@link TimeSeriesDataSource} nodes.
 *
 * TODO - this is ugly and needs a lot of re-org and work.
 *
 * @since 3.0
 */
@JsonInclude(Include.NON_DEFAULT)
@JsonDeserialize(builder = BaseTimeSeriesDataSourceConfig.Builder.class)
public abstract class BaseTimeSeriesDataSourceConfig<B extends 
    BaseTimeSeriesDataSourceConfig.Builder<B, C>, C extends 
        BaseQueryNodeConfig & TimeSeriesDataSourceConfig> extends 
          BaseQueryNodeConfig<B, C> implements TimeSeriesDataSourceConfig<B, C> {
  
  /** The source provider ID. */
  private final String source_id;

  /** An optional namespace. */
  private final String namespace;

  /** An optional starting index for pagination. */
  private final int from;

  /** An optional size for pagination. */
  private final int size;

  /** A list of data types to fetch. If empty, fetch all. */
  private final List<String> types;

  /** A optional metric filter used to determine the metric(s) to fetch. */
  private final MetricFilter metric;

  /** An optional filter ID found in the query. */
  private final String filter_id;

  /** An optional filter. If filter_id is set, this is ignored. */
  private final QueryFilter filter;

  /** Whether or not to fetch only the last value. */
  private final boolean fetch_last;

  /** An optional list of nodes to push down to the driver. */
  private final List<QueryNodeConfig> push_down_nodes;

  /** An optional summary interval from an upstream downsampler. */
  private final String summary_interval;

  /** An optional list of summary aggregations from upstream. */
  private final List<String> summary_aggregations;

  /** An optional list of rollup intervals. */
  private final List<String> rollup_intervals;

  /** Optional offset interval. */
  private final String interval;

  /** Optional data source override. */
  private final String dataSource;

  /** TODO - fixup and doc. */
  protected final Pair<Boolean, TemporalAmount> amounts;

  /** Whether or not this node has been setup already. */
  protected final boolean has_been_setup;

  /** Optional override for the start time. */
  protected final TimeStamp start_override;

  /** Optional override for the end time. */
  protected final TimeStamp end_override;

  /**
   * Private ctor for the builder.
   * @param builder The non-null builder.
   */
  protected BaseTimeSeriesDataSourceConfig(final Builder builder) {
    super(builder);
    if (result_ids == null) {
      result_ids = Lists.newArrayList();
    }
    if (result_ids.isEmpty()) {
      // this could be a Collections.emptyList() which is immutable. So  build
      // a new one.
      if (!Strings.isNullOrEmpty(builder.dataSource)) {
        result_ids = Lists.newArrayList(
                new DefaultQueryResultId(id, builder.dataSource));
      } else {
        result_ids = Lists.newArrayList(new DefaultQueryResultId(id, id));
      }
    }
    dataSource = builder.dataSource;
    source_id = builder.sourceId;
    types = builder.types;
    namespace = builder.namespace;
    from = builder.from;
    size = builder.size;
    metric = builder.metric;
    filter_id = builder.filterId;
    filter = builder.filter;
    fetch_last = builder.fetchLast;
    push_down_nodes = builder.push_down_nodes == null ?
        Collections.emptyList() : builder.push_down_nodes;
    summary_interval = builder.summary_interval;
    summary_aggregations = builder.summary_aggregations == null ?
        Collections.emptyList() : builder.summary_aggregations;
    rollup_intervals = builder.rollup_intervals == null ?
        Collections.emptyList() : builder.rollup_intervals;
    has_been_setup = builder.has_been_setup;
    start_override = builder.start_override;
    end_override = builder.end_override;
    
    if (!Strings.isNullOrEmpty(builder.interval)) {
      DateTime.parseDuration(builder.interval);
      interval = builder.interval;

      // TODO - must be easier/cleaner ways.
      // TODO - handle calendaring
      final int count = DateTime.getDurationInterval(interval);
      final String units = DateTime.getDurationUnits(interval);
      final TemporalAmount amount = DateTime.parseDuration2(
          Integer.toString(count) + units);
      amounts = new Pair<Boolean, TemporalAmount>(true, amount);
    } else {
      interval = null;
      amounts = null;
    }
  }

  @Override
  public String getSourceId() {
    return source_id;
  }

  @Override
  public List<String> getTypes() {
    return types;
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public int getFrom() {
    return from;
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public MetricFilter getMetric() {
    return metric;
  }

  @Override
  public String getFilterId() {
    return filter_id;
  }

  @Override
  public QueryFilter getFilter() {
    return filter;
  }

  @Override
  public boolean getFetchLast() {
    return fetch_last;
  }

  @Override
  public List<QueryNodeConfig> getPushDownNodes() {
    return push_down_nodes;
  }
  
  @Override
  public Collection<String> pushDownSinks() {
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
  
  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public String getSummaryInterval() {
    return summary_interval;
  }

  @Override
  public List<String> getSummaryAggregations() {
    return summary_aggregations;
  }

  @Override
  public List<String> getRollupIntervals() {
    return rollup_intervals;
  }

  @Override
  public String getTimeShiftInterval() {
    return interval;
  }

  @Override
  public Pair<Boolean, TemporalAmount> timeShifts() {
    return amounts;
  }

  @Override
  public boolean hasBeenSetup() {
    return has_been_setup;
  }

  @Override
  public TimeStamp startTimestamp() {
    return start_override;
  }

  @Override
  public long getStartOverrideMs() {
    return start_override != null ? startTimestamp().msEpoch() : 0;
  }

  @Override
  public TimeStamp endTimestamp() {
    return end_override;
  }

  @Override
  public long getEndOverrideMs() {
    return end_override != null ? end_override.msEpoch() : 0;
  }

  @Override
  public String getDataSource() {
    return dataSource;
  }

  @Override
  public boolean joins() {
    return false;
  }
  
  @Override
  public boolean readCacheable() {
    return true;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    if (!super.equals(o)) {
      return false;
    }

    final BaseTimeSeriesDataSourceConfig tsconfig = (BaseTimeSeriesDataSourceConfig) o;

    final boolean result = Objects.equal(namespace, tsconfig.getNamespace())
            && Objects.equal(source_id, tsconfig.getSourceId())
            && Objects.equal(filter_id, tsconfig.getFilterId())
            && Objects.equal(metric, tsconfig.getMetric())
            && Objects.equal(filter, tsconfig.getFilter())
            && Objects.equal(fetch_last, tsconfig.getFetchLast())
            && Objects.equal(interval, tsconfig.getTimeShiftInterval());

    if (!result) {
      return false;
    }

    // comparing types
    if (!Comparators.ListComparison.equalLists(types, tsconfig.getTypes())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  @Override
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final Hasher hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(source_id), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(namespace), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(filter_id), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(interval), Const.UTF8_CHARSET)
            .putBoolean(fetch_last);
    if (start_override != null) {
      hc.putLong(start_override.msEpoch());
    }
    if (end_override != null) {
      hc.putLong(end_override.msEpoch());
    }
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(4);

    hashes.add(super.buildHashCode());

    if (metric != null) {
      hashes.add(metric.buildHashCode());
    }

    if (filter != null) {
      hashes.add(filter.buildHashCode());
    }

    if (types != null) {
      final List<String> keys = Lists.newArrayList(types);
      Collections.sort(keys);
      for (final String key : keys) {
        hc.putString(key, Const.UTF8_CHARSET);
      }
    }
    hashes.add(hc.hash());

    return Hashing.combineOrdered(hashes);
  }

  public static void cloneBuilder(final TimeSeriesDataSourceConfig config, 
                                  final Builder builder) {
    builder
        .setSourceId(config.getSourceId())
        .setTypes(config.getTypes() != null ? Lists.newArrayList(config.getTypes()) : null)
        .setNamespace(config.getNamespace())
        .setMetric(config.getMetric())
        .setFilterId(config.getFilterId())
        .setQueryFilter(config.getFilter())
        .setFetchLast(config.getFetchLast())
        .setFrom(config.getFrom())
        .setSize(config.getSize())
        .setRollupIntervals(
            config.getRollupIntervals() == null || config.getRollupIntervals().isEmpty()
                ? null
                : Lists.newArrayList(config.getRollupIntervals()))
        .setSummaryInterval(config.getSummaryInterval())
        .setSummaryAggregations(
            config.getRollupIntervals() == null || config.getSummaryAggregations().isEmpty()
                ? null
                : Lists.newArrayList(config.getSummaryAggregations()))
        .setPushDownNodes(
            config.getPushDownNodes() == null || config.getPushDownNodes().isEmpty()
                ? null
                : Lists.newArrayList(config.getPushDownNodes()))
        .setTimeShiftInterval(config.getTimeShiftInterval())
        .setDataSource(config.getDataSource())
        .setStartTimeStamp(config.startTimestamp())
        .setEndTimeStamp(config.endTimestamp())
        .setSources(config.getSources() != null ? 
            Lists.newArrayList(config.getSources()) : null)
        .setResultIds(config.resultIds() != null ? 
            Lists.newArrayList(config.resultIds()) : null)
        .setOverrides(config.getOverrides() != null ? 
            Maps.newHashMap(config.getOverrides()) : null)
        .setType(config.getType())
        .setId(config.getId());
    if (!config.getSources().isEmpty()) {
      builder.setSources(Lists.newArrayList(config.getSources()));
    }
  }

  public static void parseConfig(final ObjectMapper mapper,
                                 final TSDB tsdb,
                                 final JsonNode node,
                                 final Builder builder) {
    // TODO - types
    JsonNode n = node.get("sourceId");
    if (n != null && !n.isNull()) {
      builder.setSourceId(n.asText());
    }

    n = node.get("namespace");
    if (n != null && !n.isNull()) {
      builder.setNamespace(n.asText());
    }

    n = node.get("from");
    if (n != null && !n.isNull()) {
      builder.setFrom(n.asInt());
    }

    n = node.get("size");
    if (n != null && !n.isNull()) {
      builder.setSize(n.asInt());
    }

    JsonNode type_node;
    String type;
    QueryFilterFactory factory;
    QueryFilter filter;
    n = node.get("metric");
    if (n != null) {
      type_node = n.get("type");
      if (type_node == null) {
        throw new IllegalArgumentException("Missing the metric type field.");
      }
      type = type_node.asText();
      if (Strings.isNullOrEmpty(type)) {
        throw new IllegalArgumentException("Metric type field cannot be null or empty.");
      }

      factory = tsdb.getRegistry().getPlugin(QueryFilterFactory.class, type);
      if (factory == null) {
        throw new IllegalArgumentException("No query filter factory found for: " + type);
      }
      filter = factory.parse(tsdb, mapper, n);
      if (filter == null || !(filter instanceof MetricFilter)) {
        throw new IllegalArgumentException("Metric query filter was not "
            + "an instanceof MetricFilter: " + filter.getClass());
      }
      builder.setMetric((MetricFilter) filter);
    }
    n = node.get("types");
    if (n != null && !n.isNull()) {
      for (final JsonNode t : n) {
        builder.addType(t.asText());
      }
    }

    n = node.get("id");
    if (n == null || Strings.isNullOrEmpty(n.asText())) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    builder.setId(n.asText());

    n = node.get("fetchLast");
    if (n != null) {
      builder.setFetchLast(n.asBoolean());
    }

    n = node.get("filterId");
    if (n != null && !n.isNull()) {
      builder.setFilterId(n.asText());
    } else {
      n = node.get("filter");
      if (n != null && !n.isNull()) {
        type_node = n.get("type");
        if (type_node == null) {
          throw new IllegalArgumentException("Missing the filter type field.");
        }

        type = type_node.asText();
        if (Strings.isNullOrEmpty(type)) {
          throw new IllegalArgumentException("Filter type field cannot be null or empty.");
        }

        factory = tsdb.getRegistry().getPlugin(QueryFilterFactory.class, type);
        if (factory == null) {
          throw new IllegalArgumentException("No query filter factory found for: " + type);
        }
        filter = factory.parse(tsdb, mapper, n);
        if (filter == null) {
          throw new IllegalArgumentException("Unable to parse filter config.");
        }
        builder.setQueryFilter(filter);
      }
    }

    n = node.get("pushDownNodes");
    if (n != null) {
      for (final JsonNode pushdown : n) {
        JsonNode temp = pushdown.get("type");
        QueryNodeFactory config_factory = null;
        if (temp != null && !temp.isNull()) {
          config_factory = tsdb.getRegistry()
              .getQueryNodeFactory(temp.asText());
        } else {
          temp = pushdown.get("id");
          if (temp != null && !temp.isNull()) {
            config_factory = tsdb.getRegistry()
                .getQueryNodeFactory(temp.asText());
          }
        }

        if (config_factory == null) {
          throw new IllegalArgumentException("Unable to find a config "
              + "factory for type: " + (temp == null || temp.isNull() ?
              "null" : temp.asText()));
        }
        builder.addPushDownNode(config_factory.parseConfig(
            mapper, tsdb, pushdown));
      }
    }

    n = node.get("rollupIntervals");
    if (n != null && !n.isNull()) {
      for (final JsonNode agg : n) {
        builder.addRollupInterval(agg.asText());
      }
    }

    n = node.get("summaryInterval");
    if (n != null && !n.isNull()) {
      builder.setSummaryInterval(n.asText());
    }

    n = node.get("summaryAggregations");
    if (n != null && !n.isNull()) {
      for (final JsonNode agg : n) {
        builder.addSummaryAggregation(agg.asText());
      }
    }

    n = node.get("timeShiftInterval");
    if (n != null && !n.isNull()) {
      builder.setTimeShiftInterval(n.asText());
    }

    // TODO - millis no good! May need time zone too!
    n = node.get("startOverrideMs");
    if (n != null && !n.isNull()) {
      long ts = n.asLong();
      if (ts > 0) {
        builder.setStartTimeStamp(new MillisecondTimeStamp(ts));
      }
    }

    n = node.get("endOverrideMs");
    if (n != null && !n.isNull()) {
      long ts = n.asLong();
      if (ts > 0) {
        builder.setEndTimeStamp(new MillisecondTimeStamp(ts));
      }
    }

    n = node.get("dataSource");
    if (n != null && !n.isNull()) {
      builder.setDataSource(n.asText());
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public abstract static class Builder<
          B extends Builder<B, C>, C extends BaseQueryNodeConfig & TimeSeriesDataSourceConfig>
      extends BaseQueryNodeConfig.Builder<B, C>
      implements TimeSeriesDataSourceConfig.Builder<B, C> {

    @JsonProperty
    protected String sourceId;
    @JsonProperty
    protected List<String> types;
    @JsonProperty
    protected String namespace;
    @JsonProperty
    protected int from;
    @JsonProperty
    protected int size;
    @JsonProperty
    protected MetricFilter metric;
    @JsonProperty
    protected String filterId;
    @JsonProperty
    protected QueryFilter filter;
    @JsonProperty
    protected boolean fetchLast;
    protected List<QueryNodeConfig> push_down_nodes;
    protected String summary_interval;
    protected List<String> summary_aggregations;
    protected List<String> rollup_intervals;
    @JsonProperty
    protected String interval;
    protected Pair<Boolean, TemporalAmount> amounts;
    protected boolean has_been_setup;
    protected TimeStamp start_override;
    protected TimeStamp end_override;
    protected String dataSource;

    protected Builder() {
      setType(TimeSeriesDataSourceConfig.DEFAULT);
    }

    @Override
    public String id() {
      return id;
    }
    
    @Override
    public String sourceId() {
      return sourceId;
    }

    @Override
    public TimeStamp startOverrideTimeStamp() {
      return start_override;
    }

    @Override
    public TimeStamp endOverrideTimeStamp() {
      return end_override;
    }

    @Override
    public Pair<Boolean, TemporalAmount> timeShifts() {
      return amounts;
    }

    @Override
    public List<QueryNodeConfig> pushDowns() {
      return push_down_nodes;
    }

    @Override
    public List<String> types() {
      return types;
    }

    @Override
    public String dataSource() {
      return dataSource;
    }

    @Override
    public B setSourceId(final String source_id) {
      sourceId = source_id;
      return self();
    }

    @Override
    public B setTypes(final List<String> types) {
      this.types = types;
      return self();
    }

    @Override
    public B addType(final String type) {
      if (types == null) {
        types = Lists.newArrayList();
      }
      types.add(type);
      return self();
    }

    @Override
    public B setNamespace(final String namespace) {
      this.namespace = namespace;
      return self();
    }

    @Override
    public B setFrom(final int from) {
      this.from = from;
      return self();
    }

    @Override
    public B setSize(final int size) {
      this.size = size;
      return self();
    }

    public B setMetric(final MetricFilter metric) {
      this.metric = metric;
      return self();
    }

    @Override
    public B setFilterId(final String filter_id) {
      this.filterId = filter_id;
      return self();
    }

    @Override
    public B setQueryFilter(final QueryFilter filter) {
      this.filter = filter;
      return self();
    }

    @Override
    public B setFetchLast(final boolean fetch_last) {
      this.fetchLast = fetch_last;
      return self();
    }

    @Override
    public B setPushDownNodes(
        final List<QueryNodeConfig> push_down_nodes) {
      this.push_down_nodes = push_down_nodes;
      return self();
    }

    @Override
    public B addPushDownNode(final QueryNodeConfig node) {
      if (push_down_nodes == null) {
        push_down_nodes = Lists.newArrayList();
      }
      push_down_nodes.add(node);
      return self();
    }

    @Override
    public B setSummaryInterval(final String summary_interval) {
      this.summary_interval = summary_interval;
      return self();
    }

    @Override
    public B setSummaryAggregations(final List<String> summary_aggregations) {
      this.summary_aggregations = summary_aggregations;
      return self();
    }

    @Override
    public B addSummaryAggregation(final String summary_aggregation) {
      if (summary_aggregations == null) {
        summary_aggregations = Lists.newArrayList();
      }
      summary_aggregations.add(summary_aggregation);
      return self();
    }

    @Override
    public B setRollupIntervals(final List<String> rollup_intervals) {
      this.rollup_intervals = rollup_intervals;
      return self();
    }

    @Override
    public B addRollupInterval(final String rollup_interval) {
      if (rollup_intervals == null) {
        rollup_intervals = Lists.newArrayList();
      }
      rollup_intervals.add(rollup_interval);
      return self();
    }

    @Override
    public B setTimeShiftInterval(final String interval) {
      if (!Strings.isNullOrEmpty(interval)) {
        this.interval = interval;
        DateTime.parseDuration(interval);

        // TODO - must be easier/cleaner ways.
        // TODO - handle calendaring
        final int count = DateTime.getDurationInterval(interval);
        final String units = DateTime.getDurationUnits(interval);
        final TemporalAmount amount = DateTime.parseDuration2(
                Integer.toString(count) + units);
        amounts = new Pair<Boolean, TemporalAmount>(true, amount);
      }
      return self();
    }

    @Override
    public B setHasBeenSetup(final boolean has_been_setup) {
      this.has_been_setup = has_been_setup;
      return self();
    }

    @Override
    public B setStartTimeStamp(final TimeStamp start) {
      this.start_override = start != null ? start.getCopy()
          : null;
      return self();
    }

    @Override
    public B setEndTimeStamp(final TimeStamp end) {
      this.end_override = end != null ? end.getCopy()
          : null;
      return self();
    }

    @Override
    public B setDataSource(final String dataSource) {
      this.dataSource = dataSource;
      return self();
    }
  }

}