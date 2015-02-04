// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import com.google.common.eventbus.EventBus;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.hbase.HBaseStore;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.Bytes;

import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.tree.TreeBuilder;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Thread-safe implementation of the TSDB client.
 * <p>
 * This class is the central class of OpenTSDB.  You use it to add new data
 * points or query the database.
 */
public class TSDB {
  private static final Logger LOG = LoggerFactory.getLogger(TSDB.class);
  
  static final byte[] FAMILY = { 't' };

  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");

  /** TsdbStore, the database cluster to use for storage.  */
  final TsdbStore tsdb_store;

  /** Name of the table in which timeseries are stored.  */
  final byte[] table;
  /** Name of the table in which UID information is stored. */
  final byte[] uidtable;
  /** Name of the table where tree data is stored. */
  final byte[] treetable;
  /** Name of the table where meta data is stored. */
  final byte[] meta_table;

  /** Configuration object for all TSDB components */
  final Config config;

  /**
   * Metrics instance used by all TSDB related objects
   */
  private final Metrics metrics;

  private final UniqueIdClient uniqueIdClient;
  private final MetaClient metaClient;
  private final TreeClient treeClient;

  /**
   * The search plugin that this TSDB instance is configured to use.
   */
  private final SearchPlugin search;

  /**
   * The realtime publisher that this TSDB instance is configured to use.
   */
  private final RTPublisher rt_publisher;

  /**
   * Constructor
   * @param client An initialized TsdbStore object
   * @param config An initialized configuration object
   * @param searchPlugin The search plugin to use
   * @param realTimePublisher The realtime publisher to use
   * @param metrics Metrics instance used by all TSDB related objects
   * @since 2.1
   */
  public TSDB(final TsdbStore client,
              final Config config,
              final SearchPlugin searchPlugin,
              final RTPublisher realTimePublisher,
              final Metrics metrics) {
    this.config = checkNotNull(config);
    this.tsdb_store = checkNotNull(client);
    this.metrics = checkNotNull(metrics);

    table = config.getString("tsd.storage.hbase.data_table").getBytes(CHARSET);
    uidtable = config.getString("tsd.storage.hbase.uid_table").getBytes(CHARSET);
    treetable = config.getString("tsd.storage.hbase.tree_table").getBytes(CHARSET);
    meta_table = config.getString("tsd.storage.hbase.meta_table").getBytes(CHARSET);

    if (config.hasProperty("tsd.core.timezone")) {
      DateTime.setDefaultTimezone(config.getString("tsd.core.timezone"));
    }

    this.search = checkNotNull(searchPlugin);
    this.rt_publisher = checkNotNull(realTimePublisher);

    EventBus idEventBus = new EventBus();
    uniqueIdClient = new UniqueIdClient(tsdb_store, config, metrics, idEventBus);
    metaClient = new MetaClient(tsdb_store, idEventBus, searchPlugin, config, uniqueIdClient);
    treeClient = new TreeClient(tsdb_store);

    LOG.debug(config.dumpConfiguration());
  }
  
  /** @return The data point column family name */
  public static byte[] FAMILY() {
    return FAMILY;
  }

  public MetaClient getMetaClient() {
    return metaClient;
  }

  public UniqueIdClient getUniqueIdClient() {
    return uniqueIdClient;
  }

  public TreeClient getTreeClient() {
    return treeClient;
  }

  /**
   * Should be called immediately after construction to initialize plugins and
   * objects that rely on such. It also moves most of the potential exception
   * throwing code out of the constructor so TSDMain can shutdown clients and
   * such properly.
   * @throws RuntimeException if the plugin path could not be processed
   * @throws IllegalArgumentException if a plugin could not be initialized
   * @since 2.0
   */
  public void initializePlugins() {
    try {
      search.initialize(this);
      LOG.info("Successfully initialized search plugin [{}] version: {}",
              search.getClass().getCanonicalName(), search.version());
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize search plugin", e);
    }

    try {
      rt_publisher.initialize(this);
      LOG.info("Successfully initialized real time publisher plugin [{}] version: {}",
              rt_publisher.getClass().getCanonicalName(), rt_publisher.version());
    } catch (Exception e) {
      throw new RuntimeException(
              "Failed to initialize real time publisher plugin", e);
    }
  }
  
  /** 
   * Returns the configured TsdbStore
   * @return The TsdbStore
   * @since 2.0 
   */
  public final TsdbStore getTsdbStore() {
    return this.tsdb_store;
  }

  /**
   * Returns the configured HBaseStore.
   * It will throw a classCastException if the TsdbStore was of the type
   * CassandraStore. This should only be used by tools and will be migrated
   * and removed later.
   *
   * @return The HBaseStore
   * @since 2.0
   */
  @Deprecated
  public final HBaseStore getHBaseStore() {
    return (HBaseStore) this.tsdb_store;
  }
  
  /** 
   * Getter that returns the configuration object
   * @return The configuration object
   * @since 2.0 
   */
  public final Config getConfig() {
    return this.config;
  }

  public MetricSet getMetrics() {
    return metrics.getRegistry();
  }

  /**
   * Verifies that the data and UID tables exist in TsdbStore and optionally the
   * tree and meta data tables if the user has enabled meta tracking or tree
   * building
   * @return An ArrayList of objects to wait for
   * @since 2.0
   */
  public Deferred<ArrayList<Object>> checkNecessaryTablesExist() {
    return tsdb_store.checkNecessaryTablesExist();
  }

  /**
   * Returns a new {@link WritableDataPoints} instance suitable for this TSDB.
   * <p>
   * If you want to add a single data-point, consider using {@link #addPoint}
   * instead.
   */
  public WritableDataPoints newDataPoints() {
    return new IncomingDataPoints(this);
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
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final long value,
                                   final Map<String, String> tags) {
    final byte[] v;
    if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
      v = new byte[] { (byte) value };
    } else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
      v = Bytes.fromShort((short) value);
    } else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
      v = Bytes.fromInt((int) value);
    } else {
      v = Bytes.fromLong(value);
    }
    final short flags = (short) (v.length - 1);  // Just the length.
    return addPointInternal(metric, timestamp, v, tags, flags);
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
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final double value,
                                   final Map<String, String> tags) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
                                         + " for metric=" + metric
                                         + " timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x7;  // A float stored on 8 bytes.
    return addPointInternal(metric, timestamp,
                            Bytes.fromLong(Double.doubleToRawLongBits(value)),
                            tags, flags);
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
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final float value,
                                   final Map<String, String> tags) {
    if (Float.isNaN(value) || Float.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
                                         + " for metric=" + metric
                                         + " timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
    return addPointInternal(metric, timestamp,
            Bytes.fromInt(Float.floatToRawIntBits(value)),
            tags, flags);
  }

  private Deferred<Object> addPointInternal(final String metric,
                                            final long timestamp,
                                            final byte[] value,
                                            final Map<String, String> tags,
                                            final short flags) {
    checkTimestamp(timestamp);
    IncomingDataPoints.checkMetricAndTags(metric, tags);


    class RowKeyCB implements Callback<Deferred<Object>, byte[]> {
      @Override
      public Deferred<Object> call(final byte[] tsuid) throws Exception {

        // TODO(tsuna): Add a callback to time the latency of HBase and store the
        // timing in a moving Histogram (once we have a class for this).
        Deferred<Object> result = tsdb_store.addPoint(tsuid, value, timestamp, flags);

        // for busy TSDs we may only enable TSUID tracking, storing a 1 in the
        // counter field for a TSUID with the proper timestamp. If the user would
        // rather have TSUID incrementing enabled, that will trump the PUT
        if (config.enable_tsuid_tracking() && !config.enable_tsuid_incrementing()) {
          tsdb_store.setTSMetaCounter(tsuid, 1);
        } else if (config.enable_tsuid_incrementing() || config.enable_realtime_ts()) {
          incrementAndGetCounter(tsuid);
        }

        rt_publisher.sinkDataPoint(metric, timestamp, value, tags, tsuid, flags);

        return result;
      }
    }

    return this.uniqueIdClient.getTSUID(metric, tags, this)
            .addCallbackDeferring(new RowKeyCB());

  }

  /**
   * Validates that the timestamp is within valid bounds.
   * @throws java.lang.IllegalArgumentException if the timestamp isn't within
   * bounds.
   */
  static long checkTimestamp(long timestamp) {
    checkArgument(timestamp >= 0, "The timestamp must be positive but was %s", timestamp);
    checkArgument((timestamp & Const.SECOND_MASK) == 0 || timestamp <= Const.MAX_MS_TIMESTAMP,
            "The timestamp was too large (%s)", timestamp);

    return timestamp;
  }

  /**
   * Forces a flush of any un-committed in memory data including left over 
   * compactions.
   * <p>
   * For instance, any data point not persisted will be sent to the TsdbStore.
   * @return A {@link Deferred} that will be called once all the un-committed
   * data has been successfully and durably stored.  The value of the deferred
   * object return is meaningless and unspecified, and can be {@code null}.
   */
  public Deferred<Object> flush() {
    return tsdb_store.flush();
  }

  /**
   * Gracefully shuts down this TSD instance.
   * <p>
   * The method must call {@code shutdown()} on all plugins as well as flush the
   * compaction queue.
   * @return A {@link Deferred} that will be called once all the un-committed
   * data has been successfully and durably stored, and all resources used by
   * this instance have been released.  The value of the deferred object
   * return is meaningless and unspecified, and can be {@code null}.
   */
  public Deferred<Object> shutdown() {
    final ArrayList<Deferred<Object>> deferreds = 
      new ArrayList<Deferred<Object>>();
    
    final class StoreShutdown implements Callback<Object, ArrayList<Object>> {
      public Object call(final ArrayList<Object> args) {
        return tsdb_store.shutdown();
      }
      public String toString() {
        return "shutdown TsdbStore";
      }
    }
    
    final class ShutdownErrback implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        final Logger LOG = LoggerFactory.getLogger(ShutdownErrback.class);
        if (e instanceof DeferredGroupException) {
          final DeferredGroupException ge = (DeferredGroupException) e;
          for (final Object r : ge.results()) {
            if (r instanceof Exception) {
              LOG.error("Failed to shutdown the TSD", (Exception) r);
            }
          }
        } else {
          LOG.error("Failed to shutdown the TSD", e);
        }
        return tsdb_store.shutdown();
      }
      public String toString() {
        return "shutdown TsdbStore after error";
      }
    }

    LOG.info("Shutting down search plugin: {}", search.getClass().getCanonicalName());
    deferreds.add(search.shutdown());

    LOG.info("Shutting down RT plugin: {}", rt_publisher.getClass().getCanonicalName());
    deferreds.add(rt_publisher.shutdown());

    // wait for plugins to shutdown before we close the TsdbStore
    return Deferred.group(deferreds)
            .addCallbacks(new StoreShutdown(), new ShutdownErrback());
  }

  /**
   * Given a prefix search, returns matching names from the specified id
   * type.
   * @param type The type of ids to search
   * @param search A prefix to search.
   * @since 2.0
   */
  public Deferred<List<String>> suggest(final UniqueIdType type,
                                        final String search) {
    UniqueId uniqueId = uniqueIdClient.uniqueIdInstanceForType(type);
    return uniqueId.suggest(search);
  }

  /**
   * Given a prefix search, returns matching names from the specified id
   * type.
   * @param type The type of ids to search
   * @param search A prefix to search.
   * @param max_results Maximum number of results to return.
   * @since 2.0
   */
  public Deferred<List<String>> suggest(final UniqueIdType type,
                                        final String search,
                                        final int max_results) {
    UniqueId uniqueId = uniqueIdClient.uniqueIdInstanceForType(type);
    return uniqueId.suggest(search, max_results);
  }

  /** @return the name of the UID table as a byte array for TsdbStore requests */
  public byte[] uidTable() {
    return this.uidtable;
  }
  
  /** @return the name of the data table as a byte array for TsdbStore requests */
  public byte[] dataTable() {
    return this.table;
  }
  
  /** @return the name of the tree table as a byte array for TsdbStore requests */
  public byte[] treeTable() {
    return this.treetable;
  }
  
  /** @return the name of the meta table as a byte array for TsdbStore requests */
  public byte[] metaTable() {
    return this.meta_table;
  }

  /**
   * Index the given timeseries meta object via the configured search plugin
   * @param meta The meta data object to index
   * @since 2.0
   */
  public void indexTSMeta(final TSMeta meta) {
    search.indexTSMeta(meta).addErrback(new PluginError());
  }
  
  /**
   * Delete the timeseries meta object from the search index
   * @param tsuid The TSUID to delete
   * @since 2.0
   */
  public void deleteTSMeta(final String tsuid) {
    search.deleteTSMeta(tsuid).addErrback(new PluginError());
  }
  
  /**
   * Index the given UID meta object via the configured search plugin
   * @param meta The meta data object to index
   * @since 2.0
   */
  public void indexUIDMeta(final UIDMeta meta) {
    search.indexUIDMeta(meta).addErrback(new PluginError());
  }
  
  /**
   * Delete the UID meta object from the search index
   * @param meta The UID meta object to delete
   * @since 2.0
   */
  public void deleteUIDMeta(final UIDMeta meta) {
    search.deleteUIDMeta(meta).addErrback(new PluginError());
  }
  
  /**
   * Index the given Annotation object via the configured search plugin
   * @param note The annotation object to index
   * @since 2.0
   */
  public void indexAnnotation(final Annotation note) {
    search.indexAnnotation(note).addErrback(new PluginError());
    rt_publisher.publishAnnotation(note);
  }
  
  /**
   * Delete the annotation object from the search index
   * @param note The annotation object to delete
   * @since 2.0
   */
  public void deleteAnnotation(final Annotation note) {
    search.deleteAnnotation(note).addErrback(new PluginError());
  }
  
  /**
   * Processes the TSMeta through all of the trees if configured to do so
   * @param meta The meta data to process
   * @since 2.0
   */
  public Deferred<Boolean> processTSMetaThroughTrees(final TSMeta meta) {
    if (config.enable_tree_processing()) {
      return TreeBuilder.processAllTrees(this, meta);
    }
    return Deferred.fromResult(false);
  }
  
  /**
   * Executes a search query using the search plugin
   * @param query The query to execute
   * @return A deferred object to wait on for the results to be fetched
   * @throws IllegalStateException if the search plugin has not been enabled or
   * configured
   * @since 2.0
   */
  public Deferred<SearchQuery> executeSearch(final SearchQuery query) {
    return search.executeQuery(query);
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
    return tsdb_store.executeQuery(query)
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
      final ByteMap<SpanGroup> groups = new ByteMap<SpanGroup>();
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

  /**
   * Simply logs plugin errors when they're thrown by attaching as an errorback. 
   * Without this, exceptions will just disappear (unless logged by the plugin) 
   * since we don't wait for a result.
   */
  final class PluginError implements Callback<Object, Exception> {
    @Override
    public Object call(final Exception e) throws Exception {
      LOG.error("Exception from Search plugin indexer", e);
      return null;
    }
  }

  /**
   * Attempts a CompareAndSet storage call, loading the object from storage,
   * synchronizing changes, and attempting a put. Also verifies that associated
   * UID name mappings exist before merging.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * or there weren't any changes, then the data will not be written and an
   * exception will be thrown.
   * <b>Note:</b> We do not store the UIDMeta information with TSMeta's since
   * users may change a single UIDMeta object and we don't want to update every
   * TSUID that includes that object with the new data. Instead, UIDMetas are
   * merged into the TSMeta on retrieval so we always have canonical data. This
   * also saves space in storage.
   * @param tsMeta The TSMeta to stored
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * @return True if the storage call was successful, false if the object was
   * modified in storage during the CAS call. If false, retry the call. Other
   * failures will result in an exception being thrown.
   * @throws IllegalArgumentException if parsing failed
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<Boolean> syncToStorage(final TSMeta tsMeta,
                                         final boolean overwrite) {
    tsMeta.checkTSUI();

    if (!tsMeta.hasChanges()) {
      LOG.debug("{} does not have changes, skipping sync to storage", this);
      throw new IllegalStateException("No changes detected in TSUID meta data");
    }

    /**
     * Callback used to verify that the UID name mappings exist. We don't need
     * to process the actual name, we just want it to throw an error if any
     * of the UIDs don't exist.
     */
    class UidCB implements Callback<Object, String> {

      @Override
      public Object call(String name) throws Exception {
        // nothing to do as missing mappings will throw a NoSuchUniqueId
        return null;
      }

    }

    // parse out the tags from the tsuid
    final List<byte[]> parsed_tags = UniqueId.getTagsFromTSUID(tsMeta.getTSUID());

    // Deferred group used to accumulate UidCB callbacks so the next call
    // can wait until all of the UIDs have been verified
    ArrayList<Deferred<Object>> uid_group =
            new ArrayList<Deferred<Object>>(parsed_tags.size() + 1);

    // calculate the metric UID and fetch it's name mapping
    final byte[] metric_uid = UniqueId.stringToUid(
            tsMeta.getTSUID().substring(0, Const.METRICS_WIDTH * 2));
    uid_group.add(uniqueIdClient.getUidName(UniqueIdType.METRIC, metric_uid)
            .addCallback(new UidCB()));

    int idx = 0;
    for (byte[] tag : parsed_tags) {
      if (idx % 2 == 0) {
        uid_group.add(uniqueIdClient.getUidName(UniqueIdType.TAGK, tag)
                .addCallback(new UidCB()));
      } else {
        uid_group.add(uniqueIdClient.getUidName(UniqueIdType.TAGV, tag)
                .addCallback(new UidCB()));
      }
      idx++;
    }

    return tsdb_store.syncToStorage(tsMeta, Deferred.group(uid_group), overwrite);
  }

  /**
   * Increments the tsuid datapoint counter or creates a new counter. Also
   * creates a new meta data entry if the counter did not exist.
   * <b>Note:</b> This method also:
   * <ul><li>Passes the new TSMeta object to the Search plugin after loading
   * UIDMeta objects</li>
   * <li>Passes the new TSMeta through all configured trees if enabled</li></ul>
   * @param tsuid The TSUID to increment or create
   * @return 0 if the put failed, a positive LONG if the put was successful
   * @throws org.hbase.async.HBaseException if there was a storage issue
   * @throws net.opentsdb.utils.JSONException if the data was corrupted
   */
  public Deferred<Long> incrementAndGetCounter(final byte[] tsuid) {

    /**
     * Callback that will create a new TSMeta if the increment result is 1 or
     * will simply return the new value.
     */
    final class TSMetaCB implements Callback<Deferred<Long>, Long> {

      /**
       * Called after incrementing the counter and will create a new TSMeta if
       * the returned value was 1 as well as pass the new meta through trees
       * and the search indexer if configured.
       * @return 0 if the put failed, a positive LONG if the put was successful
       */
      @Override
      public Deferred<Long> call(final Long incremented_value)
              throws Exception {

        if (incremented_value > 1) {
          // TODO - maybe update the search index every X number of increments?
          // Otherwise the search engine would only get last_updated/count
          // whenever the user runs the full sync CLI
          return Deferred.fromResult(incremented_value);
        }

        // create a new meta object with the current system timestamp. Ideally
        // we would want the data point's timestamp, but that's much more data
        // to keep track of and may not be accurate.
        final TSMeta meta = new TSMeta(tsuid,
                System.currentTimeMillis() / 1000);

        /**
         * Called after the meta has been passed through tree processing. The
         * result of the processing doesn't matter and the user may not even
         * have it enabled, so we'll just return the counter.
         */
        final class TreeCB implements Callback<Deferred<Long>, Boolean> {

          @Override
          public Deferred<Long> call(Boolean success) throws Exception {
            return Deferred.fromResult(incremented_value);
          }

        }

        /**
         * Called after retrieving the newly stored TSMeta and loading
         * associated UIDMeta objects. This class will also pass the meta to the
         * search plugin and run it through any configured trees
         */
        final class FetchNewCB implements Callback<Deferred<Long>, TSMeta> {

          @Override
          public Deferred<Long> call(TSMeta stored_meta) throws Exception {

            // pass to the search plugin
            indexTSMeta(stored_meta);

            // pass through the trees
            return processTSMetaThroughTrees(stored_meta)
                    .addCallbackDeferring(new TreeCB());
          }

        }

        /**
         * Called after the CAS to store the new TSMeta object. If the CAS
         * failed then we return immediately with a 0 for the counter value.
         * Otherwise we keep processing to load the meta and pass it on.
         */
        final class StoreNewCB implements Callback<Deferred<Long>, Boolean> {

          @Override
          public Deferred<Long> call(Boolean success) throws Exception {
            if (!success) {
              LOG.warn("Unable to save metadata: {}", meta);
              return Deferred.fromResult(0L);
            }

            LOG.info("Successfullly created new TSUID entry for: {}", meta);
            final Deferred<TSMeta> meta = tsdb_store.getTSMeta(tsuid)
                    .addCallbackDeferring(
                            new LoadUIDs(UniqueId.uidToString(tsuid)));
            return meta.addCallbackDeferring(new FetchNewCB());
          }

        }

        // store the new TSMeta object and setup the callback chain
        return metaClient.create(meta).addCallbackDeferring(new StoreNewCB());
      }

    }

    Deferred<Long> res = tsdb_store.incrementAndGetCounter(tsuid);
    if (!config.enable_realtime_ts())
      return res;
    return res.addCallbackDeferring(
            new TSMetaCB());
  }



  /**
   * Attempts to fetch the timeseries meta data from storage.
   * This method will fetch the {@code counter} and {@code meta} columns.
   * If load_uids is false this method will not load the UIDMeta objects.
   * <b>Note:</b> Until we have a caching layer implemented, this will make at
   * least 4 reads to the storage system, 1 for the TSUID meta, 1 for the
   * metric UIDMeta and 1 each for every tagk/tagv UIDMeta object.
   * <p>
   * @param tsuid The UID of the meta to fetch
   * @param load_uids Set this you true if you also want to load the UIDs
   * @return A TSMeta object if found, null if not
   * @throws org.hbase.async.HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws net.opentsdb.utils.JSONException if the data was corrupted
   */
  public Deferred<TSMeta> getTSMeta(final String tsuid, final boolean load_uids) {
    if (load_uids)
    return tsdb_store.getTSMeta(UniqueId.stringToUid(tsuid))
            .addCallbackDeferring(new LoadUIDs( tsuid));

    return  tsdb_store.getTSMeta(UniqueId.stringToUid(tsuid));

  }

  /**
   * Parses a TSMeta object from the given column, optionally loading the
   * UIDMeta objects
   *
   * @param column_key The KeyValue.key() of the column to parse also know as uid
   * @param column_value The KeyValue.value() of the column to parse
   * @param load_uidmetas Whether or not UIDmeta objects should be loaded
   * @return A TSMeta if parsed successfully
   * @throws net.opentsdb.utils.JSONException if the data was corrupted
   */
  public Deferred<TSMeta> parseFromColumn(final byte[] column_key,
                                          final byte[] column_value,
                                          final boolean load_uidmetas) {
    if (column_value == null || column_value.length < 1) {
      throw new IllegalArgumentException("Empty column value");
    }

    final TSMeta meta = JSON.parseToObject(column_value, TSMeta.class);

    // fix in case the tsuid is missing
    if (meta.getTSUID() == null || meta.getTSUID().isEmpty()) {
      meta.setTSUID(UniqueId.uidToString(column_key));
    }

    if (!load_uidmetas) {
      return Deferred.fromResult(meta);
    }

    final LoadUIDs deferred = new LoadUIDs(meta.getTSUID());
    try {
      return deferred.call(meta);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Asynchronously loads the UIDMeta objects into the given TSMeta object. Used
   * by multiple methods so it's broken into it's own class here.
   */
  private class LoadUIDs implements Callback<Deferred<TSMeta>, TSMeta> {

    private final byte[] metric_uid;
    private final List<byte[]> tags;

    public LoadUIDs(final String tsuid) {

      final byte[] byte_tsuid = UniqueId.stringToUid(tsuid);

      metric_uid = Arrays.copyOfRange(byte_tsuid, 0, Const.METRICS_WIDTH);
      tags = UniqueId.getTagsFromTSUID(tsuid);
    }

    /**
     * @return A TSMeta object loaded with UIDMetas if successful
     * @throws org.hbase.async.HBaseException if there was a storage issue
     * @throws net.opentsdb.utils.JSONException if the data was corrupted
     */
    @Override
    public Deferred<TSMeta> call(final TSMeta meta) throws Exception {
      if (meta == null) {
        return Deferred.fromResult(null);
      }

      final ArrayList<Deferred<UIDMeta>> uid_group =
              new ArrayList<Deferred<UIDMeta>>(tags.size());

      Iterator<byte[]> tag_iter = tags.iterator();

      while(tag_iter.hasNext()) {
        uid_group.add(metaClient.getUIDMeta(UniqueIdType.TAGK, tag_iter.next()));
        uid_group.add(metaClient.getUIDMeta(UniqueIdType.TAGV, tag_iter.next()));
      }

      /**
       * A callback that will place the loaded UIDMeta objects for the tags in
       * order on meta.tags.
       */
      final class UIDMetaTagsCB implements Callback<TSMeta, ArrayList<UIDMeta>> {
        @Override
        public TSMeta call(final ArrayList<UIDMeta> uid_metas) {
          meta.setTags(uid_metas);
          return meta;
        }
      }

      /**
       * A callback that will place the loaded UIDMeta object for the metric
       * UID on meta.metric.
       */
      class UIDMetaMetricCB implements Callback<Deferred<TSMeta>, UIDMeta> {
        @Override
        public Deferred<TSMeta> call(UIDMeta uid_meta) {
          meta.setMetric(uid_meta);

          // This will chain the UIDMetaTagsCB on this callback which is what
          // allows us to just return the result of the callback chain bellow
          // . groupInOrder is used so that the resulting list will be in the
          // same order as they were added to uid_group.
          return Deferred.groupInOrder(uid_group)
                  .addCallback(new UIDMetaTagsCB());
        }
      }

      return metaClient.getUIDMeta(UniqueIdType.METRIC, metric_uid)
              .addCallbackDeferring(new UIDMetaMetricCB());
    }
  }

}
