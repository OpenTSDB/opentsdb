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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes.ByteMap;

import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;

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
  private final TsdbStore tsdb_store;

  /** Name of the table in which timeseries are stored.  */
  final byte[] table;
  /** Name of the table in which UID information is stored. */
  final byte[] uidtable;
  /** Name of the table where tree data is stored. */
  final byte[] treetable;
  /** Name of the table where meta data is stored. */
  final byte[] meta_table;

  /** Configuration object for all TSDB components */
  private final Config config;

  /**
   * Metrics instance used by all TSDB related objects
   */
  private final Metrics metrics;

  private final UniqueIdClient uniqueIdClient;
  private final DataPointsClient dataPointsClient;
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
    treeClient = new TreeClient(tsdb_store);
    metaClient = new MetaClient(tsdb_store, idEventBus, searchPlugin, config, uniqueIdClient, treeClient, rt_publisher);
    dataPointsClient = new DataPointsClient(tsdb_store, config, uniqueIdClient, metaClient, rt_publisher);

    LOG.debug(config.dumpConfiguration());
  }
  
  /** @return The data point column family name */
  public static byte[] FAMILY() {
    return FAMILY;
  }

  public MetaClient getMetaClient() {
    return metaClient;
  }

  public DataPointsClient getDataPointsClient() {
    return dataPointsClient;
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
  public static final class PluginError implements Callback<Object, Exception> {
    @Override
    public Object call(final Exception e) throws Exception {
      LOG.error("Exception from Search plugin indexer", e);
      return null;
    }
  }
}
