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

import com.codahale.metrics.MetricSet;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.hbase.HBaseStore;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.core.StringCoder.toBytes;

/**
 * Thread-safe implementation of the TSDB client.
 * <p>
 * This class is the central class of OpenTSDB.  You use it to add new data
 * points or query the database.
 */
public class TSDB {
  private static final Logger LOG = LoggerFactory.getLogger(TSDB.class);
  
  static final byte[] FAMILY = { 't' };

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
  @Inject
  public TSDB(final TsdbStore client,
              final Config config,
              final SearchPlugin searchPlugin,
              final RTPublisher realTimePublisher,
              final Metrics metrics,
              final UniqueIdClient uniqueIdClient,
              final TreeClient treeClient,
              final MetaClient metaClient,
              final DataPointsClient dataPointsClient) {
    this.config = checkNotNull(config);
    this.tsdb_store = checkNotNull(client);
    this.metrics = checkNotNull(metrics);

    table = toBytes(config.getString("tsd.storage.hbase.data_table"));
    uidtable = toBytes(config.getString("tsd.storage.hbase.uid_table"));
    treetable = toBytes(config.getString("tsd.storage.hbase.tree_table"));
    meta_table = toBytes(config.getString("tsd.storage.hbase.meta_table"));

    if (config.hasPath("tsd.core.timezone")) {
      DateTime.setDefaultTimezone(config.getString("tsd.core.timezone"));
    }

    this.search = checkNotNull(searchPlugin);
    this.rt_publisher = checkNotNull(realTimePublisher);

    this.uniqueIdClient = checkNotNull(uniqueIdClient);
    this.treeClient = checkNotNull(treeClient);
    this.metaClient = checkNotNull(metaClient);
    this.dataPointsClient = checkNotNull(dataPointsClient);

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
      @Override
      public Object call(final ArrayList<Object> args) {
        return tsdb_store.shutdown();
      }
      public String toString() {
        return "shutdown TsdbStore";
      }
    }
    
    final class ShutdownErrback implements Callback<Object, Exception> {
      @Override
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

}
