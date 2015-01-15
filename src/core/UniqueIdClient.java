package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Map;

import net.opentsdb.stats.StatsCollector;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class UniqueIdClient {
  private TsdbStore tsdbStore;

  /** Unique IDs for the metric names. */
  final UniqueId metrics;
  /** Unique IDs for the tag names. */
  final UniqueId tag_names;
  /** Unique IDs for the tag values. */
  final UniqueId tag_values;

  /** Name of the table in which UID information is stored. */
  final byte[] uidtable;

  public UniqueIdClient(final TsdbStore tsdbStore, final Config config, final TSDB tsdb) {
    this.tsdbStore = checkNotNull(tsdbStore);

    uidtable = config.getString("tsd.storage.hbase.uid_table").getBytes(Const.CHARSET_ASCII);

    metrics = new UniqueId(tsdbStore, uidtable, UniqueIdType.METRIC);
    tag_names = new UniqueId(tsdbStore, uidtable, UniqueIdType.TAGK);
    tag_values = new UniqueId(tsdbStore, uidtable, UniqueIdType.TAGV);

    if (config.enable_realtime_ts() || config.enable_realtime_uid()) {
      // this is cleaner than another constructor and defaults to null. UIDs
      // will be refactored with DAL code anyways
      metrics.setTSDB(tsdb);
      tag_names.setTSDB(tsdb);
      tag_values.setTSDB(tsdb);
    }

    if (config.getBoolean("tsd.core.preload_uid_cache")) {
      final Bytes.ByteMap<UniqueId> uid_cache_map = new Bytes.ByteMap<UniqueId>();
      uid_cache_map.put(Const.METRICS_QUAL.getBytes(Const.CHARSET_ASCII), metrics);
      uid_cache_map.put(Const.TAG_NAME_QUAL.getBytes(Const.CHARSET_ASCII), tag_names);
      uid_cache_map.put(Const.TAG_VALUE_QUAL.getBytes(Const.CHARSET_ASCII), tag_values);
      UniqueId.preloadUidCache(tsdb, uid_cache_map);
    }
  }

  /**
   * Collects the stats for a {@link net.opentsdb.uid.UniqueId}.
   * @param uid The instance from which to collect stats.
   * @param collector The collector to use.
   */
  static void collectUidStats(final UniqueId uid,
                              final StatsCollector collector) {
    collector.record("uid.cache-hit", uid.cacheHits(), "kind=" + uid.type().qualifier);
    collector.record("uid.cache-miss", uid.cacheMisses(), "kind=" + uid.type().qualifier);
    collector.record("uid.cache-size", uid.cacheSize(), "kind=" + uid.type().qualifier);
  }

  UniqueId uniqueIdInstanceForType(UniqueIdType type) {
    switch (type) {
      case METRIC:
        return metrics;
      case TAGK:
        return tag_names;
      case TAGV:
        return tag_values;
      default:
        throw new IllegalArgumentException(type + " is unknown");
    }
  }

  /**
   * Discards all in-memory caches.
   * @since 1.1
   */
  public void dropCaches() {
    metrics.dropCaches();
    tag_names.dropCaches();
    tag_values.dropCaches();
  }

  /**
   * Attempts to assign a UID to a name for the given type
   * Used by the UniqueIdRpc call to generate IDs for new metrics, tagks or
   * tagvs. The name must pass validation and if it's already assigned a UID,
   * this method will throw an error with the proper UID. Otherwise if it can
   * create the UID, it will be returned
   * @param type The type of uid to assign, metric, tagk or tagv
   * @param name The name of the uid object
   * @return A byte array with the UID if the assignment was successful
   * @throws IllegalArgumentException if the name is invalid or it already
   * exists
   * @since 2.0
   */
  public byte[] assignUid(final UniqueIdType type, final String name) {
    Tags.validateString(type.toString(), name);
    UniqueId instance = uniqueIdInstanceForType(type);

    try {
      try {
        final byte[] uid = instance.getIdAsync(name).joinUninterruptibly();
        throw new IllegalArgumentException("Name already exists with UID: " +
                UniqueId.uidToString(uid));
      } catch (NoSuchUniqueName nsue) {
        return instance.createId(name).joinUninterruptibly();
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Attempts to find the name for a unique identifier given a type
   * @param type The type of UID
   * @param uid The UID to search for
   * @return The name of the UID object if found
   * @throws IllegalArgumentException if the type is not valid
   * @throws net.opentsdb.uid.NoSuchUniqueId if the UID was not found
   * @since 2.0
   */
  public Deferred<String> getUidName(final UniqueIdType type, final byte[] uid) {
    checkArgument(uid != null, "Missing UID");

    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.getNameAsync(uid);
  }

  /**
   * Attempts to find the UID matching a given name
   * @param type The type of UID
   * @param name The name to search for
   * @throws IllegalArgumentException if the type is not valid
   * @since 2.0
   */
  public Deferred<byte[]> getUID(final UniqueIdType type, final String name) {
    checkArgument(!Strings.isNullOrEmpty(name), "Missing UID name");
    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.getIdAsync(name);
  }

  /** Number of cache hits during lookups involving UIDs. */
  public int uidCacheHits() {
    return (metrics.cacheHits() + tag_names.cacheHits()
            + tag_values.cacheHits());
  }

  /** Number of cache misses during lookups involving UIDs. */
  public int uidCacheMisses() {
    return (metrics.cacheMisses() + tag_names.cacheMisses()
            + tag_values.cacheMisses());
  }

  /** Number of cache entries currently in RAM for lookups involving UIDs. */
  public int uidCacheSize() {
    return (metrics.cacheSize() + tag_names.cacheSize()
            + tag_values.cacheSize());
  }

  /**
   * Returns a initialized TSUID for this metric and these tags.
   * @since 2.0
   * @param metric The metric to use in the TSUID
   * @param tags The string tags to use in the TSUID
   * @param tsdb TSDB instance used with Tags class
   */
  Deferred<byte[]> getTSUID(final String metric,
                            final Map<String, String> tags,
                            final TSDB tsdb) {
    final short metric_width = metrics.width();
    final short tag_name_width = tag_names.width();
    final short tag_value_width = tag_values.width();
    final short num_tags = (short) tags.size();

    int row_size = (metric_width
                    + tag_name_width * num_tags
                    + tag_value_width * num_tags);
    final byte[] row = new byte[row_size];

    final boolean auto_create_metrics =
            tsdb.config.getBoolean("tsd.core.auto_create_metrics");

    // Lookup or create the metric ID.
    final Deferred<byte[]> metric_id = metrics.getIdAsync(metric);

    // Copy the metric ID at the beginning of the row key.
    class CopyMetricInRowKeyCB implements Callback<byte[], byte[]> {
      public byte[] call(final byte[] metricid) {
        copyInRowKey(row, (short) 0, metricid);
        return row;
      }
    }

    class HandleNoSuchUniqueNameCB implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        if (e instanceof NoSuchUniqueName && auto_create_metrics) {
          return metrics.createId(metric);
        }

        return e; // Other unexpected exception, let it bubble up.
      }
    }

    // Copy the tag IDs in the row key.
    class CopyTagsInRowKeyCB
      implements Callback<Deferred<byte[]>, ArrayList<byte[]>> {
      public Deferred<byte[]> call(final ArrayList<byte[]> tags) {
        short pos = metric_width;
        for (final byte[] tag : tags) {
          copyInRowKey(row, pos, tag);
          pos += tag.length;
        }
        // Once we've resolved all the tags, schedule the copy of the metric
        // ID and return the row key we produced.
        return metric_id
                .addErrback(new HandleNoSuchUniqueNameCB())
                .addCallback(new CopyMetricInRowKeyCB());
      }
    }

    // Kick off the resolution of all tags.
    return Tags.resolveOrCreateAllAsync(tsdb, tags)
      .addCallbackDeferring(new CopyTagsInRowKeyCB());
  }

  /**
   * Copies the specified byte array at the specified offset in the row key.
   * @param row The row key into which to copy the bytes.
   * @param offset The offset in the row key to start writing at.
   * @param bytes The bytes to copy.
   */
  private void copyInRowKey(final byte[] row, final short offset, final byte[] bytes) {
    System.arraycopy(bytes, 0, row, offset, bytes.length);
  }

  void collectStats(final StatsCollector collector, final TSDB tsdb) {
    final byte[][] kinds = {
            Const.METRICS_QUAL.getBytes(Const.CHARSET_ASCII),
            Const.TAG_NAME_QUAL.getBytes(Const.CHARSET_ASCII),
            Const.TAG_VALUE_QUAL.getBytes(Const.CHARSET_ASCII)
    };
    try {
      final Map<String, Long> used_uids = UniqueId.getUsedUIDs(tsdb, kinds)
              .joinUninterruptibly();

      collectUidStats(metrics, collector);
      collector.record("uid.ids-used", used_uids.get(Const.METRICS_QUAL),
              "kind=" + Const.METRICS_QUAL);
      collector.record("uid.ids-available",
              (metrics.maxPossibleId() - used_uids.get(Const.METRICS_QUAL)),
              "kind=" + Const.METRICS_QUAL);

      collectUidStats(tag_names, collector);
      collector.record("uid.ids-used", used_uids.get(Const.TAG_NAME_QUAL),
              "kind=" + Const.TAG_NAME_QUAL);
      collector.record("uid.ids-available",
              (tag_names.maxPossibleId() - used_uids.get(Const.TAG_NAME_QUAL)),
              "kind=" + Const.TAG_NAME_QUAL);

      collectUidStats(tag_values, collector);
      collector.record("uid.ids-used", used_uids.get(Const.TAG_VALUE_QUAL),
              "kind=" + Const.TAG_VALUE_QUAL);
      collector.record("uid.ids-available",
              (tag_values.maxPossibleId() - used_uids.get(Const.TAG_VALUE_QUAL)),
              "kind=" + Const.TAG_VALUE_QUAL);
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    }
  }
}
