package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.NoSuchUniqueId;
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
  private static final SortResolvedTagsCB SORT_CB = new SortResolvedTagsCB();

  private final Config config;
  private final TsdbStore tsdbStore;

  /** Unique IDs for the metric names. */
  final UniqueId metrics;
  /** Unique IDs for the tag names. */
  final UniqueId tag_names;
  /** Unique IDs for the tag values. */
  final UniqueId tag_values;

  /** Name of the table in which UID information is stored. */
  final byte[] uidtable;

  public UniqueIdClient(final TsdbStore tsdbStore,
                        final Config config,
                        final TSDB tsdb,
                        final Metrics metricsRegistry) {
    this.tsdbStore = checkNotNull(tsdbStore);
    this.config = checkNotNull(config);

    uidtable = config.getString("tsd.storage.hbase.uid_table").getBytes(Const.CHARSET_ASCII);

    metrics = new UniqueId(tsdbStore, uidtable, UniqueIdType.METRIC, metricsRegistry);
    tag_names = new UniqueId(tsdbStore, uidtable, UniqueIdType.TAGK, metricsRegistry);
    tag_values = new UniqueId(tsdbStore, uidtable, UniqueIdType.TAGV, metricsRegistry);

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
   * Ensures that a given string is a valid metric name or tag name/value.
   * @param what A human readable description of what's being validated.
   * @param s The string to validate.
   * @throws IllegalArgumentException if the string isn't valid.
   */
  public static void validateUidName(final String what, final String s) {
    if (s == null) {
      throw new IllegalArgumentException("Invalid " + what + ": null");
    }
    final int n = s.length();
    for (int i = 0; i < n; i++) {
      final char c = s.charAt(i);
      if (!(('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
          || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.'
          || c == '/' || Character.isLetter(c))) {
        throw new IllegalArgumentException("Invalid " + what
            + " (\"" + s + "\"): illegal character: " + c);
      }
    }
  }

  /**
   * Resolves all the tags (name=value) into the a sorted byte arrays.
   * This function is the opposite of {@link #getTagNames(java.util.List)}.
   * @param tags The tags to resolve.
   * @return an array of sorted tags (tag id, tag name).
   * @throws net.opentsdb.uid.NoSuchUniqueName if one of the elements in the map contained an
   * unknown tag name or tag value.
   */
  public Deferred<ArrayList<byte[]>> getAllTags(final Map<String, String> tags) {
      return getOrCreateAllTags(tags, false);
  }

  /**
   * Resolves (and creates, if necessary) all the tags (name=value) into the a
   * sorted byte arrays.
   * @param tags The tags to resolve.  If a new tag name or tag value is
   * seen, it will be assigned an ID.
   * @return an array of sorted tags (tag id, tag name).
   * @since 2.0
   */
  Deferred<ArrayList<byte[]>> getOrCreateAllTags(final Map<String, String> tags) {
    return getOrCreateAllTags(tags, true);
  }

  private Deferred<ArrayList<byte[]>> getOrCreateAllTags(final Map<String, String> tags,
                                                         final boolean create) {
    final ArrayList<Deferred<byte[]>> tag_ids =
      new ArrayList<Deferred<byte[]>>(tags.size());

    final boolean create_tagks = config.getBoolean("tsd.core.auto_create_tagks");
    final boolean create_tagvs = config.getBoolean("tsd.core.auto_create_tagvs");

    // For each tag, start resolving the tag name and the tag value.
    for (final Map.Entry<String, String> entry : tags.entrySet()) {
      final Deferred<byte[]> name_id = tag_names.getId(entry.getKey());
      final Deferred<byte[]> value_id = tag_values.getId(entry.getValue());

      class NoSuchTagNameCB implements Callback<Object, Exception> {
        public Object call(final Exception e) {
          if (e instanceof NoSuchUniqueName && create && create_tagks) {
            return tag_names.createId(entry.getKey());
          }

          return e; // Other unexpected exception, let it bubble up.
        }
      }

      class NoSuchTagValueCB implements Callback<Object, Exception> {
        public Object call(final Exception e) {
          if (e instanceof NoSuchUniqueName && create && create_tagvs) {
            return tag_values.createId(entry.getValue());
          }

          return e; // Other unexpected exception, let it bubble up.
        }
      }

      // Then once the tag name is resolved, get the resolved tag value.
      class TagNameResolvedCB implements Callback<Deferred<byte[]>, byte[]> {
        public Deferred<byte[]> call(final byte[] nameid) {
          // And once the tag value too is resolved, paste the two together.
          class TagValueResolvedCB implements Callback<byte[], byte[]> {
            public byte[] call(final byte[] valueid) {
              final byte[] thistag = new byte[nameid.length + valueid.length];
              System.arraycopy(nameid, 0, thistag, 0, nameid.length);
              System.arraycopy(valueid, 0, thistag, nameid.length, valueid.length);
              return thistag;
            }
          }

          return value_id
                  .addErrback(new NoSuchTagValueCB())
                  .addCallback(new TagValueResolvedCB());
        }
      }

      // Put all the deferred tag resolutions in this list.
      final Deferred<byte[]> resolve = name_id
              .addErrback(new NoSuchTagNameCB())
              .addCallbackDeferring(new TagNameResolvedCB());

      tag_ids.add(resolve);
    }

    // And then once we have all the tags resolved, sort them.
    return Deferred.group(tag_ids).addCallback(SORT_CB);
  }

  /**
   * Resolves all the tags IDs asynchronously (name followed by value) into a map.
   * This function is the opposite of {@link #getAllTags(java.util.Map)}.
   * @param tags The tag IDs to resolve.
   * @return A map mapping tag names to tag values.
   * @throws net.opentsdb.uid.NoSuchUniqueId if one of the elements in the array contained an
   * invalid ID.
   * @throws IllegalArgumentException if one of the elements in the array had
   * the wrong number of bytes.
   * @since 2.0
   */
  public Deferred<HashMap<String, String>> getTagNames(final List<byte[]> tags)
    throws NoSuchUniqueId {
    final short name_width = tag_names.width();
    final short value_width = tag_values.width();
    final short tag_bytes = (short) (name_width + value_width);
    final HashMap<String, String> result
      = new HashMap<String, String>(tags.size());
    final ArrayList<Deferred<String>> deferreds
      = new ArrayList<Deferred<String>>(tags.size());

    for (final byte[] tag : tags) {
      final byte[] tmp_name = new byte[name_width];
      final byte[] tmp_value = new byte[value_width];
      if (tag.length != tag_bytes) {
        throw new IllegalArgumentException("invalid length: " + tag.length
            + " (expected " + tag_bytes + "): " + Arrays.toString(tag));
      }
      System.arraycopy(tag, 0, tmp_name, 0, name_width);
      deferreds.add(tag_names.getName(tmp_name));
      System.arraycopy(tag, name_width, tmp_value, 0, value_width);
      deferreds.add(tag_values.getName(tmp_value));
    }

    class GroupCB implements Callback<HashMap<String, String>, ArrayList<String>> {
      public HashMap<String, String> call(final ArrayList<String> names)
          throws Exception {
        for (int i = 0; i < names.size(); i++) {
          if (i % 2 != 0) {
            result.put(names.get(i - 1), names.get(i));
          }
        }
        return result;
      }
    }

    return Deferred.groupInOrder(deferreds).addCallback(new GroupCB());
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
    validateUidName(type.toString(), name);
    UniqueId instance = uniqueIdInstanceForType(type);

    try {
      try {
        final byte[] uid = instance.getId(name).joinUninterruptibly();
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
    return uniqueId.getName(uid);
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
    return uniqueId.getId(name);
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
    final Deferred<byte[]> metric_id = metrics.getId(metric);

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
    return getOrCreateAllTags(tags)
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

  /**
   * Sorts a list of tags.
   * Each entry in the list expected to be a byte array that contains the tag
   * name UID followed by the tag value UID.
   */
  private static class SortResolvedTagsCB
    implements Callback<ArrayList<byte[]>, ArrayList<byte[]>> {
    public ArrayList<byte[]> call(final ArrayList<byte[]> tags) {
      // Now sort the tags.
      Collections.sort(tags, Bytes.MEMCMP);
      return tags;
    }
  }
}
