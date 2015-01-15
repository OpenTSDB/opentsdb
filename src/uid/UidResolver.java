package net.opentsdb.uid;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;

import static com.google.common.base.Preconditions.checkNotNull;

public class UidResolver {
  private final TSDB tsdb;

  private final boolean create_metrics;
  private final boolean create_tagks;
  private final boolean create_tagvs;

  public UidResolver(final TSDB tsdb) {
    this.tsdb = checkNotNull(tsdb);

    Config conf = tsdb.getConfig();
    create_metrics = conf.getBoolean("tsd.core.auto_create_metrics");
    create_tagks = conf.getBoolean("tsd.core.auto_create_tagks");
    create_tagvs = conf.getBoolean("tsd.core.auto_create_tagvs");
  }

  public Deferred<ArrayList<byte[]>> resolve(final Iterable<String> uid_names,
                                             final UniqueIdType type) {
    return resolve(uid_names.iterator(), type);
  }

  public Deferred<ArrayList<byte[]>> resolve(final Iterator<String> uid_names,
                                             final UniqueIdType type) {
    final ArrayList<Deferred<byte[]>> uids = Lists.newArrayList();

    // For each tag, start resolving the tag name and the tag value.
    while (uid_names.hasNext()) {
      final String uid_name = uid_names.next();
      uids.add(tsdb.getUniqueIdClient().getUID(type, uid_name));
    }

    // And then once we have all the tags resolved, sort them.
    return Deferred.group(uids).addCallback(SORT_CB);
  }

  private boolean autoCreate(UniqueIdType type) {
    switch (type) {
      case METRIC:
        return create_metrics;
      case TAGK:
        return create_tagks;
      case TAGV:
        return create_tagvs;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  /**
   * Sorts a list of tags.
   * Each entry in the list expected to be a byte array that contains the tag
   * name UID followed by the tag value UID.
   */
  private static class SortResolvedTagsCB implements Callback<ArrayList<byte[]>, ArrayList<byte[]>> {
    public ArrayList<byte[]> call(final ArrayList<byte[]> tags) {
      // Now sort the tags.
      Collections.sort(tags, Bytes.MEMCMP);
      return tags;
    }
  }
  private static final SortResolvedTagsCB SORT_CB = new SortResolvedTagsCB();
}
