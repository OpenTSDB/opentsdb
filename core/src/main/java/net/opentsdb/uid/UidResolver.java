package net.opentsdb.uid;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import com.google.common.primitives.SignedBytes;
import com.typesafe.config.Config;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.UniqueIdClient;

public class UidResolver {
  private final UniqueIdClient idClient;

  private final boolean create_metrics;
  private final boolean create_tagks;
  private final boolean create_tagvs;

  public UidResolver(final UniqueIdClient idClient, final Config config) {
    this.idClient = idClient;

    create_metrics = config.getBoolean("tsd.core.auto_create_metrics");
    create_tagks = config.getBoolean("tsd.core.auto_create_tagks");
    create_tagvs = config.getBoolean("tsd.core.auto_create_tagvs");
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
      uids.add(idClient.getUID(type, uid_name));
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
    @Override
    public ArrayList<byte[]> call(final ArrayList<byte[]> tags) {
      // Now sort the tags.
      Collections.sort(tags, SignedBytes.lexicographicalComparator());
      return tags;
    }
  }
  private static final SortResolvedTagsCB SORT_CB = new SortResolvedTagsCB();
}
