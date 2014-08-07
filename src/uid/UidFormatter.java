package net.opentsdb.uid;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.TSDB;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.uid.UniqueId.UniqueIdType;
import static net.opentsdb.uid.UniqueId.UniqueIdType.*;

/**
 * Looks up uid names based on uids.
 */
public class UidFormatter {
  /**
   * The internally held tsdb instance.
   */
  private final TSDB tsdb;

  /**
   * Construct a UidFormatter with a TSDB instance.
   * @param tsdb
   */
  public UidFormatter(final TSDB tsdb) {
    this.tsdb = checkNotNull(tsdb);
  }

  /**
   * Returns the name of the metric behind the {@code uid}.
   * @param uid The uid to lookup
   */
  public Deferred<String> formatMetric(final byte[] uid) {
    return tsdb.getUidName(METRIC, checkNotNull(uid));
  }

  /**
   * Returns the tag names of the ids stores in {@code tags}.
   * @param tags The tag ids to format
   * @return A map of tag names (keys), tag values (values).
   */
  public Deferred<Map<String,String>> formatTags(final Map<byte[],byte[]> tags) {
    checkNotNull(tags);

    final ArrayList<Deferred<String>> deferreds =
            Lists.newArrayListWithCapacity(tags.size() * 2);

    for (Map.Entry<byte[], byte[]> tag : tags.entrySet()) {
      deferreds.add(tsdb.getUidName(TAGK, tag.getKey()));
      deferreds.add(tsdb.getUidName(TAGV, tag.getValue()));
    }

    class NameCB implements Callback<Map<String,String>, ArrayList<String>> {
      @Override
      public Map<String, String> call(final ArrayList<String> names) {
        final Map<String,String> result = Maps.newHashMapWithExpectedSize(tags.size());

        Iterator<String> name_it = names.iterator();

        while (name_it.hasNext()) {
          final String tagk = name_it.next();
          final String tagv = name_it.next();
          result.put(tagk, tagv);
        }

        return result;
      }
    }

    return Deferred.groupInOrder(deferreds).addCallback(new NameCB());
  }

  public Deferred<ArrayList<String>> formatUids(final List<byte[]> uids,
                                                final UniqueIdType type) {
    checkNotNull(uids);

    final ArrayList<Deferred<String>> deferreds =
            Lists.newArrayListWithCapacity(uids.size());

    for (byte[] uid : uids) {
      deferreds.add(tsdb.getUidName(type, uid));
    }

    return Deferred.groupInOrder(deferreds);
  }
}
