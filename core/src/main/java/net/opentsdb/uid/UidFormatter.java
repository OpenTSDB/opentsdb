package net.opentsdb.uid;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.UniqueIdClient;

import static com.google.common.base.Preconditions.checkNotNull;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static net.opentsdb.uid.UniqueIdType.TAGK;
import static net.opentsdb.uid.UniqueIdType.TAGV;

/**
 * Looks up uid names based on uids.
 */
public class UidFormatter {
  /**
   * The internally held tsdb instance.
   */
  private final UniqueIdClient idClient;

  /**
   * Construct a UidFormatter with the provided id client.
   */
  public UidFormatter(final UniqueIdClient idClient) {
    this.idClient = checkNotNull(idClient);
  }

  /**
   * Returns the name of the metric behind the {@code uid}.
   * @param uid The uid to lookup
   */
  public Deferred<String> formatMetric(final byte[] uid) {
    return idClient.getUidName(METRIC, checkNotNull(uid));
  }

  /**
   * Returns the tag names of the ids stores in {@code tags}.
   * @param tags The tag ids to format
   * @return A map of tag names (keys), tag values (values).
   */
  public Deferred<ImmutableMap<String, String>> formatTags(final Map<byte[],byte[]> tags) {
    checkNotNull(tags);

    final ArrayList<Deferred<String>> deferreds =
            Lists.newArrayListWithCapacity(tags.size() * 2);

    for (Map.Entry<byte[], byte[]> tag : tags.entrySet()) {
      deferreds.add(idClient.getUidName(TAGK, tag.getKey()));
      deferreds.add(idClient.getUidName(TAGV, tag.getValue()));
    }



    return Deferred.groupInOrder(deferreds).addCallback(new NameCB());
  }

  public Deferred<ImmutableMap<String, String>> formatTags(final List<byte[]> tags) {
    checkNotNull(tags);

    final ArrayList<Deferred<String>> deferreds =
            Lists.newArrayListWithCapacity(tags.size());

    for (int i = 0 ; i < tags.size() ; i+=2) {
      deferreds.add(idClient.getUidName(TAGK, tags.get(i)));
      deferreds.add(idClient.getUidName(TAGV, tags.get(i + 1)));
    }

    return Deferred.groupInOrder(deferreds).addCallback(new NameCB());
  }

  public Deferred<ArrayList<String>> formatUids(final List<byte[]> uids,
                                                final UniqueIdType type) {
    checkNotNull(uids);

    final ArrayList<Deferred<String>> deferreds =
            Lists.newArrayListWithCapacity(uids.size());

    for (byte[] uid : uids) {
      deferreds.add(idClient.getUidName(type, uid));
    }

    return Deferred.groupInOrder(deferreds);
  }

  private static class NameCB implements Callback<ImmutableMap<String,String>,
          ArrayList<String>> {
    @Override
    public ImmutableMap<String, String> call(final ArrayList<String> names) {
      ImmutableMap.Builder<String, String> result = ImmutableMap.builder();

      Iterator<String> name_it = names.iterator();

      while (name_it.hasNext()) {
        final String tagk = name_it.next();
        final String tagv = name_it.next();
        result.put(tagk, tagv);
      }

      return result.build();
    }
  }
}
