package net.opentsdb.uid.callbacks;

import com.google.common.collect.Maps;
import com.google.common.primitives.SignedBytes;
import com.stumbleupon.async.Callback;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * A callback that transforms a striped list of tag IDs into a map whose keys
 * are the tag keys and the map values are the tag values. Odd indexes in the
 * list are assumed to be the tag keys and even numbers are assumed to be the
 * tag values. The callback further assumes an even number of elements in the
 * given list. If the list does not have an even number of elements then {@link
 * java.util.Iterator#next} will throw a {@link java.util.NoSuchElementException}.
 */
public class StripedTagIdsToMap implements Callback<Map<byte[], byte[]>, ArrayList<byte[]>> {
  @Override
  public Map<byte[], byte[]> call(final ArrayList<byte[]> stripedTagIds) {
    final Map<byte[], byte[]> tagIdMap = Maps.newTreeMap(SignedBytes.lexicographicalComparator());

    Iterator<byte[]> tagIterator = stripedTagIds.iterator();

    while (tagIterator.hasNext()) {
      // The second call to #next here will throw NoSuchElementException if the
      // input list does not have an even number of elements and thus is missing
      // the tag value for the element.
      tagIdMap.put(tagIterator.next(), tagIterator.next());
    }

    return tagIdMap;
  }
}
