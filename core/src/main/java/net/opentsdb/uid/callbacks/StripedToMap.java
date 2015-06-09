package net.opentsdb.uid.callbacks;

import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * A callback that transforms a striped list of into a map. Odd indexes in the list are assumed to
 * be the keys and even indexes are assumed to be the values. The callback further assumes an even
 * number of elements in the given list. If the list does not have an even number of elements then
 * {@link java.util.Iterator#next} will throw a {@link java.util.NoSuchElementException}.
 */
public class StripedToMap<K extends Comparable<K>> implements Callback<Map<K, K>, ArrayList<K>> {
  @Override
  public Map<K, K> call(final ArrayList<K> stripedTagIds) {
    final Map<K, K> tagIdMap = Maps.newTreeMap();

    Iterator<K> tagIterator = stripedTagIds.iterator();

    while (tagIterator.hasNext()) {
      // The second call to #next here will throw NoSuchElementException if the
      // input list does not have an even number of elements and thus is missing
      // the tag value for the element.
      tagIdMap.put(tagIterator.next(), tagIterator.next());
    }

    return tagIdMap;
  }
}
