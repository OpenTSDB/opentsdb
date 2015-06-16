package net.opentsdb.uid.callbacks;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link Function} that transforms a striped list of things into a map. Odd indexes in the list
 * are assumed to be the keys and even indexes are assumed to be the values. The function further
 * assumes an even number of elements in the given list. If the list does not have an even number of
 * elements then {@link java.util.Iterator#next} will throw a {@link
 * java.util.NoSuchElementException}.
 */
public class StripedToMap<K extends Comparable<K>> implements Function<List<K>, Map<K, K>> {
  @Nullable
  @Override
  public Map<K, K> apply(final List<K> stripedTagIds) {
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
