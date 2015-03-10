package net.opentsdb.uid.callbacks;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.stumbleupon.async.Callback;

import java.util.ArrayList;
import java.util.Iterator;

public class StripedTagIdsToList implements Callback<ArrayList<byte[]>, ArrayList<byte[]>> {
  @Override
  public ArrayList<byte[]> call(final ArrayList<byte[]> stripedTagIds) {
    final ArrayList<byte[]> tagIdList = Lists.newArrayListWithCapacity(stripedTagIds.size() / 2);

    Iterator<byte[]> tagIterator = stripedTagIds.iterator();

    while (tagIterator.hasNext()) {
      // The second call to #next here will throw NoSuchElementException if the
      // input list does not have an even number of elements and thus is missing
      // the tag value for the element.
      tagIdList.add(Bytes.concat(tagIterator.next(), tagIterator.next()));
    }

    return tagIdList;
  }
}
