package net.opentsdb.uid;

import com.stumbleupon.async.Deferred;

public class SimpleIdLookupStrategy implements IdLookupStrategy {
  @Override
  public Deferred<byte[]> getId(final UniqueId uniqueId, final String name) {
    return uniqueId.getId(name);
  }
}
