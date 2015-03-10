package net.opentsdb.uid;

import com.stumbleupon.async.Deferred;

public interface IdLookupStrategy {
  Deferred<byte[]> getId(final UniqueId uniqueId, final String name);
}
