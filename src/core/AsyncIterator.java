package net.opentsdb.core;

import java.util.Iterator;

import com.stumbleupon.async.Deferred;

public interface AsyncIterator<E> extends Iterator<E> {
  public Deferred<Boolean> hasMore();
}
