package net.opentsdb.storage.cassandra;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.stumbleupon.async.Deferred;

class MoreFutures {
  static <V> Deferred<V> wrap(ListenableFuture<V> future) {
    final Deferred<V> deferred = new Deferred<>();

    Futures.addCallback(future, new FutureCallback<V>() {
      @Override
      public void onSuccess(final V result) {
        deferred.callback(result);
      }

      @Override
      public void onFailure(final Throwable t) {
        // TODO this is not entirely correct. The deferred should be called with
        // an exception and not a throwable?
        deferred.callback(t);
      }
    });

    return deferred;
  }
}
