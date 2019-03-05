package net.opentsdb.events;

import com.stumbleupon.async.Deferred;
import net.opentsdb.stats.Span;

import java.util.Map;

public interface EventsStorageSchema {


  /**
   * Executes the given query, run it against events store.
   * @param query A non-null query source to resolve.
   * @param span An optional tracing span.
   * @return A deferred resolving to events result or
   * an exception if something went very wrong. It's better to return
   * a result with the exception set.
   */
  public Deferred<Map<String, EventsStorageResult>> runQuery(
    final BatchEventQuery query,
    final Span span);

}
