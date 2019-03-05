package net.opentsdb.events;

import com.yahoo.yamas.events.Event;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventResult implements EventsStorageResult {

  private BatchEventQuery batch_event_query;
  private EventQuery event_query;
  private long total_hits;
  private Throwable throwable;
  private List<Event> events;
  private Map<String, List<Bucket>> buckets;
  private EventResult result;

  /**
   * Package private ctor to construct a good query. Populates the namespaces
   * if the query type is not a namespace query.
   * @param result A non- null result.
   * @param query A non-null query.
   */
  EventResult(final EventsStorageResult.EventResult result,
                                     final BatchEventQuery query,
                                     final EventQuery event_query) {
    this.result = result;
    this.batch_event_query = query;
  }

  EventResult(final EventsStorageResult.EventResult result,
                                     final Throwable throwable,
                                     final BatchEventQuery query) {
    this.result = result;
    this.throwable = throwable;
    this.batch_event_query = query;
  }


  @Override
  public long totalHits() {
    return 0;
  }

  @Override
  public EventResult result() {
    return null;
  }

  @Override
  public Throwable exception() {
    return null;
  }

  @Override
  public Collection<Event> events() {
    return null;
  }

  @Override
  public Map<String, List<Bucket>> buckets() {
    return buckets;
  }

  public void addBuckets(final Map<String, List<Bucket>> bucket) {
    if (buckets == null) {
      buckets = new HashMap<>(bucket);

    } else {
      buckets.putAll(bucket);
    }
  }

  public void setTotalHits(long hits) {
    this.total_hits = hits;
  }
}
