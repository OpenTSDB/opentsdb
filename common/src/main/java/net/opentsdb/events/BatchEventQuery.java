package net.opentsdb.events;

import java.util.List;

/**
 * Represents parameters to search for metadata.
 *
 * @since 3.0
 */
public interface BatchEventQuery {

  public long startTimestamp();

  public long endTimestamp();

  public List<EventQuery> eventQueries();

  public boolean isSummary();

  public String interval();

  public int from();

  public int to();

  /**
   * Builder through which the query is parsed and parameters are set
   */
  public static abstract class Builder {
    protected long start_timestamp;
    protected long end_timestamp;
    protected List<EventQuery> event_queries;
    protected boolean is_summary;
    protected String interval;
    protected int from;
    protected int to;

    public Builder setStartTimestamp(final long start_timestamp) {
      this.start_timestamp = start_timestamp;
      return this;
    }

    public Builder setEndTimestamp(final long end_timestamp) {
      this.end_timestamp = end_timestamp;
      return this;
    }

    public Builder setEventQuery(final List<EventQuery> event_queries) {
      this.event_queries = event_queries;
      return this;
    }

    public Builder setIsSummary(final boolean is_summary) {
      this.is_summary = is_summary;
      return this;
    }

    public Builder setInterval(final String interval) {
      this.interval = interval;
      return this;
    }

    public Builder setFrom(final int from) {
      this.from = from;
      return this;
    }

    public Builder setTo(final int to) {
      this.to = to;
      return this;
    }


    public abstract BatchEventQuery build();

  }

}