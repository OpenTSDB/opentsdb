package net.opentsdb.events;

import net.opentsdb.query.filter.QueryFilter;

/**
 * Represents parameters to search for metadata.
 *
 * @since 3.0
 */
public interface EventQuery {

  public String namespace();

  public QueryFilter filter();

  /**
   * Builder through which the query is parsed and parameters are set
   */
  public static abstract class Builder {
    protected String namespace;
    protected QueryFilter filter;

    public Builder setNamespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setFilter(final QueryFilter filter) {
      this.filter = filter;
      return this;
    }

    public abstract EventQuery build();

  }

}