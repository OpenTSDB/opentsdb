package net.opentsdb.meta;

import net.opentsdb.data.TimeStamp;

import java.util.ArrayList;
import java.util.List;

public interface BatchMetaQuery {

  public static enum QueryType {
    NAMESPACES,
    METRICS,
    TAG_KEYS,
    TAG_VALUES,
    TAG_KEYS_AND_VALUES,
    TIMESERIES
  }

  public static enum Order {
    ASCENDING,
    DESCENDING
  }

  public int from();

  public int to();

  public String aggregationField();

  public int aggregationSize();

  public QueryType type();

  public Order order();

  public TimeStamp start();

  public TimeStamp end();

  public List<MetaQuery> metaQueries();

  public List<MetaQuery> meta_query = new ArrayList<>();

  /**
   * Builder through which the query is parsed and parameters are set
   */
  public static abstract class Builder {
    protected int from;
    protected int to;
    protected String aggregationField;
    protected int agg_size;
    protected QueryType type;
    protected Order order = Order.ASCENDING;
    protected String start;
    protected String end;
    protected String time_zone;
    protected List<MetaQuery> meta_query;

    public Builder setFrom(final int from) {
      this.from = from;
      return this;
    }

    public Builder setTo(final int to) {
      this.to = to;
      return this;
    }

    public Builder setAggregationField(final String aggregationField) {
      this.aggregationField = aggregationField;
      return this;
    }

    public Builder setAggregationSize(final int aggregation_size) {
      this.agg_size = aggregation_size;
      return this;
    }

    public Builder setType(final QueryType type) {
      this.type = type;
      return this;
    }

    public Builder setOrder(final Order order) {
      this.order = order;
      return this;
    }

    public Builder setStart(final String start) {
      this.start = start;
      return this;
    }

    public Builder setEnd(final String end) {
      this.end = end;
      return this;
    }

    public Builder setTimeZone(final String time_zone) {
      this.time_zone = time_zone;
      return this;
    }

    public Builder setMetaQuery(final List<MetaQuery> meta_query) {
      this.meta_query = meta_query;
      System.out.println("here" + this.meta_query);
      return this;
    }

    public abstract BatchMetaQuery build();

  }

}
