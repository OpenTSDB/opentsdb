package net.opentsdb.uid;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;

public class IdQuery {
  public static final int NO_LIMIT = 0;

  private final IdType type;
  private final String query;
  private final int maxResults;

  /**
   * Create a new id query that will search among the ids of the provided type and return at most
   * the number of results as indicated with {@code maxResults}.
   */
  public IdQuery(final String query,
                 final IdType type,
                 final int maxResults) {
    checkArgument(maxResults >= 0,
        "The provided max results must either have no limit or be > 0");

    this.query = Strings.emptyToNull(query);
    this.type = type;
    this.maxResults = maxResults;
  }

  public IdType getType() {
    return type;
  }

  public String getQuery() {
    return query;
  }

  public int getMaxResults() {
    return maxResults;
  }
}
