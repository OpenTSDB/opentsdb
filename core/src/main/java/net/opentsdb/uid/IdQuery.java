package net.opentsdb.uid;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;

public class IdQuery {
  public static final int NO_LIMIT = 0;

  private final UniqueIdType type;
  private final String query;
  private final int maxResults;

  public IdQuery(final String query,
                 final UniqueIdType type) {
    this(query, type, NO_LIMIT);
  }

  public IdQuery(final String query,
                 final UniqueIdType type,
                 final int maxResults) {
    checkArgument(maxResults >= 0,
            "The provided max results must either have no limit or be > 0");

    this.query = Strings.emptyToNull(query);
    this.type = type;
    this.maxResults = maxResults;
  }

  public UniqueIdType getType() {
    return type;
  }

  public String getQuery() {
    return query;
  }

  public int getMaxResults() {
    return maxResults;
  }
}
