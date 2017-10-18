//This file is part of OpenTSDB.
//Copyright (C) 2017  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

/**
 * A simple base config class for {@link TimeSeriesDataSource} nodes.
 * 
 * @since 3.0
 */
public class QuerySourceConfig implements QueryNodeConfig {
  
  /** The query from the caller. */
  protected final TimeSeriesQuery query;
  
  /** A unique name for this config. */
  private final String id;
  
  QuerySourceConfig(final Builder builder) {
    query = builder.query;
    id = builder.id;
  }
  
  public TimeSeriesQuery query() {
    return query;
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private TimeSeriesQuery query;
    private String id;
    
    public Builder setQuery(final TimeSeriesQuery query) {
      this.query = query;
      return this;
    }
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public QuerySourceConfig build() {
      return new QuerySourceConfig(this);
    }
  }
}
