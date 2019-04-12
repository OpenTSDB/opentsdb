// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.meta;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.filter.QueryFilter;

/**
 * Represents parameters to search for metadata.
 *
 * @since 3.0
 */
public interface MetaQuery {

  public String namespace();

  public QueryFilter filter();

  public String id();
  
  /**
   * Builder through which the query is parsed and parameters are set
   */
  public static abstract class Builder {
    protected String namespace;
    protected QueryFilter filter;
    protected String id;

    public Builder setNamespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setFilter(final QueryFilter filter) {
      this.filter = filter;
      return this;
    }

    public Builder setId(final String id) {
      this.id = id;
      return this;
    }

    public abstract MetaQuery build();

  }
  
}
