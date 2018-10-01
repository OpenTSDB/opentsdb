// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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
package net.opentsdb.query.execution.serdes;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import net.opentsdb.query.serdes.SerdesOptions;

/**
 * A base serdes option class.
 * 
 * @since 3.0
 */
public class BaseSerdesOptions implements SerdesOptions {
  
  /** The ID of this config. */
  protected String id;
  
  /** The type of serialization plugin this config refers to. */
  protected String type;
  
  /** A list of serialization filters. */
  protected List<String> filter;
  
  /**
   * Default ctor.
   * @param builder Non-null builder.
   */
  protected BaseSerdesOptions(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    id = builder.id;
    if (Strings.isNullOrEmpty(builder.type)) {
      type = builder.id;
    } else {
      type = builder.type;
    }
    filter = builder.filter;
  }
  
  public String getId() {
    return id;
  }
  
  public String getType() {
    return type;
  }
  
  @Override
  public List<String> getFilter() {
    return filter == null ? Collections.emptyList() : filter;
  }
  
  /** @return A new builder. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    @JsonProperty
    private String id;
    @JsonProperty
    private String type;
    @JsonProperty
    private List<String> filter;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setType(final String type) {
      this.type = type;
      return this;
    }
    
    public Builder setFilter(final List<String> filter) {
      this.filter = filter;
      return this;
    }
    
    public Builder addFilter(final String filter) {
      if (this.filter == null) {
        this.filter = Lists.newArrayList();
      }
      this.filter.add(filter);
      return this;
    }
    
    /** @return A constructed serdes object. May throw exceptions. */
    public SerdesOptions build() {
      return new BaseSerdesOptions(this);
    }
  }
}
