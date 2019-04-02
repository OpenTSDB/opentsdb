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
package net.opentsdb.servlet.sinks;

import java.util.List;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.query.QuerySinkConfig;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.stats.StatsCollector.StatsTimer;

/**
 * A simple sink config for the Servlet resources.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = ServletSinkConfig.Builder.class)
public class ServletSinkConfig implements QuerySinkConfig {

  private final String id;
  private final SerdesOptions options;
  private final AsyncContext async;
  private final HttpServletResponse response;
  private final HttpServletRequest request;
  private final StatsTimer stats_timer;
  
  ServletSinkConfig(final Builder builder) {
    id = builder.id;
    options = builder.serdesOptions;
    async = builder.async;
    response = builder.response;
    request = builder.request;
    stats_timer = builder.stats_timer;
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SerdesOptions serdesOptions() {
    return options;
  }

  public AsyncContext async() {
    return async;
  }
  
  public HttpServletResponse response() {
    return response;
  }
  
  public HttpServletRequest request() {
    return request;
  }
  
  public StatsTimer statsTimer() {
    return stats_timer;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static Builder newBuilder(final QuerySinkConfig config) {
    return new Builder()
        .setId(config.getId())
        .setSerdesOptions(config.serdesOptions());
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String id;
    @JsonProperty
    private SerdesOptions serdesOptions;
    
    private AsyncContext async;
    private HttpServletResponse response;
    private HttpServletRequest request;
    private StatsTimer stats_timer;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setSerdesOptions(final SerdesOptions serdes_options) {
      serdesOptions = serdes_options;
      return this;
    }
    
    public Builder setAsync(final AsyncContext async) {
      this.async = async;
      return this;
    }
    
    public Builder setResponse(final HttpServletResponse response) {
      this.response = response;
      return this;
    }
    
    public Builder setRequest(final HttpServletRequest request) {
      this.request = request;
      return this;
    }
    
    public Builder setStatsTimer(final StatsTimer stats_timer) {
      this.stats_timer = stats_timer;
      return this;
    }
    
    public ServletSinkConfig build() {
      return new ServletSinkConfig(this);
    }
  }
}
