// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timeshift;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import com.google.common.hash.Hashing;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.TimeSeriesDataSourceConfig;

import java.util.List;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TimeShiftConfig.Builder.class)
public class TimeShiftConfig extends BaseQueryNodeConfig {

  protected TimeSeriesDataSourceConfig config;
  
  protected TimeShiftConfig(final Builder builder) {
    super(builder);
    config = builder.config;
  }
  
  public TimeSeriesDataSourceConfig getConfig() {
    return config;
  }
  
  @Override
  public Builder toBuilder() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public int compareTo(final QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    if (!super.equals(o)) {
      return false;
    }

    final TimeShiftConfig tsconfig = (TimeShiftConfig) o;

    return Objects.equal(config, tsconfig.getConfig());
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  @Override
  public HashCode buildHashCode() {
    final List<HashCode> hashes =
            Lists.newArrayListWithCapacity(2);

    hashes.add(super.buildHashCode());

    hashes.add(config.buildHashCode());

    return Hashing.combineOrdered(hashes);
  }

  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return false;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseQueryNodeConfig.Builder {
    protected TimeSeriesDataSourceConfig config;
    
    Builder() {
      setType(TimeShiftFactory.TYPE);
    }
    
    public Builder setConfig(final TimeSeriesDataSourceConfig config) {
      this.config = config;
      return this;
    }
    
    public QueryNodeConfig build() {
      return new TimeShiftConfig(this);
    }
  }
  
}
