// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.idconverter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import net.opentsdb.core.Const;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.query.BaseQueryNodeConfig;

import java.util.Map;

/**
 * Simple config wherein all we need is the ID and some factories. 
 * Nothing else is configurable.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = ByteToStringIdConverterConfig.Builder.class)
public class ByteToStringIdConverterConfig extends BaseQueryNodeConfig<ByteToStringIdConverterConfig.Builder, ByteToStringIdConverterConfig> {
  
  /** The map of data sources to factories. */
  private Map<String, TimeSeriesDataSourceFactory> data_sources;
  
  protected ByteToStringIdConverterConfig(final Builder builder) {
    super(builder);
    data_sources = builder.data_sources;
  }
  
  /**
   * Returns the factory for a source if found.
   * @param source The non-null source to look for.
   * @return A factory for the source, null if not found.
   */
  public TimeSeriesDataSourceFactory getFactory(final String source) {
    if (data_sources == null) {
      return null;
    }
    return data_sources.get(source);
  }
  
  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return Const.HASH_FUNCTION().newHasher()
        .putString(id, Const.UTF8_CHARSET)
        .hash();
  }

  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return false;
  }

  @Override
  public Builder toBuilder() {
    return null;
  }

  @Override
  public int compareTo(final ByteToStringIdConverterConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(final Object o) {
    // TODO Auto-generated method stub
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof ByteToStringIdConverterConfig)) {
      return false;
    }
    
    return id.equals(((ByteToStringIdConverterConfig) o).id);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder extends BaseQueryNodeConfig.Builder<Builder, ByteToStringIdConverterConfig> {
    protected Map<String, TimeSeriesDataSourceFactory> data_sources;
    
    Builder() {
      setType(ByteToStringIdConverterFactory.TYPE);
    }
    
    public Builder setDataSources(
        final Map<String, TimeSeriesDataSourceFactory> data_sources) {
      this.data_sources = data_sources;
      return this;
    }
    
    public Builder addDataSource(final String source, 
                                 final TimeSeriesDataSourceFactory factory) {
      if (data_sources == null) {
        data_sources = Maps.newHashMap();
      }
      data_sources.put(source, factory);
      return this;
    }
    
    @Override
    public ByteToStringIdConverterConfig build() {
      return new ByteToStringIdConverterConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
  }
}