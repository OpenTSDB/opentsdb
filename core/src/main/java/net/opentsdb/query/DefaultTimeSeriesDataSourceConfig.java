// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.HashCode;

import net.opentsdb.core.TSDB;

@JsonInclude(Include.NON_DEFAULT)
@JsonDeserialize(builder = DefaultTimeSeriesDataSourceConfig.Builder.class)
public class DefaultTimeSeriesDataSourceConfig
    extends BaseTimeSeriesDataSourceConfig<
        DefaultTimeSeriesDataSourceConfig.Builder, DefaultTimeSeriesDataSourceConfig> {

  public static final String TYPE = "TimeSeriesDataSourceConfig";

  protected DefaultTimeSeriesDataSourceConfig(final Builder builder) {
    super(builder);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Builder toBuilder() {
    Builder builder = new Builder();
    cloneBuilder(this, builder);
    return builder;
  }

  public static DefaultTimeSeriesDataSourceConfig parseConfig(
      final ObjectMapper mapper, final TSDB tsdb, final JsonNode node) {
    final Builder builder = new Builder();
    parseConfig(mapper, tsdb, node, builder);
    return builder.build();
  }

  @Override
  public int compareTo(final DefaultTimeSeriesDataSourceConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HashCode buildHashCode() {
    if (cached_hash != null) {
      return cached_hash;
    }
    cached_hash = super.buildHashCode();
    return cached_hash;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder
      extends BaseTimeSeriesDataSourceConfig.Builder<Builder, DefaultTimeSeriesDataSourceConfig> {

    @Override
    public DefaultTimeSeriesDataSourceConfig build() {
      return new DefaultTimeSeriesDataSourceConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }

  }
}
