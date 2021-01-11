// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timedifference;

import java.time.temporal.ChronoUnit;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;

/**
 * Config for the time difference processor. Just has a resolution.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = TimeDifferenceConfig.Builder.class)
public class TimeDifferenceConfig extends BaseQueryNodeConfig<
  TimeDifferenceConfig.Builder, TimeDifferenceConfig> {

  private final ChronoUnit resolution;
  
  protected TimeDifferenceConfig(final Builder builder) {
    super(builder);
    resolution = builder.resolution;
    if (resolution != ChronoUnit.HOURS &&
        resolution != ChronoUnit.MINUTES &&
        resolution != ChronoUnit.SECONDS &&
        resolution != ChronoUnit.MILLIS &&
        resolution != ChronoUnit.NANOS) {
      throw new IllegalArgumentException("Resolution " + resolution + " is not supported.");
    }
  }

  public ChronoUnit getResolution() {
    return resolution;
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
    final Builder builder = new Builder()
        .setResolution(resolution);
    super.toBuilder(builder);
    return builder;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TimeDifferenceConfig config = (TimeDifferenceConfig) o;
    return Objects.equal(resolution, config.getResolution()) &&
        Objects.equal(id, config.getId());
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final List<HashCode> hashes =
        Lists.newArrayListWithCapacity(3);
    hashes.add(super.buildHashCode());
    
    final Hasher hasher = Const.HASH_FUNCTION().newHasher();
    hasher.putString(resolution.name(), Const.UTF8_CHARSET);
    hashes.add(hasher.hash());
    return Hashing.combineOrdered(hashes);
  }
  
  @Override
  public int compareTo(final TimeDifferenceConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  /**
   * Parses a JSON config.
   * @param mapper The non-null mapper.
   * @param tsdb The non-null TSDB for factories.
   * @param node The non-null node.
   * @return The parsed config.
   */
  public static TimeDifferenceConfig parse(final ObjectMapper mapper,
                                           final TSDB tsdb,
                                           final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("resolution");
    if (n != null && !n.isNull()) {
      builder.setResolution(ChronoUnit.valueOf(n.asText()));
    } else {
      throw new IllegalArgumentException("Missing resolution.");
    }
    
    BaseQueryNodeConfigWithInterpolators.parse(builder, mapper, tsdb, node);
    
    return builder.build();
  }
  
  /** @return A new builder to construct a RateOptions from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Builder extends BaseQueryNodeConfig.Builder<Builder, TimeDifferenceConfig> {

    private ChronoUnit resolution;
    
    Builder() {
      setType(TimeDifferenceFactory.TYPE);
    }
    
    public Builder setResolution(final ChronoUnit resolution) {
      this.resolution = resolution;
      return this;
    }
    
    @Override
    public TimeDifferenceConfig build() {
      return new TimeDifferenceConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
    
  }
}
