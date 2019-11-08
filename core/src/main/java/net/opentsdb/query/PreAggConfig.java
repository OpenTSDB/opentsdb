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
package net.opentsdb.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import net.opentsdb.common.Const;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = PreAggConfig.Builder.class)
public class PreAggConfig {

  public static final TypeReference<List<PreAggConfig>> TYPE_REF =
      new TypeReference<List<PreAggConfig>>() { };
  
  private String namespace;
  private List<MetricPattern> metrics;
  
  protected PreAggConfig(final Builder builder) {
    namespace = builder.namespace;
    metrics = builder.metrics;
    Collections.sort(metrics);
  }
  
  public String getNamespace() {
    return namespace;
  }

  public List<MetricPattern> getMetrics() {
    return metrics;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    
    final PreAggConfig p = (PreAggConfig) o;
    return Objects.equal(namespace, p.namespace) &&
           Objects.equal(metrics, p.metrics);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  public HashCode buildHashCode() {
    final List<HashCode> hashes =
        Lists.newArrayListWithCapacity(metrics.size() + 1);
    hashes.add(Const.HASH_FUNCTION().newHasher()
        .putString(namespace, Const.UTF8_CHARSET)
        .hash());
    for (int i = 0; i < metrics.size(); i++) {
      hashes.add(metrics.get(i).buildHashCode());
    }
    return Hashing.combineOrdered(hashes);
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String namespace;
    @JsonProperty
    private List<MetricPattern> metrics;
    
    public Builder setNamespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }
    
    public Builder setMetrics(final List<MetricPattern> metrics) {
      this.metrics = metrics;
      return this;
    }
    
    public PreAggConfig build() {
      return new PreAggConfig(this);
    }
    
  }
  
  @JsonInclude(Include.NON_NULL)
  @JsonDeserialize(builder = MetricPattern.Builder.class)
  public static class MetricPattern implements Comparable<MetricPattern> {
    private String metric;
    private Pattern pattern;
    private List<TagsAndAggs> aggs;
    
    protected MetricPattern(final Builder builder) {
      metric = builder.metric;
      aggs = builder.aggs;
      Collections.sort(aggs);
      pattern = Pattern.compile(metric);
    }
    
    public String getMetric() {
      return metric;
    }

    public Pattern getPattern() {
      return pattern;
    }

    public List<TagsAndAggs> getAggs() {
      return aggs;
    }
    
    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      
      final MetricPattern m = (MetricPattern) o;
      return Objects.equal(metric, m.metric) &&
             Objects.equal(aggs, m.aggs);
    }
    
    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }
    
    public HashCode buildHashCode() {
      final List<HashCode> hashes =
          Lists.newArrayListWithCapacity(aggs.size() + 1);
      hashes.add(Const.HASH_FUNCTION().newHasher()
          .putString(metric, Const.UTF8_CHARSET)
          .hash());
      for (int i = 0; i < aggs.size(); i++) {
        hashes.add(aggs.get(i).buildHashCode());
      }
      return Hashing.combineOrdered(hashes);
    }

    @Override
    public int compareTo(final MetricPattern o) {
      return metric.compareTo(o.metric);
    }
    
    public static Builder newBuilder() {
      return new Builder();
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder {
      @JsonProperty
      private String metric;
      @JsonProperty
      private List<TagsAndAggs> aggs;
      
      public Builder setMetric(final String metric) {
        this.metric = metric;
        return this;
      }
      
      public Builder setAggs(final List<TagsAndAggs> aggs) {
        this.aggs = aggs;
        return this;
      }
      
      public MetricPattern build() {
        return new MetricPattern(this);
      }
    }

    
  }
  
  @JsonInclude(Include.NON_NULL)
  @JsonDeserialize(builder = TagsAndAggs.Builder.class)
  public static class TagsAndAggs implements Comparable<TagsAndAggs> {
    private List<String> tags;
    private Map<String, Integer> aggs;
    
    protected TagsAndAggs(final Builder builder) {
      tags = builder.tags;
      Collections.sort(tags);
      aggs = new TreeMap<String, Integer>(builder.aggs);
    }
    
    public List<String> getTags() {
      return tags;
    }
    
    public Map<String, Integer> getAggs() {
      return aggs;
    }
    
    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      
      final TagsAndAggs aggs = (TagsAndAggs) o;
      return Objects.equal(tags, aggs.tags) &&
             Objects.equal(aggs, aggs.aggs);
    }
    
    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }
    
    public HashCode buildHashCode() {
      Hasher hasher = Const.HASH_FUNCTION().newHasher();
      for (int i = 0; i < tags.size(); i++) {
        hasher.putString(tags.get(i), Const.UTF8_CHARSET);
      }
      for (final Entry<String, Integer> entry : aggs.entrySet()) {
        hasher.putString(entry.getKey(), Const.UTF8_CHARSET)
              .putInt(entry.getValue());
      }
      return hasher.hash();
    }
    
    @Override
    public int compareTo(final TagsAndAggs o) {
      if (tags.size() != o.tags.size()) {
        return tags.size() - o.tags.size();
      }
      
      for (int i = 0; i < tags.size(); i++) {
        int cmp = tags.get(i).compareTo(o.tags.get(i));
        if (cmp != 0) {
          return cmp;
        }
      }
   
      if (aggs.size() != o.aggs.size()) {
        return aggs.size() - o.aggs.size();
      }
      
      for (final Entry<String, Integer> entry : aggs.entrySet()) {
        Integer other = o.aggs.get(entry.getKey());
        if (other == null) {
          return 1;
        }
        if (other != entry.getValue()) {
          return entry.getValue() - other;
        }
      }
      return 0;
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder {
      @JsonProperty
      private List<String> tags;
      @JsonProperty
      private Map<String, Integer> aggs;
      
      public Builder setTags(final List<String> tags) {
        this.tags = tags;
        return this;
      }
      
      public Builder setAggs(final Map<String, Integer> aggs) {
        this.aggs = aggs;
        return this;
      }
      
      public TagsAndAggs build() {
        return new TagsAndAggs(this);
      }
    }
  }
}
