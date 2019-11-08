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
import java.util.Set;
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
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import net.opentsdb.common.Const;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = PreAggConfig.Builder.class)
public class PreAggConfig {

  public static final TypeReference<Map<String, PreAggConfig>> TYPE_REF =
      new TypeReference<Map<String, PreAggConfig>>() { };
  
  private List<MetricPattern> metrics;
  
  protected PreAggConfig(final Builder builder) {
    metrics = builder.metrics;
    Collections.sort(metrics);
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
    return Objects.equal(metrics, p.metrics);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  public HashCode buildHashCode() {
    final List<HashCode> hashes =
        Lists.newArrayListWithCapacity(metrics.size());
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
    private List<MetricPattern> metrics;
    
    public Builder addMetric(final MetricPattern metric) {
      if (metrics == null) {
        metrics = Lists.newArrayList();
      }
      metrics.add(metric);
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
    private TagsAndAggs group_all;
    private List<TagsAndAggs> aggs;
    
    protected MetricPattern(final Builder builder) {
      metric = builder.metric;
      aggs = builder.aggs;
      Collections.sort(aggs);
      pattern = Pattern.compile(metric);
      
      for (final TagsAndAggs agg : aggs) {
        if (agg.getTags().isEmpty()) {
          group_all = agg;
          break;
        }
      }
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
    
    public TagsAndAggs matchingTagsAndAggs(final Set<String> tag_keys) {
      if (tag_keys.isEmpty()) {
        if (group_all != null) {
          return group_all;
        } else {
          return null;
        }
      }
      
      for (final TagsAndAggs tags_and_aggs : aggs) {
        if (tags_and_aggs.matches(tag_keys)) {
          return tags_and_aggs;
        }
      }
      return null;
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
      
      public Builder addAggs(final TagsAndAggs aggs) {
        if (this.aggs == null) {
          this.aggs = Lists.newArrayList();
        }
        this.aggs.add(aggs);
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
      if (tags == null) {
        tags = Collections.emptyList();
      } else {
        Collections.sort(tags);
      }
      aggs = new TreeMap<String, Integer>();
      for (final Entry<String, Integer> entry : builder.aggs.entrySet()) {
        aggs.put(entry.getKey().toUpperCase(), entry.getValue());
      }
    }
    
    public List<String> getTags() {
      return tags;
    }
    
    public Map<String, Integer> getAggs() {
      return aggs;
    }
    
    boolean matches(final Set<String> tag_keys) {
      for (final String tag_key : tag_keys) {
        if (!tags.contains(tag_key)) {
          return false;
        }
      }
      return true;
    }
    
    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final TagsAndAggs tags_and_aggs = (TagsAndAggs) o;
      return Objects.equal(tags, tags_and_aggs.tags) &&
             Objects.equal(aggs, tags_and_aggs.aggs);
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
    
    public static Builder newBuilder() {
      return new Builder();
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder {
      @JsonProperty
      private List<String> tags;
      @JsonProperty
      private Map<String, Integer> aggs;
      
      public Builder addTag(final String tag) {
        if (tags == null) {
          tags = Lists.newArrayList();
        }
        tags.add(tag);
        return this;
      }
      
      public Builder setTags(final List<String> tags) {
        this.tags = tags;
        return this;
      }
      
      public Builder addAgg(final String agg, final int timestamp) {
        if (aggs == null) {
          aggs = Maps.newHashMap();
        }
        aggs.put(agg, timestamp);
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
