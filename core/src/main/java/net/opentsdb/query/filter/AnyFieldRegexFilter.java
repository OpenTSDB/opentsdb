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
package net.opentsdb.query.filter;

import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.Span;

/**
 * Regular expression filter to search for in any field for meta.
 *
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = AnyFieldRegexFilter.Builder.class)
public class AnyFieldRegexFilter extends BaseTagValueFilter {

  /**
   * The compiled pattern
   */
  final Pattern pattern;


  /**
   * Protected ctor.
   *
   * @param builder The non-null builder.
   */
  protected AnyFieldRegexFilter(final Builder builder) {
    super(".*", builder.filter);
    pattern = Pattern.compile(filter.trim());
  }

  @Override
  public boolean matches(final Map<String, String> tags) {
    final String tagv = tags.get(tag_key);
    if (tagv == null) {
      return false;
    }

    return pattern.matcher(tagv).find();
  }

  @Override
  public String getType() {
    return AnyFieldRegexFactory.TYPE;
  }


  @Override
  public String toString() {
    return new StringBuilder()
            .append("{type=")
            .append(getClass().getSimpleName())
            .append(", filter=")
            .append(filter)
            .toString();
  }

  @Override
  public Deferred<Void> initialize(final Span span) {
    return INITIALIZED;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {

    @JsonProperty
    private String filter;


    public Builder setFilter(final String filter) {
      this.filter = filter;
      return this;
    }

    public AnyFieldRegexFilter build() {
      return new AnyFieldRegexFilter(this);
    }
  }

}
