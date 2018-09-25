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
package net.opentsdb.query.processor.dedup;

import com.google.common.hash.HashCode;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;

/**
 * A configuration for handling out-of-order and de-duplication of values 
 * in a time series stream when the data source would emit such values.
 * 
 * TODO - we want to add settings here for things like keep first vs 
 * last and/or aggregate.
 * 
 * @since 3.0
 */
public class DedupConfig extends BaseQueryNodeConfig {

  /**
   * Protected ctor.
   * @param builder Non-null builder.
   */
  protected DedupConfig(final Builder builder) {
    super(builder);
  }

  @Override
  public boolean pushDown() {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public boolean joins() {
    return false;
  }
  
  @Override
  public boolean equals(Object o) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int hashCode() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends BaseQueryNodeConfig.Builder {
    @Override
    public DedupConfig build() {
      return new DedupConfig(this);
    }
  }
}
