// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric.aggregators;

/**
 * An abstract base for array aggregator configs.
 * 
 * @since 3.0
 */
public class BaseArrayAggregatorConfig implements NumericArrayAggregatorConfig {
  private final int array_size;
  private final boolean infectious_nan;
  
  protected BaseArrayAggregatorConfig(final Builder builder) {
    this.array_size = builder.array_size;
    this.infectious_nan = builder.infectious_nan;
  }
  
  @Override
  public boolean infectiousNan() {
    return infectious_nan;
  }
  
  @Override
  public int arraySize() {
    return array_size;
  }
  
  public static class Builder {
    private int array_size;
    private boolean infectious_nan;
    
    public Builder setArraySize(final int array_size) {
      this.array_size = array_size;
      return this;
    }
    
    public Builder setInfectiousNaN(final boolean infectious_nan) {
      this.infectious_nan = infectious_nan;
      return this;
    }
    
    public BaseArrayAggregatorConfig build() {
      return new BaseArrayAggregatorConfig(this);
    }
  }
}