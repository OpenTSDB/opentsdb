// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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
package net.opentsdb.query.execution.serdes;

import com.fasterxml.jackson.annotation.JsonProperty;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.serdes.SerdesOptions;

/**
 * A base serdes option class.
 * 
 * @since 3.0
 */
public class BaseSerdesOptions implements SerdesOptions {
  /** The start timestamp for serialization. */
  protected TimeStamp start;
  
  /** The end timestamp for serialization. */
  protected TimeStamp end;
  
  /**
   * Default ctor.
   * @param builder Non-null builder.
   */
  protected BaseSerdesOptions(final Builder builder) {
    if (builder.start == null) {
      throw new IllegalArgumentException("Start timestamp cannot be null.");
    }
    if (builder.end == null) {
      throw new IllegalArgumentException("End timestamp cannot be null.");
    }
    start = builder.start;
    end = builder.end;
  }
  
  @Override
  public TimeStamp start() {
    return start;
  }
  
  @Override
  public TimeStamp end() {
    return end;
  }
  
  /** @return A new builder. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    @JsonProperty
    private TimeStamp start;
    @JsonProperty
    private TimeStamp end;
    
    /**
     * @param start A non-null inclusive start timestamp.
     * @return The builder.
     */
    public Builder setStart(final TimeStamp start) {
      this.start = start;
      return this;
    }
    
    /**
     * @param end A non-null inclusive end timestamp.
     * @return The builder.
     */
    public Builder setEnd(final TimeStamp end) {
      this.end = end;
      return this;
    }
    
    /** @return A constructed serdes object. May throw exceptions. */
    public SerdesOptions build() {
      return new BaseSerdesOptions(this);
    }
  }
}
