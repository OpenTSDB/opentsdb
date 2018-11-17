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
package net.opentsdb.query;

public class SemanticQueryContext extends BaseQueryContext {
  
  protected SemanticQueryContext(final Builder builder) {
    super((BaseQueryContext.Builder) builder);
    pipeline = new LocalPipeline(this, builder.sinks);
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder extends BaseQueryContext.Builder {
   
    @Override
    public QueryContext build() {
      return (QueryContext) new SemanticQueryContext(this);
    }
    
  }

}
