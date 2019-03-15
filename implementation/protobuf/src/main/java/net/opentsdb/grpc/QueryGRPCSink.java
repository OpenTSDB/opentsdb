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
package net.opentsdb.grpc;

import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashCode;

import io.grpc.stub.StreamObserver;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.pbuf.QueryResultPB.QueryResult;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkCallback;
import net.opentsdb.query.QuerySinkConfig;
import net.opentsdb.query.serdes.PBufSerdes;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.utils.JSON;

/**
 * A sink for serializing results to PRotobuf for transmission over 
 * GRPC.
 * 
 * @since 3.0
 */
public class QueryGRPCSink implements QuerySink {
  private static final Logger LOG = LoggerFactory.getLogger(QueryGRPCSink.class);
  
  private final QueryContext context;
  private final GRPCSinkConfig config;
  private final PBufSerdes serdes;
  
  /**
   * Default ctor.
   * @param context The non-null context.
   * @param config The non-null config.
   */
  public QueryGRPCSink(final QueryContext context, 
                       final QuerySinkConfig config) {
    this.context = context;
    this.config = (GRPCSinkConfig) config;
    
    final SerdesFactory factory = context.tsdb().getRegistry()
        .getPlugin(SerdesFactory.class, config.serdesOptions().getType());
    if (factory == null) {
      throw new IllegalArgumentException("Unable to find a serdes "
          + "factory for the type: " + config.serdesOptions().getType());
    }
    TimeSeriesSerdes serdes = factory.newInstance(context, config.serdesOptions(), 
        (OutputStream) null);
    if (serdes == null) {
      throw new IllegalArgumentException("Factory returned a null "
          + "instance for the type: " + config.serdesOptions().getType());
    }
    if (!(serdes instanceof PBufSerdes)) {
      throw new IllegalArgumentException("Serdes was an instance of: " 
          + serdes.getClass());
    }
    this.serdes = (PBufSerdes) serdes;
  }
  
  @Override
  public void onComplete() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Yay, all done!");
    }
    config.observer.onCompleted();
  }
  
  @Override
  public void onNext(final net.opentsdb.query.QueryResult next) {
    try {
      config.observer.onNext(serdes.serializeResult(next));
    } catch (Throwable t) {
      LOG.error("Failed to send query upstream: " 
          + JSON.serializeToString(context.query()), t);
      onError(t);
    }
  }
  
  @Override
  public void onNext(final PartialTimeSeries next,
                     final QuerySinkCallback callback) {
    callback.onError(next, new IllegalStateException("Not implemented yet."));
  }

  @Override
  public void onError(final Throwable t) {
    LOG.error("Error executing query: " 
        + JSON.serializeToString(context.query()), t);
    try {
      config.observer.onError(t);
    } catch (Exception e) {
      LOG.error("Failed to send error upstream.", e);
    }
  }
  
  static class GRPCSinkConfig implements QuerySinkConfig {

    private final StreamObserver<QueryResult> observer;
    private final SerdesOptions options;
    
    private GRPCSinkConfig(final Builder builder) {
      observer = builder.observer;
      options = builder.options;
    }
    
    @Override
    public String getId() {
      return QueryGRPCSinkFactory.TYPE;
    }

    @Override
    public HashCode buildHashCode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public SerdesOptions serdesOptions() {
      return options;
    }
    
    public static Builder newBuilder() {
      return new Builder();
    }
    
    public static class Builder {
      private StreamObserver<QueryResult> observer;
      private SerdesOptions options;
      
      public Builder setObserver(final StreamObserver<QueryResult> observer) {
        this.observer = observer;
        return this;
      }
      
      public Builder setOptions(final SerdesOptions options) {
        this.options = options;
        return this;
      }
      
      public GRPCSinkConfig build() {
        return new GRPCSinkConfig(this);
      }
    }
  }
}