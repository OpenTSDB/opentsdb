// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryContext;

/**
 * A time series data processor that iterates over one or more time series
 * synchronized on timestamps. The processor can either pass through the 
 * source series or apply mutations on top of the data and return the 
 * modifications without modifying the source.
 * <p>
 * Processors work across all data types. If a type is not supported it may be
 * passed through the the upstream processor.
 * <p>
 * Processors may also perform multiple passes across the source data (e.g. for
 * standard deviation calculations). In such a case, the underlying iterators 
 * should support buffering of the data to improve query speed.
 * TODO - more docs
 * 
 * @since 3.0
 */
public abstract class TimeSeriesProcessor {
  private static final Logger LOG = 
      LoggerFactory.getLogger(TimeSeriesProcessor.class);
  
  /** An optional config for the implementing processor. */
  protected final TimeSeriesProcessorConfig<?> config;
  
  /** The local group of iterators, likely extending another set. */
  protected IteratorGroups iterators;
  
  /** A query context this processor is associated with. */
  protected QueryContext context;
  
  /** The deferred called on initialization. */
  protected Deferred<Object> init_deferred;
  
  /**
   * Default ctor initializes the initialization deferred and iterators.
   */
  public TimeSeriesProcessor() {
    this(null, null);
  }
  
  /**
   * Initializes the initialization deferred and iterators. Also registers
   * with a context.
   * @param context A context to register with.
   */
  public TimeSeriesProcessor(final QueryContext context) {
    this(context, null);
  }
  
  /**
   * Initializes the initialization deferred and iterators and sets the config.
   * @param config A config to associate with this processor.
   */
  public TimeSeriesProcessor(final TimeSeriesProcessorConfig<?> config) {
    this(null, config);
  }
  
  /**
   * Initializes the initialization deferred and iterators. Sets the config and
   * registers with the context.
   * @param context A query context to associate with.
   * @param config A config for this processor.
   */
  public TimeSeriesProcessor(final QueryContext context, 
      final TimeSeriesProcessorConfig<?> config) {
    this.config = config;
    iterators = new DefaultIteratorGroups();
    init_deferred = new Deferred<Object>();
    setContext(context);
  }
  
  /** @return The initialization deferred object. */
  public Deferred<Object> initializationDeferred() {
    return init_deferred;
  }
  
  /**
   * Initializes the processor if initialization is required. If a processor 
   * requires multiple passes over the data set, it may copy the source and run
   * through it one or more times. Otherwise this method should setup the output
   * iterators.
   * 
   * @return A Deferred resolving to a null on success or an exception if 
   * initialization failed.
   */
  public Deferred<Object> initialize() {
    try {
      class InitCB implements Callback<Object, Object> {
        @Override
        public Object call(final Object arg) throws Exception {
          init_deferred.callback(null);
          return null;
        }
      }
      
      class ErrCB implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception ex) throws Exception {
          init_deferred.callback(ex);
          return null;
        }
      }
      
      iterators.initialize()
        .addCallback(new InitCB())
        .addErrback(new ErrCB());
    } catch (Exception e) {
      init_deferred.callback(e);
      return init_deferred;
    }
    return init_deferred;
  }
  
  /**
   * The set of iterators for use upstream by another processor or a sink.
   * @return A non-null grouped iterator set.
   */
  public IteratorGroups iterators() {
    return iterators;
  }
  
  /**
   * Sets the context this processor is associated with.
   * @param context An optional context. May be null.
   */
  public void setContext(final QueryContext context) {
    if (this.context != null && this.context != context) {
      this.context.unregister(this);
    }
    this.context = context;
    if (this.context != null) {
      context.register(this);
    }
    iterators.setContext(context);
  }
  
  /**
   * Adds the given series to the iterator set with the proper group.
   * @param group A non-null time series group the series should be associated 
   * with.
   * @param series A non-null time series iterator that has already been 
   * initialized.
   */
  public void addSeries(final TimeSeriesGroupId group, 
      final TimeSeriesIterator<?> series) {
    if (group == null) {
      throw new IllegalArgumentException("Group ID cannot be null.");
    }
    if (series == null) {
      throw new IllegalArgumentException("Series cannot be null.");
    }
    iterators.addIterator(group, series);
    series.setContext(context);
    if (context != null) {
      context.register(series);
    }
  }

  /**
   * Creates and returns a deep copy of this processor and all sources/child 
   * iterators for another view on the time series.
   * <p>Requirements:
   * <ul>
   * <li>The copy must return a new view of the underlying data. If this method
   * was called in the middle of iteration, the copy must start at the top of
   * the beginning of the data and the original iterator left in it's current 
   * state.</li>
   * <li>If the source iterator has not been initialized, the copy will not
   * be initialized either. Likewise if the source <i>has</i> been initialized
   * then the copy will have been as well.</li>
   * </ul>
   * @param context A query context to associate the clone with.
   * @return A non-null copy of the processor and underlying iterators.
   */
  public abstract TimeSeriesProcessor getClone(final QueryContext context);
  
  /**
   * Closes and releases any resources held by this processor. If this processor
   * is a copy, the method is a no-op.
   * @return A deferred resolving to a null on success, an exception on failure.
   */
  public Deferred<Object> close() {
    return iterators.close();
  }
  
  /** @return A callback class that will execute {@link #initialize()} when triggered. */
  public Callback<Deferred<Object>, Object> initializationCallback() {
    class InitCB implements Callback<Deferred<Object>, Object> {
      @Override
      public Deferred<Object> call(final Object result_or_exception) throws Exception {
        if (result_or_exception instanceof Throwable) {
          init_deferred.callback((Exception) result_or_exception);
          return init_deferred;
        }
        return initialize();
      }
    }
    return new InitCB();
  }
}
