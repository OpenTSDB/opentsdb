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
package net.opentsdb.query.processor.expressions;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.processor.Joiner;
import net.opentsdb.query.processor.TimeSeriesProcessor;
import net.opentsdb.query.processor.TimeSeriesProcessorConfig;
import net.opentsdb.utils.Deferreds;

/**
 * An expression processor that allows for flexible custom mutations of 
 * time series.
 * TODO - more docs and work on this.
 * 
 * @since 3.0
 */
public class JexlBinderProcessor extends TimeSeriesProcessor {
  
  /** A holding location for iterators as we compile from one or more sources. */
  private IteratorGroups source_iterators;
  
  /** A list of source processors to read from after initialization. */
  private final List<TimeSeriesProcessor> source_processors;
  
  /**
   * Default Ctor used by the registry.
   * @param context A context to associate the processor with.
   * @param config A required expression config.
   * @throws IllegalArgumentException if the config was null.
   */
  public JexlBinderProcessor(final QueryContext context,
      final TimeSeriesProcessorConfig<JexlBinderProcessor> config) {
    super(context, config);
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null for "
          + "expression processors.");
    }
    source_iterators = new DefaultIteratorGroups();
    source_processors = Lists.newArrayListWithExpectedSize(1);
  }
  
  @Override
  public Deferred<Object> initialize() {
    final List<TimeSeriesIterator<?>> its = source_iterators.flattenedIterators();
    if (its.isEmpty() && source_processors.isEmpty()) {
      return Deferred.fromError(new IllegalStateException("No sources were "
          + "provided for the expression processor."));
    }
    
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithExpectedSize(its.size());

    for (final TimeSeriesIterator<?> it : its) {
      deferreds.add(it.initialize());
    }
    
    class ErrorCB implements Callback<Object, Exception> {
      @Override
      public Object call(Exception e) throws Exception {
        init_deferred.callback(e);
        return e;
      }
      @Override
      public String toString() {
        return "JexlBinderProcessor Init Errorback.";
      }
    }

    class InitCB implements Callback<Deferred<Object>, Object> {
      @Override
      public Deferred<Object> call(final Object result_or_exception) throws Exception {
        if (result_or_exception instanceof Exception) {
          init_deferred.callback((Exception) result_or_exception);
          return init_deferred;
        }
        
        final IteratorGroups iterators_to_join = new DefaultIteratorGroups();
        try {
          for (final Entry<TimeSeriesGroupId, IteratorGroup> group : 
              source_iterators) {
            iterators_to_join.addGroup(group.getValue());
          }
          // add the processor iterators now that they've been initialized
          for (final TimeSeriesProcessor processor : source_processors) {
            for (final Entry<TimeSeriesGroupId, IteratorGroup> group : 
                processor.iterators()) {
              iterators_to_join.addGroup(group.getValue());
            }
          }
          
          final TimeSeriesGroupId id = new SimpleStringGroupId(
              ((ExpressionProcessorConfig) config).getExpression().getId());
          
          final Joiner joiner = new Joiner((ExpressionProcessorConfig) config);        
          final Map<String, IteratorGroups> joins = joiner.join(iterators_to_join);
          
          final List<Deferred<Object>> deferreds = 
              Lists.newArrayListWithExpectedSize(joins.size());
          for (final IteratorGroups join : joins.values()) {
            final JexlBinderNumericIterator binder_iterator =
                new JexlBinderNumericIterator(context, 
                    (ExpressionProcessorConfig) config);
            int numerics = 0;
            
            for (final Entry<TimeSeriesGroupId, IteratorGroup> group : join) {
              for (final TimeSeriesIterator<?> it : 
                  group.getValue().flattenedIterators()) {
                if (it.type().equals(NumericType.TYPE)) {
                  binder_iterator.addIterator(group.getKey().id(), it);
                  ++numerics;
                }
              }
            }
            
            if (numerics > 0) {
              binder_iterator.setId();
              iterators.addIterator(id, binder_iterator);
              deferreds.add(binder_iterator.initialize());
            }
          }
          
          Deferred.group(deferreds)
            .addCallback(new Deferreds.NullGroupCB(init_deferred))
            .addErrback(new ErrorCB());
        } catch (Exception e) {
          init_deferred.callback(e);
        }
        return init_deferred;
      }
      @Override
      public String toString() {
        return "JexlBinderProcessor Init Callback.";
      }
    }
    
    Deferred.group(deferreds)
      .addCallback(Deferreds.NULL_GROUP_CB)
      .addBoth(new InitCB());
    return init_deferred;
  }

  @Override
  public void addSeries(final TimeSeriesGroupId group, 
      final TimeSeriesIterator<?> series) {
    if (group == null) {
      throw new IllegalArgumentException("Group ID cannot be null.");
    }
    if (series == null) {
      throw new IllegalArgumentException("Series cannot be null.");
    }
    source_iterators.addIterator(group, series);
    series.setContext(context);
  }
  
  /**
   * Adds the processor's iterators to the local iterator set to be passed to
   * the joiner in the {@link #initialize()} phase.
   * @param processor A non-null processor.
   * @throws IllegalArgumentException if the processor was null or was the
   * same processor as {@code this}.
   */
  public void addProcessor(final TimeSeriesProcessor processor) {
    if (processor == null) {
      throw new IllegalArgumentException("Processor source cannot be null.");
    }
    if (processor == this) {
      throw new IllegalArgumentException("Cannot add a processor to itself.");
    }
    source_processors.add(processor);
    if (context != null) {
      context.register(this, processor);
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public TimeSeriesProcessor getClone(final QueryContext context) {
    final JexlBinderProcessor clone = new JexlBinderProcessor(context, 
        (TimeSeriesProcessorConfig<JexlBinderProcessor>) config);
    clone.source_iterators = source_iterators.getCopy(context);
    for (final TimeSeriesProcessor processor : source_processors) {
      clone.addProcessor(processor.getClone(context));
    }
    return clone;
  }
}
