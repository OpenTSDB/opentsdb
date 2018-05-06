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

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.MergedTimeSeriesId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.NumericFillPolicy;
import net.opentsdb.utils.Deferreds;

/**
 * A sub iterator for numeric types that computes an expression result. One or 
 * more variables must be set via {@link #addIterator(String, TimeSeriesIterator)}.
 * 
 * @since 3.0
 */
public class JexlBinderNumericIterator extends 
    TimeSeriesIterator<NumericType> {
  private static final Logger LOG = LoggerFactory.getLogger(
      JexlBinderNumericIterator.class);
  
  /** Reference to the processor config. */
  private final ExpressionProcessorConfig config;
  
  /** The local jexl script. */
  private final Script script;
  
  /** The ID merger. */
  private final MergedTimeSeriesId.Builder merger;
  
  /** Map of variable name to iterators to run through the context. */ 
  private final Map<String, TimeSeriesIterator<?>> iterators;

  /** The data point we'll update and send upstream. */
  private MutableNumericValue dp;
  
  /** The context where we'll dump results for processing through the expression */
  // TODO - see if this can be shared
  private final JexlContext jexl_context = new MapContext();
  
  /**
   * Default ctor accepting a source and config.
   * @param context The query context this iterator belongs to.
   * @param config A non-null config to pull the expression from.
   * @throws IllegalArgumentException if the config was null.
   */
  public JexlBinderNumericIterator(
      final QueryContext context, 
      final ExpressionProcessorConfig config) {
    super();
    setContext(context);
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = config;
    script = Expression.JEXL_ENGINE.createScript(config.getExpression().getExpr());
    merger = MergedTimeSeriesId.newBuilder();
    iterators = Maps.newHashMapWithExpectedSize(
        config.getExpression().getVariables().size());
  }
  
  /**
   * Adds the iterator to the map with the proper variable name.
   * @param var A non-null variable name matching the expression variable set.
   * @param it A non-null time series to pull data from.
   */
  public void addIterator(final String var, final TimeSeriesIterator<?> it) {
    if (Strings.isNullOrEmpty(var)) {
      throw new IllegalArgumentException("Variable name cannot be null.");
    }
    if (it == null) {
      throw new IllegalArgumentException("Iterator source cannot be null.");
    }
    if (!config.getExpression().getVariables().contains(var)) {
      throw new IllegalArgumentException("Variable list did not contain the "
          + "variable: " + var);
    }
    if (iterators.containsKey(var)) {
      throw new IllegalArgumentException("Iterator already has a value for "
          + "variable: " + var);
    }
    iterators.put(var, it);
    if (context != null) {
      context.register(this, it);
    }
    merger.addSeries(it.id());
  }
  
  @Override
  public Deferred<Object> initialize() {
    // NOTE: We don't need to initialize the source iterators here as the 
    // JexlBinderProcessor has already done so.
    for (final String var : config.getExpression().getVariables()) {
      if (!iterators.containsKey(var)) {
        if (config.getExpression().getFillPolicies() == null ||
            !config.getExpression().getFillPolicies().containsKey(var)) {
          return Deferred.fromError(new IllegalStateException("Expression "
              + "iterator was missing an interator or fill for variable " + var));
        }
      }
    }
    if (id == null || dp == null) {
      setId();
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericType.TYPE;
  }

  @Override
  public IteratorStatus status() {
    if (context != null) {
      throw new IllegalStateException("Iterator is a part of a context.");
    }
    IteratorStatus status = IteratorStatus.END_OF_DATA;
    for (final TimeSeriesIterator<?> it : iterators.values()) {
      status = IteratorStatus.updateStatus(status, it.status());
    }
    return status;
  }
  
  @Override
  public TimeSeriesValue<NumericType> next() {
    try {
      for (final String variable : config.getExpression().getVariables()) {
        final TimeSeriesIterator<?> it = iterators.get(variable);
        if (it == null) {
          jexl_context.set(variable, getFillValue(variable));
        } else {
          @SuppressWarnings("unchecked")
          final TimeSeriesValue<NumericType> value = 
             (TimeSeriesValue<NumericType>) it.next();
          if (value == null) {
            LOG.error("Iterator " + it + " returned a null value.");
            if (context != null) {
              context.updateContext(IteratorStatus.EXCEPTION, null);
            }
            return null;
          }
          if (!value.value().isInteger() && 
              !Double.isFinite(value.value().doubleValue())) {
            jexl_context.set(variable, getFillValue(variable));
          } else {
            jexl_context.set(variable, value.value().isInteger() ? 
                value.value().longValue() : value.value().doubleValue());
          }
        }
      }
      
      final Object output = script.execute(jexl_context);
      if (output instanceof Double) {
        if (Double.isNaN((Double) output) && 
            config.getExpression().getFillPolicy() != null) {
          // TODO - infectious nan
          dp.reset(context.syncTimestamp(), 
              config.getExpression().getFillPolicy().getValue());
        } else {
          dp.reset(context.syncTimestamp(), 
              (Double) output);
        }
      } else if (output instanceof Boolean) {
        dp.reset(context.syncTimestamp(), (((Boolean) output) ? 1 : 0));
      } else {
        throw new IllegalStateException("Expression returned a result of type: " 
            + output.getClass().getName() + " for " + this);
      }
      return dp;
    } catch (RuntimeException e) {
      if (context != null) {
        context.updateContext(IteratorStatus.EXCEPTION, null);
      }
      throw e;
    }
  }

  @Override
  public TimeSeriesId id() {
    return id;
  }
  
  @Override
  public TimeSeriesIterator<NumericType> getShallowCopy(final QueryContext context) {
    final JexlBinderNumericIterator copy = new JexlBinderNumericIterator(context, config);
    copy.id = id;
    for (final Entry<String, TimeSeriesIterator<?>> entry : iterators.entrySet()) {
      copy.addIterator(entry.getKey(), entry.getValue().getShallowCopy(context));
    }
    return copy;
  }

  @Override
  public TimeSeriesIterator<NumericType> getDeepCopy(final QueryContext context, 
                                                 final TimeStamp start, 
                                                 final TimeStamp end) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public Deferred<Object> fetchNext() {
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithExpectedSize(iterators.size());
    for (final TimeSeriesIterator<?> it : iterators.values()) {
      deferreds.add(it.fetchNext());
    }
    return Deferred.group(deferreds).addBoth(Deferreds.NULL_GROUP_CB);
  }
  
  @Override
  public void setContext(final QueryContext context) {
    if (this.context != null && this.context != context) {
      this.context.unregister(this);
    }
    this.context = context;
    if (context != null) {
      context.register(this);
      if (iterators != null) {
        for (final TimeSeriesIterator<?> it : iterators.values()) {
          context.register(this, it);
        }
      }
    }
  }

  @Override
  public TimeStamp startTime() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeStamp endTime() {
    // TODO Auto-generated method stub
    return null;
  }
  
  /**
   * Helper to determine what value to fill with from the config. Individual
   * variable fills take precedence, then the expression fill, and finally
   * it will return NaN.
   * @param variable The variable name to get a fill for.
   * @return A fill value or NaN if no fill was specified.
   */
  private double getFillValue(final String variable) {
    if (config.getExpression().getFillPolicies() != null) {
      final NumericFillPolicy fill = 
          config.getExpression().getFillPolicies().get(variable);
      if (fill != null) {
        return fill.getValue();
      }
    }
    
    if (config.getExpression().getFillPolicy() != null) {
      return config.getExpression().getFillPolicy().getValue();
    }
    
    return Double.NaN;
  }

  protected void setId() {
    id = merger.build();
    dp = new MutableNumericValue();
  }
  
  @Override
  public TimeSeriesValue<NumericType> peek() {
    // TODO fill this out
    return null;
  }
  
  @Override
  protected void updateContext() {
    // TODO Auto-generated method stub
    
  }
}
