// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.processor.expressions;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.NumericFillPolicy;

/**
 * A sub iterator for numeric types that computes an expression result. One or 
 * more variables must be set via {@link #addIterator(String, TimeSeriesIterator)}.
 * 
 * @since 3.0
 */
public class JexlBinderIterator extends 
  TimeSeriesIterator<NumericType> {
  
  /** Reference to the processor config. */
  private final JexlBinderProcessorConfig config;
  
  /** The local jexl script. */
  private final Script script;
  
  /** Map of variable name to iterators to run through the context. */ 
  private final Map<String, TimeSeriesIterator<?>> iterators;

  /** The data point we'll update and send upstream. */
  private MutableNumericType dp;
  
  /** The context where we'll dump results for processing through the expression */
  // TODO - see if this can be shared
  private final JexlContext jexl_context = new MapContext();
  
  /**
   * Default ctor accepting a source and config.
   * @param config A non-null config to pull the expression from.
   * @throws IllegalArgumentException if the config was null.
   */
  public JexlBinderIterator(final JexlBinderProcessorConfig config) {
    super(null);
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = config;
    script = Expression.JEXL_ENGINE.createScript(config.getExpression().getExpr());
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
  }
  
  @Override
  public Deferred<Object> initialize() {
    for (final String var : config.getExpression().getVariables()) {
      if (!iterators.containsKey(var)) {
        if (config.getExpression().getFillPolicies() == null ||
            !config.getExpression().getFillPolicies().containsKey(var)) {
          return Deferred.fromError(new IllegalStateException("Expression "
              + "iterator was missing an interator or fill for variable " + var));
        }
      }
    }
    // TODO - proper ID
    // NO!!
    dp = new MutableNumericType(iterators.values().iterator().next().id());
    return Deferred.fromResult(null);
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericType.TYPE;
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    int reals = 0;
    for (final Entry<String, TimeSeriesIterator<?>> it : iterators.entrySet()) {
      @SuppressWarnings("unchecked")
      final TimeSeriesValue<NumericType> value = 
          (TimeSeriesValue<NumericType>) it.getValue().next(); 
      if (!value.value().isInteger() && Double.isNaN(value.value().doubleValue()) &&
          config.getExpression().getFillPolicies() != null) {
        final NumericFillPolicy fill = config.getExpression().getFillPolicies()
            .get(it.getKey());
        if (fill != null) {
          jexl_context.set(it.getKey(), fill.getValue());
        } else {
          // TODO - default to something else?
          jexl_context.set(it.getKey(), Double.NaN);
        }
      } else {
        jexl_context.set(it.getKey(), value.value().isInteger() ? 
            value.value().longValue() : value.value().doubleValue());
        reals += value.realCount();
      }
    }
    
    final Object output = script.execute(jexl_context);
    if (output instanceof Double) {
      if (Double.isNaN((Double) output) && 
          config.getExpression().getFillPolicy() != null) {
        // TODO - infectious nan
        dp.reset(context.syncTimestamp(), 
            config.getExpression().getFillPolicy().getValue(),
            reals);
      } else {
        dp.reset(context.syncTimestamp(), 
            (Double) output, 
            reals);
      }
    } else if (output instanceof Boolean) {
      dp.reset(context.syncTimestamp(), (((Boolean) output) ? 1 : 0), reals);
    } else {
      throw new IllegalStateException("Expression returned a result of type: " 
          + output.getClass().getName() + " for " + this);
    }
    return dp;
  }

  @Override
  public TimeSeriesIterator<NumericType> getCopy(final QueryContext context) {
    final JexlBinderIterator copy = new JexlBinderIterator(config);
    for (final Entry<String, TimeSeriesIterator<?>> entry : iterators.entrySet()) {
      copy.addIterator(entry.getKey(), entry.getValue().getCopy(context));
    }
    return copy;
  }

}
