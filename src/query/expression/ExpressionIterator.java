// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.expression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.opentsdb.core.FillPolicy;
import net.opentsdb.query.expression.VariableIterator.SetOperator;
import net.opentsdb.utils.ByteSet;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.scripting.JexlScriptEngineFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

/**
 * A iterator that applies an expression to the results of multiple sub queries.
 * To use this class:
 * - Instantiate with a valid expression
 * - Call {@link #getVariableNames()} and iterate over a set of TSSubQueries and
 *   their results. For each query that matches a variable name, call
 *   {@link #addResults(String, ITimeSyncedIterator)} with the result set.
 * - Call {@link #compile()} to setup the meta data, fills and compute the
 *   intersection of the series.
 * - Call {@link #values()} and store the reference. Results for each
 *   series will be written here as you iterate.
 * - Call {@link #hasNext()} and {@link #next()} to iterate over results.
 * - At each iteration, fetch the timestamp and value from the data points array.
 * <p>
 * Iteration is performed across all series supplied to the iterator, synchronizing
 * on the timestamps and substituting fill values where appropriate.
 * <p>
 * WARNING: You MUST supply a result set and associated sub query to match each
 * of the variable names in the expression. If you fail to do so, when you call
 * {@link #compile()} you'll get an exception.
 * <p>
 * NOTE: Right now this class only supports intersection on the series so that
 * each metric result must contain series with the same tags based on the flags
 * provided in the ctor.
 * NOTE: If a result set doesn't include a fill policy, we default to ZERO for
 * "missing" data points.
 */
public class ExpressionIterator implements ITimeSyncedIterator {
  private static final Logger LOG = LoggerFactory.getLogger(ExpressionIterator.class);
  
  /** This is only here to to force the shade plugin to include the class in the fat-jar */
  private static final JexlScriptEngineFactory JEXL_FACTORY = null;
  
  /** Docs don't say whether this is thread safe or not. SOME methods are marked
   * as not thread safe, so I assume it's ok to instantiate one of these guys
   * and keep creating scripts from it.
   */
  public final static JexlEngine JEXL_ENGINE = new JexlEngine();
  
  /** Whether or not to intersect on the query tagks instead of the result set
   * tagks */
  private final boolean intersect_on_query_tagks;
  
  /** Whether or not to include the aggregated tags in the result set */
  private final boolean include_agg_tags;
  
  /** List of iterators and their IDs */
  private final Map<String, ITimeSyncedIterator> results;
  
  /** The compiled expression */
  private final Script expression;
  
  /** The context where we'll dump results for processing through the expression */
  private final JexlContext context = new MapContext();
  
  /** A list of unique variable names pulled from the expression */
  private final Set<String> names;
  
  /** The intersection iterator we'll use for processing */
  // TODO - write an interface to allow other set operators, e.g. union, disjoint
  private VariableIterator iterator;

  /** A map of results from the intersection iterator to pass to the expression */
  private Map<String, ExpressionDataPoint[]> iteration_results;
  
  /** The results of processing the expressions */
  private ExpressionDataPoint[] dps;
  
  /** The ID of this iterator */
  private final String id;
  
  /** The index of this iterator in expressions */
  private int index;
  
  /** A fill policy for this expression if data is missing */
  private NumericFillPolicy fill_policy;
  
  /** The set operator to use for joining sets */
  private SetOperator set_operator;
  
  // NOTE - if the query is set to NONE for the aggregation and the query has
  // no tagk filters then we shouldn't set the II's intersect_on_query_tagks
  /**
   * Default Ctor that compiles the expression for use with this iterator.
   * @param id The id of this iterator.
   * @param expression The expression to compile and use
   * @param set_operator The type of set operator to use
   * @param intersect_on_query_tagks Whether or not to include only the query 
   * specified tags during intersection
   * @param include_agg_tags Whether or not to include aggregated tags during
   * intersection
   * @throws IllegalArgumentException if the expression is null or empty or doesn't
   * contain any variables. 
   * @throws JexlException if the expression isn't valid
   */
  public ExpressionIterator(final String id, final String expression, 
      final SetOperator set_operator,
      final boolean intersect_on_query_tagks, final boolean include_agg_tags) {
    if (expression == null || expression.isEmpty()) {
      throw new IllegalArgumentException("The expression cannot be  null");
    }
    if (set_operator == null) {
      throw new IllegalArgumentException("The set operator cannot be null");
    }
    this.id = id;
    this.intersect_on_query_tagks = intersect_on_query_tagks;
    this.include_agg_tags = include_agg_tags;
    results = new HashMap<String, ITimeSyncedIterator>();
    this.expression = JEXL_ENGINE.createScript(expression);
    names = new HashSet<String>();
    extractVariableNames();
    if (names.size() < 1) {
      throw new IllegalArgumentException(
          "The expression didn't appear to have any variables");
    }
    this.set_operator = set_operator;
    fill_policy = new NumericFillPolicy(FillPolicy.NOT_A_NUMBER);
  }
  
  /**
   * Copy constructor that setups up a dupe of this iterator with fresh sub
   * iterator objects for use in a nested expression.
   * @param iterator The expression to copy from.
   */
  private ExpressionIterator(final ExpressionIterator iterator) {
    id = iterator.id;
    // need to recompile, don't know if we'll run into threading issues
    expression = JEXL_ENGINE.createScript(iterator.expression.toString());
    intersect_on_query_tagks = iterator.intersect_on_query_tagks;
    include_agg_tags = iterator.include_agg_tags;
    set_operator = iterator.set_operator;
    
    results = new HashMap<String, ITimeSyncedIterator>();
    for (Entry<String, ITimeSyncedIterator> entry : iterator.results.entrySet()) {
      results.put(entry.getKey(), entry.getValue().getCopy());
    }
    
    names = new HashSet<String>();
    extractVariableNames();
    if (names.size() < 1) {
      throw new IllegalArgumentException(
          "The expression didn't appear to have any variables");
    }
  }
  
  @Override
  public String toString() {
    final StringBuffer buf = new StringBuffer();
    buf.append("ExpressionIterator(id=")
       .append(id)
       .append(", expression=\"")
       .append(expression.toString())
       .append(", setOperator=")
       .append(set_operator)
       .append(", fillPolicy=")
       .append(fill_policy)
       .append(", intersectOnQueryTagks=")
       .append(intersect_on_query_tagks)
       .append(", includeAggTags=")
       .append(include_agg_tags)
       .append(", index=")
       .append(index)
       .append("\", VariableIterator=")
       .append(iterator)
       .append(", dps=")
       .append(dps)
       .append(", results=")
       .append(results)
       .append(")");
    return buf.toString();
  }
  
  /**
   * Adds a sub query result object to the iterator.
   * TODO - accept a proper object, not a map
   * @param id The ID of source iterator.
   * @param iterator The source iterator. 
   * @throws IllegalArgumentException if the object is missing required data
   */
  public void addResults(final String id, final ITimeSyncedIterator iterator) {
    if (id == null) {
      throw new IllegalArgumentException("Missing ID");
    }
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator cannot be null");
    }
    results.put(id, iterator);
  }
  
  /**
   * Builds the iterator by computing the intersection of all series in all sets
   * and sets up the output.
   * @throws IllegalArgumentException if there aren't any results, or we don't 
   * have a result for each variable, or something else is wrong.
   * @throws IllegalDataException if no series were left after computing the
   * intersection.
   */
  public void compile() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Compiling " + this);
    }
    if (results.size() < 1) {
      throw new IllegalArgumentException("No results for any variables in "
          + "the expression: " + this);
    }
    if (results.size() < names.size()) {
      throw new IllegalArgumentException("Not enough query results [" 
          + results.size() + " total results found] for the expression variables [" 
          + names.size() + " expected] " + this);
    }
    
    // don't care if we have extra results, but we had darned well better make
    // sure we have a result set for each variable    
    for (final String variable : names) {
      // validation
      final ITimeSyncedIterator it = results.get(variable.toLowerCase());
      if (it == null) {
        throw new IllegalArgumentException("Missing results for variable " + variable);
      }

      if (it instanceof ExpressionIterator) {
        ((ExpressionIterator)it).compile();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Matched variable " + variable + " to " + it);
      }
    }
    
    // TODO implement other set functions
    switch (set_operator) {
    case INTERSECTION:
      iterator = new IntersectionIterator(id, results, intersect_on_query_tagks, 
          include_agg_tags);
      break;
    case UNION:
      iterator = new UnionIterator(id, results, intersect_on_query_tagks, 
          include_agg_tags);
    }
    iteration_results = iterator.getResults();
    
    dps = new ExpressionDataPoint[iterator.getSeriesSize()];
    for (int i = 0; i < iterator.getSeriesSize(); i++) {
      final Iterator<Entry<String, ExpressionDataPoint[]>> it = 
          iteration_results.entrySet().iterator();
      Entry<String, ExpressionDataPoint[]> entry = it.next();
      
      if (entry.getValue() == null || entry.getValue()[i] == null) {
        dps[i] = new ExpressionDataPoint();
      } else {
        dps[i] = new ExpressionDataPoint(entry.getValue()[i]);
      }
      while (it.hasNext()) {
        entry = it.next();
        if (entry.getValue() != null && entry.getValue()[i] != null) {
          dps[i].add(entry.getValue()[i]);
        }
      }
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished compiling " + this);
    }
  }

  /**
   * Checks to see if we have another value in any of the series.
   * Make sure to call {@link #compile()} first.
   * @return True if there is more data to process, false if not
   */
  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }
  
  /**
   * Fetches the next set of data and computes a value for the expression.
   * Make sure to call {@link #compile()} first.
   * And make sure to call {@link #hasNext()} before calling this.
   * @return A link to the data points for this result set
   * @throws IllegalDataException if there wasn't any data left in any of the
   * series.
   * @throws JexlException if something went pear shaped processing the expression
   */
  public ExpressionDataPoint[] next(final long timestamp) {
    
    // fetch the timestamp ONCE to save some cycles.
    // final long timestamp = iterator.nextTimestamp();
    iterator.next();
    
    // set aside a couple of addresses for the variables
    double val;
    double result;
    for (int i = 0; i < iterator.getSeriesSize(); i++) {
      // this here is why life sucks. there MUST be a better way to bind variables
      for (final String variable : names) {
        if (iteration_results.get(variable)[i] == null) {
          context.set(variable, results.get(variable).getFillPolicy().getValue());
        } else {
          val = iteration_results.get(variable)[i].toDouble();
          if (Double.isNaN(val)) {
            context.set(variable, results.get(variable).getFillPolicy().getValue());
          } else {
            context.set(variable, val);
          }
        }
      }
      final Object output = expression.execute(context);
      if (output instanceof Double) {
        result = (Double) expression.execute(context);
      } else if (output instanceof Boolean) {
        result = (((Boolean) expression.execute(context)) ? 1 : 0);
      } else {
        throw new IllegalStateException("Expression returned a result of type: " 
            + output.getClass().getName() + " for " + this);
      }
      dps[i].reset(timestamp, result);
    }
    return dps;
  }
  
  /** @return a list of expression results. You can keep this list and check the 
   * results on each call to {@link #next()} */
  @Override
  public ExpressionDataPoint[] values() {
    return dps;
  }
  
  /**
   * Pulls the variable names from the expression and stores them in {@link #names}
   */
  private void extractVariableNames() {
    if (expression == null) {
      throw new IllegalArgumentException("The expression was null");
    }

    for (final List<String> exp_list : JEXL_ENGINE.getVariables(expression)) {
      for (final String variable : exp_list) {
        names.add(variable);
      }
    }
  }

  /** @return an immutable set of the variable IDs used in the expression. Case
   * sensitive. */
  public Set<String> getVariableNames() {
    return ImmutableSet.copyOf(names);
  }
  
  public void setSetOperator(final SetOperator set_operator) {
    this.set_operator = set_operator;
  }
  
  @Override
  public long nextTimestamp() {
    return iterator.nextTimestamp();
  }

  @Override
  public int size() {
    return dps.length;
  }

  @Override
  public void nullIterator(int index) {
    if (index < 0 || index >= dps.length) {
      throw new IllegalArgumentException("Index out of bounds");
    }
    // TODO - do it
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public void setIndex(int index) {
    this.index = index;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public ByteSet getQueryTagKs() {
    return null;
  }

  @Override
  public void setFillPolicy(NumericFillPolicy policy) {
    fill_policy = policy;
  }

  @Override
  public NumericFillPolicy getFillPolicy() {
    return fill_policy;
  }

  @Override
  public ITimeSyncedIterator getCopy() {
    final ExpressionIterator ei = new ExpressionIterator(this);
    return ei;
  }

  @Override
  public boolean hasNext(final int i) {
    return iterator.hasNext(i);
  }
  
  @Override
  public void next(final int i) {
    iterator.next(i);
    
    // set aside a couple of addresses for the variables
    double val;
    double result;
    // this here is why life sucks. there MUST be a better way to bind variables
    long ts = Long.MAX_VALUE;
    for (final String variable : names) {
      if (iteration_results.get(variable)[i] == null) {
        context.set(variable, results.get(variable).getFillPolicy().getValue());
      } else {
        if (iteration_results.get(variable)[i].timestamp() < ts) {
          ts = iteration_results.get(variable)[i].timestamp();
        }
        val = iteration_results.get(variable)[i].toDouble();
        if (Double.isNaN(val)) {
          context.set(variable, results.get(variable).getFillPolicy().getValue());
        } else {
          context.set(variable, val);
        }
      }
    }
    result = (Double)expression.execute(context);
    dps[i].reset(ts, result);
  }
  
}
