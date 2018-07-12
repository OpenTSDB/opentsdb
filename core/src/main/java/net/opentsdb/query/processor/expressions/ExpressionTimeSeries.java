//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package net.opentsdb.query.processor.expressions;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.BaseMultiQueryNodeFactory;

/**
 * A container class for computing a binary operation on one or two 
 * time series in an expression node graph.
 * 
 * @since 3.0
 */
public class ExpressionTimeSeries implements TimeSeries {

  /** Keys to populate the map. */
  public static final String LEFT_KEY = "L";
  public static final String RIGHT_KEY = "R";
  
  /** The parent node. */
  protected final BinaryExpressionNode node;
  
  /** The query result this series belongs to. */
  protected final QueryResult result;
  
  /** The left time series, may be null. */
  protected final TimeSeries left;
  
  /** The right time series, may be null. */
  protected final TimeSeries right;

  /** The set of types in this series. */
  protected final Set<TypeToken<?>> types;
  
  /** The new Id of the time series. */
  protected final TimeSeriesId id;
  
  /**
   * Package private ctor. Constructs the new ID from the joiner belonging
   * to the node.
   * @param node The non-null parent node.
   * @param result The non-null result this series belongs to.
   * @param left The optional left hand time series.
   * @param right The optional right hand time series.
   */
  ExpressionTimeSeries(final BinaryExpressionNode node,
                       final QueryResult result,
                       final TimeSeries left, 
                       final TimeSeries right) {
    if (left == null && right == null) {
      throw new IllegalArgumentException("At least one operand must "
          + "be non-null.");
    }
    this.node = node;
    this.result = result;
    this.left = left;
    this.right = right;
    
    types = Sets.newHashSetWithExpectedSize(1);
    if (left != null) {
      types.addAll(left.types());
    }
    if (right != null) {
      types.addAll(right.types());
    }
    id = node.joiner().joinIds(left, right, 
        ((ExpressionConfig) node.config()).getAs());
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
      final TypeToken<?> type) {
    if (!types.contains(type)) {
      return Optional.empty();
    }
    final ImmutableMap.Builder<String, TimeSeries> builder = 
        ImmutableMap.<String, TimeSeries>builder();
    if (left != null) {
      builder.put(LEFT_KEY, left);
    }
    if (right != null) {
      builder.put(RIGHT_KEY, right);
    }
    
    final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        ((BaseMultiQueryNodeFactory) node.factory()).newIterator(type, node, result, 
            (Map<String, TimeSeries>) builder.build());
    if (iterator == null) {
      return Optional.empty();  
    }
    return Optional.of(iterator);
  }

  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    final ImmutableMap.Builder<String, TimeSeries> builder = 
        ImmutableMap.<String, TimeSeries>builder();
    if (left != null) {
      builder.put(LEFT_KEY, left);
    }
    if (right != null) {
      builder.put(RIGHT_KEY, right);
    }
    final Map<String, TimeSeries> sources = (Map<String, TimeSeries>) builder.build();
    final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = 
        Lists.newArrayListWithExpectedSize(types.size());
    for (final TypeToken<?> type : types) {
      iterators.add(((BaseMultiQueryNodeFactory) node.factory()).newIterator(
          type, node, result, sources));
    }
    return iterators;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    return types;
  }

  @Override
  public void close() {
    
  }
}