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
package net.opentsdb.query.processor;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.MultiQueryNodeFactory;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * A factory for generating {@link QueryNode}s that perform some kind of
 * data manipulation, hence the need to instantiate new iterators to work
 * over various types of data.
 * 
 * @since 3.0
 */
public abstract class BaseMultiQueryNodeFactory 
    implements MultiQueryNodeFactory, TSDBPlugin {
  private final Logger LOG = LoggerFactory.getLogger(getClass());
  
  /** The ID of this node factory. */
  protected final String id;
  
  /** The map of iterator factories keyed on type. */
  protected final Map<TypeToken<?>, QueryIteratorFactory> iterator_factories;
  
  /**
   * Default ctor.
   * @param id A non-null and non-empty ID.
   */
  public BaseMultiQueryNodeFactory(final String id) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    this.id = id;
    iterator_factories = Maps.newHashMapWithExpectedSize(3);
  }
  
  @Override
  public String id() {
    return id;
  }
  
  /**
   * @return The types of data this factory can instantiate iterators for.
   */
  public Collection<TypeToken<?>> types() {
    return iterator_factories.keySet();
  }
  
  /**
   * Registers a specific iterator factory for a given type.
   * @param type A non-null type.
   * @param factory A non-null factory for the type.
   */
  public void registerIteratorFactory(final TypeToken<?> type,
                                      final QueryIteratorFactory factory) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (iterator_factories.containsKey(type)) {
      LOG.warn("Replacing existing iterator factory: " + 
          iterator_factories.get(type) + " with factory: " + factory);
    }
    iterator_factories.put(type, factory);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registering iteratator factory: " + factory 
          + " with type: " + type);
    }
  }

  /**
   * Returns an instantiated iterator of the given type if supported
   * @param type A non-null type.
   * @param node The parent node.
   * @param result The result this source is a part of.
   * @param sources A collection of sources to incorporate in the iterator.
   * @return A non-null iterator if successful or null if an iterator is
   * not present for the type.
   */
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final TypeToken<?> type,
      final QueryNode node,
      final QueryResult result,
      final Collection<TimeSeries> sources) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (sources == null || sources.isEmpty()) {
      throw new IllegalArgumentException("Sources cannot be null or empty.");
    }
    
    final QueryIteratorFactory factory = iterator_factories.get(type);
    if (factory == null) {
      return null;
    }
    return factory.newIterator(node, result, sources);
  }

  /**
   * Returns an instantiated iterator of the given type if supported
   * @param type A non-null type.
   * @param node The parent node.
   * @param result The result this source is a part of.
   * @param sources A map of sources to incorporate in the iterator.
   * @return A non-null iterator if successful or null if an iterator is
   * not present for the type.
   */
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final TypeToken<?> type,
      final QueryNode node,
      final QueryResult result,
      final Map<String, TimeSeries> sources) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (sources == null || sources.isEmpty()) {
      throw new IllegalArgumentException("Sources cannot be null or empty.");
    }
    
    final QueryIteratorFactory factory = iterator_factories.get(type);
    if (factory == null) {
      return null;
    }
    return factory.newIterator(node, result, sources);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }
  
  @Override
  public String version() {
    return "3.0.0";
  }

}
