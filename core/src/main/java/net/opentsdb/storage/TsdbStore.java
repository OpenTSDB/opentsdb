// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;

import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.opentsdb.uid.TimeseriesId;
import net.opentsdb.uid.UniqueIdType;

/**
 * A interface defining the functions any database used with TSDB must implement.
 * Another requirement is tha the database connection has to be asynchronous.
 */
public interface TsdbStore {
  //
  // Identifier management
  //
  public Deferred<byte[]> allocateUID(final String name,
                                      final UniqueIdType type);

  Deferred<byte[]> allocateUID(final String name,
                               final byte[] uid,
                               final UniqueIdType type);

  Deferred<Object> deleteUID(byte[] name, UniqueIdType type);

  /**
   * Lookup time series related to a metric, tagk, tagv or any combination
   * thereof. See {@link net.opentsdb.core.UniqueIdClient#executeTimeSeriesQuery}
   * for a more formal specification how the query language and logic.
   *
   * @param query The query that filters out which TSUIDs to lookup
   * @return All TSUIDs that matches the provided query
   */
  Deferred<List<byte[]>> executeTimeSeriesQuery(final ResolvedSearchQuery query);

  /**
   * Lookup all IDs that matches the provided {@link net.opentsdb.uid.IdQuery}.
   * There are no demands on how the exact the results are but the lookup should
   * be efficient. In fact, the provided should be viewed as a hint about what
   * should be returned but in reality all IDs or nothing at all may be
   * returned.
   *
   * @param query An object that describes the query parameters
   * @return A deferred with a list of matching IDs
   */
  Deferred<List<IdentifierDecorator>> executeIdQuery(final IdQuery query);

  public Deferred<Optional<byte[]>> getId(final String name, final UniqueIdType type);

  public Deferred<Optional<String>> getName(final byte[] id, final UniqueIdType type);

  //
  // Datapoints
  //
  Deferred<Object> addPoint(final TimeseriesId tsuid,
                            final long timestamp,
                            final float value);

  Deferred<Object> addPoint(final TimeseriesId tsuid,
                            final long timestamp,
                            final double value);

  Deferred<Object> addPoint(final TimeseriesId tsuid,
                            final long timestamp,
                            final long value);

  /**
   * Should execute the provided {@link net.opentsdb.core.Query} and
   * return a deferred. Every single item in
   * the returned iterator may contain multiple datapoints but every single
   * instance must only contain the datapoints for a single TSUID. The
   * iterator may return multiple items for the same TSUID.
   * @param query The query to execute
   */
  Deferred<ImmutableList<DataPoints>> executeQuery(final Query query);

  Deferred<Map<byte[], Long>> getLastWriteTimes(final ResolvedSearchQuery query);

  //
  // Annotations
  //
  /**
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annoation does not exist in storage, this delete call will not throw an
   * error.
   *
   * @param annotation@return A meaningless Deferred for the caller to wait on until the call is
   * complete. The value may be null.
   */
  Deferred<Object> delete(Annotation annotation);

  Deferred<Integer> deleteAnnotationRange(final byte[] tsuid, final long start_time, final long end_time);

  /**
   * Attempts to fetch a global or local annotation from storage
   * @param tsuid The TSUID as a byte array. May be null if retrieving a global
   * annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  Deferred<Annotation> getAnnotation(byte[] tsuid, long start_time);

  Deferred<List<Annotation>> getGlobalAnnotations(final long start_time, final long end_time);

  Deferred<Boolean> updateAnnotation(Annotation original, Annotation annotation);

  //
  // UIDMeta
  //
  public Deferred<Object> add(final UIDMeta meta);

  Deferred<Object> delete(UIDMeta meta);

  public Deferred<UIDMeta> getMeta(byte[] uid, String name,
                                   UniqueIdType type);

  public Deferred<Boolean> updateMeta(final UIDMeta meta,
                                      final boolean overwrite);

  //
  // TSMeta
  //
  Deferred<Boolean> TSMetaCounterExists(final byte[] tsuid);

  Deferred<Boolean> TSMetaExists(final String tsuid);

  Deferred<Boolean> create(final TSMeta tsMeta);

  Deferred<Object> delete(final TSMeta tsMeta);

  Deferred<Object> deleteTimeseriesCounter(final TSMeta ts);

  Deferred<TSMeta> getTSMeta(final byte[] tsuid);

  Deferred<Long> incrementAndGetCounter(final byte[] tsuid);

  Deferred<Object> setTSMetaCounter(final byte[] tsuid, final long number);

  Deferred<Boolean> syncToStorage(final TSMeta tsMeta, final Deferred<ArrayList<Object>> uid_group, final boolean overwrite);

  Deferred<List<TSMeta>> executeTimeseriesMetaQuery(ResolvedSearchQuery query);

  //
  // Misc
  //
  public Deferred<Object> shutdown();
}
