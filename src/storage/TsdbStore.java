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

import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;

import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import org.hbase.async.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.opentsdb.uid.UniqueIdType;

/**
 * A interface defining the functions any database used with TSDB must implement.
 * Another requirement is tha the database connection has to be asynchronous.
 */
public interface TsdbStore {

  /**
   * Attempts to fetch a global or local annotation from storage
   * @param tsuid The TSUID as a byte array. May be null if retrieving a global
   * annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  Deferred<Annotation> getAnnotation(byte[] tsuid, long start_time);

  public Deferred<ArrayList<Object>> checkNecessaryTablesExist();

  public Deferred<Object> flush();

  public Deferred<ArrayList<KeyValue>> get(final GetRequest request);

  long getFlushInterval();

  public Scanner newScanner(final byte[] table);

  void setFlushInterval(short aShort);

  Deferred<Object> addPoint(final byte[] tsuid, final byte[] value, final long timestamp, final short flags);

  public Deferred<Object> shutdown();

  public Deferred<com.google.common.base.Optional<byte[]>> getId(final String name, final UniqueIdType type);
  public Deferred<com.google.common.base.Optional<String>> getName(final byte[] id, final UniqueIdType type);

  public Deferred<Object> add(final UIDMeta meta);

  Deferred<Object> delete(UIDMeta meta);

  public Deferred<UIDMeta> getMeta(byte[] uid, String name,
                            UniqueIdType type);

  public Deferred<Boolean> updateMeta(final UIDMeta meta,
                                      final boolean overwrite);

  Deferred<Object> deleteUID(byte[] name, UniqueIdType type);

  public Deferred<byte[]> allocateUID(final String name,
                                      final UniqueIdType type);

  Deferred<byte[]> allocateUID(final String name,
                               final byte[] uid,
                               final UniqueIdType type);

  // ------------------ //
  // Compaction helpers //
  // ------------------ //
  void scheduleForCompaction(byte[] row);

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annoation does not exist in storage, this delete call will not throw an
   * error.
   *
   * @param annotation@return A meaningless Deferred for the caller to wait on until the call is
   * complete. The value may be null.
   */
  Deferred<Object> delete(Annotation annotation);

  Deferred<Boolean> updateAnnotation(Annotation original, Annotation annotation);

  Deferred<List<Annotation>> getGlobalAnnotations(final long start_time, final long end_time);

  Deferred<Integer> deleteAnnotationRange(final byte[] tsuid, final long start_time, final long end_time);

  /**
   * Should execute the provided {@link net.opentsdb.core.Query} and
   * return a deferred. Every single item in
   * the returned iterator may contain multiple datapoints but every single
   * instance must only contain the datapoints for a single TSUID. The
   * iterator may return multiple items for the same TSUID.
   * @param query The query to execute
   */
  Deferred<ImmutableList<DataPoints>> executeQuery(final Query query);

  Deferred<Tree> fetchTree(final int tree_id);

  Deferred<Boolean> storeTree(final Tree tree, final boolean overwrite);

  public Deferred<Integer> createNewTree(final Tree tree);

  public Deferred<List<Tree>> fetchAllTrees();

  Deferred<Boolean> deleteTree(final int tree_id, final boolean delete_definition);

  Deferred<Map<String,String>> fetchCollisions(final int tree_id, final List<String> tsuids);

  Deferred<Map<String,String>> fetchNotMatched(final int tree_id,final  List<String> tsuids);

  Deferred<Boolean> flushTreeCollisions(final Tree tree);

  Deferred<Boolean> flushTreeNotMatched(final Tree tree);

  Deferred<Boolean> storeLeaf(final Leaf leaf,final Branch branch,final Tree tree);

  Deferred<ArrayList<Boolean>> storeBranch(final Tree tree, final Branch branch, final boolean store_leaves);

  Deferred<Branch> fetchBranchOnly(final byte[] branch_id);

  Deferred<Branch> fetchBranch(final byte[] branch_id, final boolean load_leaf_uids, final TSDB tsdb);

  Deferred<TreeRule> fetchTreeRule(final int tree_id, final int level, final int order);

  Deferred<Object> deleteTreeRule(final int tree_id, final int level, final int order);

  Deferred<Object> deleteAllTreeRule(final int tree_id);

  Deferred<Boolean> syncTreeRuleToStorage(final TreeRule rule, final boolean overwrite);

  Deferred<Object> delete(final TSMeta tsMeta);

  Deferred<Object> deleteTimeseriesCounter(final TSMeta ts);

  Deferred<Boolean> create(final TSMeta tsMeta);

  Deferred<TSMeta> getTSMeta(final byte[] tsuid);

  Deferred<Boolean> syncToStorage(final TSMeta tsMeta, final Deferred<ArrayList<Object>> uid_group, final boolean overwrite);

  Deferred<Boolean> TSMetaExists(final String tsuid);

  Deferred<Boolean> TSMetaCounterExists(final byte[] tsuid);

  Deferred<Long> incrementAndGetCounter(final byte[] tsuid);

  Deferred<Object> setTSMetaCounter(final byte[] tsuid, final long number);
}
