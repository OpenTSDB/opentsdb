// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRule;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.stats.Span;
import net.opentsdb.uid.IdOrError;
import net.opentsdb.uid.RandomUniqueId;
import net.opentsdb.uid.UniqueIdAssignmentAuthorizer;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;

/**
 * Represents a table of Unique IDs, manages the lookup and creation of IDs.
 * <p>
 * Don't attempt to use {@code equals()} or {@code hashCode()} on
 * this class.
 * 
 * @since 1.0
 */
public class Tsdb1xBigtableUniqueIdStore implements UniqueIdStore {
  private static final Logger LOG = LoggerFactory.getLogger(
      Tsdb1xBigtableUniqueIdStore.class);

  /** A map of the various config UID types. */
  protected static final Map<UniqueIdType, String> CONFIG_PREFIX = 
      Maps.newHashMapWithExpectedSize(UniqueIdType.values().length);
  static {
    for (final UniqueIdType type : UniqueIdType.values()) {
      CONFIG_PREFIX.put(type, "uid." + type.toString().toLowerCase() + ".");
    }
  }
  
  /** The error message returned when an ID is being assigned in the
   * background and should be retried later. */
  public static final String ASSIGN_AND_RETRY = 
      "Assigning ID, queue and retry the data.";
  
  /** Various configuration keys. */
  public static final String CHARACTER_SET_KEY = "character_set";
  public static final String CHARACTER_SET_DEFAULT = "ISO-8859-1";
  public static final String ASSIGN_AND_RETRY_KEY = "assign_and_retry";
  public static final String RANDOM_ASSIGNMENT_KEY = "assign_random";
  public static final String RANDOM_ATTEMPTS_KEY = "attempts.max_random";
  public static final String ATTEMPTS_KEY = "attempts.max";
  
  public static final byte[] METRICS_QUAL = 
      "metrics".getBytes(Const.ASCII_CHARSET);
  public static final byte[] TAG_NAME_QUAL = 
      "tagk".getBytes(Const.ASCII_CHARSET);
  public static final byte[] TAG_VALUE_QUAL = 
      "tagv".getBytes(Const.ASCII_CHARSET);

  /** The single column family used by this class. */
  public static final byte[] ID_FAMILY = "id".getBytes(Const.ASCII_CHARSET);
  /** The single column family used by this class. */
  public static final byte[] NAME_FAMILY = "name".getBytes(Const.ASCII_CHARSET);
  /** Row key of the special row used to track the max ID already assigned. */
  public static final byte[] MAXID_ROW = { 0 };
  /** How many time do we try to assign an ID before giving up. */
  public static final short DEFAULT_ATTEMPTS_ASSIGN_ID = 3;
//  /** How many time do we try to apply an edit before giving up. */
//  public static final short MAX_ATTEMPTS_PUT = 6;
  /** How many time do we try to assign a random ID before giving up. */
  public static final short DEFAULT_ATTEMPTS_ASSIGN_RANDOM_ID = 10;
//  /** Initial delay in ms for exponential backoff to retry failed RPCs. */
//  private static final short INITIAL_EXP_BACKOFF_DELAY = 800;
//  /** Maximum number of results to return in suggest(). */
//  private static final short MAX_SUGGESTIONS = 25;
  
  private final Tsdb1xBigtableDataStore data_store; 

  /** The authorizer pulled from the registry. */
  protected final UniqueIdAssignmentAuthorizer authorizer;
  
  /** Character sets for each type. */
  protected final Charset metric_character_set;
  protected final Charset tagk_character_set;
  protected final Charset tagv_character_set;

  /** Whether or not to immediately return a RETRY state and run the 
   * assignment process in the background. */
  protected final boolean assign_and_retry;
  
  /** Max attempts. */
  protected final short max_attempts_assign;
  protected final short max_attempts_assign_random;
  
  /** Whethe ror not to randomize UID assignments for these types. */
  protected final boolean randomize_metric_ids;
  protected final boolean randomize_tagk_ids;
  protected final boolean randomize_tagv_ids;
  
  final Map<UniqueIdType, Map<String, Deferred<IdOrError>>> pending_assignments;

  public Tsdb1xBigtableUniqueIdStore(final Tsdb1xBigtableDataStore data_store) {
    if (data_store == null) {
      throw new IllegalArgumentException("Data store cannot be null.");
    }
    this.data_store = data_store;
    registerConfigs(data_store.tsdb());
    
    authorizer = data_store.tsdb().getRegistry()
        .getDefaultPlugin(UniqueIdAssignmentAuthorizer.class);
    
    metric_character_set = Charset.forName(data_store.tsdb().getConfig()
        .getString(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            CONFIG_PREFIX.get(UniqueIdType.METRIC) + CHARACTER_SET_KEY)));
    tagk_character_set = Charset.forName(data_store.tsdb().getConfig()
        .getString(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            CONFIG_PREFIX.get(UniqueIdType.TAGK) + CHARACTER_SET_KEY)));
    tagv_character_set = Charset.forName(data_store.tsdb().getConfig()
        .getString(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            CONFIG_PREFIX.get(UniqueIdType.TAGV) + CHARACTER_SET_KEY)));
    
    randomize_metric_ids = data_store.tsdb().getConfig()
        .getBoolean(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            CONFIG_PREFIX.get(UniqueIdType.METRIC) + RANDOM_ASSIGNMENT_KEY));
    randomize_tagk_ids = data_store.tsdb().getConfig()
        .getBoolean(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            CONFIG_PREFIX.get(UniqueIdType.TAGK) + RANDOM_ASSIGNMENT_KEY));
    randomize_tagv_ids = data_store.tsdb().getConfig()
        .getBoolean(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), 
            CONFIG_PREFIX.get(UniqueIdType.TAGV) + RANDOM_ASSIGNMENT_KEY));
    
    assign_and_retry = data_store.tsdb().getConfig()
        .getBoolean(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), ASSIGN_AND_RETRY_KEY));
    max_attempts_assign = (short) data_store.tsdb().getConfig()
        .getInt(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), ATTEMPTS_KEY));
    max_attempts_assign_random = (short) data_store.tsdb().getConfig()
        .getInt(Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), RANDOM_ATTEMPTS_KEY));
    
    // It's important to create maps here.
    pending_assignments = Maps.newHashMapWithExpectedSize(
        UniqueIdType.values().length);
    for (final UniqueIdType type : UniqueIdType.values()) {
      pending_assignments.put(type, Maps.newConcurrentMap());
    }
    
    LOG.info("Initalized UniqueId store.");
  }

  @Override
  public Deferred<String> getName(final UniqueIdType type, 
                                  final byte[] id,
                                  final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (Bytes.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getName")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("id", net.opentsdb.uid.UniqueId.uidToString(id))
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    final Deferred<String> deferred = new Deferred<String>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(data_store.uidTable()))
        .setRows(RowSet.newBuilder()
            .addRowKeys(ByteStringer.wrap(id)))
        .setFilter(RowFilter.newBuilder()
            .setChain(RowFilter.Chain.newBuilder()
                .addFilters(RowFilter.newBuilder()
                    .setFamilyNameRegexFilterBytes(ByteStringer.wrap(NAME_FAMILY)))
                .addFilters(RowFilter.newBuilder()
                    .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier)))))
        .build();
    
    class ResultCB implements FutureCallback<List<Row>> {

      @Override
      public void onSuccess(final List<Row> results) {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        
        deferred.callback(results.isEmpty() ? null :
          new String(results.get(0).getFamilies(0).getColumns(0)
              .getCells(0).getValue().toByteArray(), characterSet(type)));
      }

      @Override
      public void onFailure(final Throwable t) {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", t)
            .finish();
        }
        deferred.callback(new StorageException("Failed to fetch name.", t));
      }
      
    }
    
    try {
      Futures.addCallback(data_store.executor().readRowsAsync(request), 
          new ResultCB(), data_store.pool());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
    return deferred;
  }
  
  @Override
  public Deferred<List<String>> getNames(final UniqueIdType type, 
                                         final List<byte[]> ids,
                                         final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("IDs cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getNames")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          //.withTag("ids", /* TODO - an array to hex method */ "")
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    RowSet.Builder rows = RowSet.newBuilder();
    for (final byte[] id : ids) {
      if (Bytes.isNullOrEmpty(id)) {
        throw new IllegalArgumentException("A null or empty ID was "
            + "found in the list.");
      }
      rows.addRowKeys(ByteStringer.wrap(id));
    }
    
    final Deferred<List<String>> deferred = new Deferred<List<String>>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(data_store.uidTable()))
        .setRows(rows.build())
        .setFilter(RowFilter.newBuilder()
            .setChain(RowFilter.Chain.newBuilder()
                .addFilters(RowFilter.newBuilder()
                    .setFamilyNameRegexFilterBytes(ByteStringer.wrap(NAME_FAMILY)))
                .addFilters(RowFilter.newBuilder()
                    .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier)))))
        .build();
    
    class ResultCB implements FutureCallback<List<Row>> {

      @Override
      public void onSuccess(final List<Row> results) {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        
        final List<String> names = Lists.newArrayListWithCapacity(ids.size());
        // TODO - can we assume that the order of values returned is
        // the same as those requested?
        int id_idx = 0;
        for (int i = 0; i < results.size(); i++) {
          if (results.get(i).getFamiliesCount() < 1 ||
              results.get(i).getFamilies(0).getColumnsCount() < 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Empty row from result at index " + i);
            }
            names.add(null);
          } else {
            while (Bytes.memcmp(results.get(i).getKey().toByteArray(), 
                ids.get(id_idx++)) != 0) {
              names.add(null);
            }
            names.add(new String(results.get(i).getFamilies(0).getColumns(0)
                .getCells(0).getValue().toByteArray(), 
                characterSet(type)));
          }
        }
        // fill trailing empties.
        for (int i = names.size(); i < ids.size(); i++) {
          names.add(null);
        }
        
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        deferred.callback(names);
      }

      @Override
      public void onFailure(final Throwable t) {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", t)
            .finish();
        }
        deferred.callback(new StorageException("Failed to fetch names.", t));
      }
      
    }
    
    try {
      Futures.addCallback(data_store.executor().readRowsAsync(request), 
          new ResultCB(), data_store.pool());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
    return deferred;
  }

  @Override
  public Deferred<byte[]> getId(final UniqueIdType type, 
                                final String name,
                                final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Name cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getId")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("name", name)
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    final Deferred<byte[]> deferred = new Deferred<byte[]>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(data_store.uidTable()))
        .setRows(RowSet.newBuilder()
            .addRowKeys(ByteStringer.wrap(name.getBytes(characterSet(type)))))
        .setFilter(RowFilter.newBuilder()
            .setChain(RowFilter.Chain.newBuilder()
                .addFilters(RowFilter.newBuilder()
                    .setFamilyNameRegexFilterBytes(ByteStringer.wrap(ID_FAMILY)))
                .addFilters(RowFilter.newBuilder()
                    .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier)))))
        .build();
    
   class ResultCB implements FutureCallback<List<Row>> {

      @Override
      public void onSuccess(final List<Row> results) {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        
        deferred.callback(results.isEmpty() ? null :
          results.get(0).getFamilies(0).getColumns(0).getCells(0)
            .getValue().toByteArray());
      }

      @Override
      public void onFailure(final Throwable t) {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", t)
            .finish();
        }
        deferred.callback(new StorageException("Failed to fetch name.", t));
      }
      
    }
    
    try {
      Futures.addCallback(data_store.executor().readRowsAsync(request), 
          new ResultCB(), data_store.pool());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
    return deferred;
  }
  
  @Override
  public Deferred<List<byte[]>> getIds(final UniqueIdType type, 
                                       final List<String> names,
                                       final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (names == null || names.isEmpty()) {
      throw new IllegalArgumentException("Names cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getIds")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("names", names.toString())
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    RowSet.Builder rows = RowSet.newBuilder();
    for (final String name : names) {
      if (Strings.isNullOrEmpty(name)) {
        throw new IllegalArgumentException("A null or empty name was "
            + "found in the list.");
      }
      rows.addRowKeys(ByteStringer.wrap(name.getBytes(characterSet(type))));
    }
    
    final Deferred<List<byte[]>> deferred = new Deferred<List<byte[]>>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(data_store.uidTable()))
        .setRows(rows.build())
        .setFilter(RowFilter.newBuilder()
            .setChain(RowFilter.Chain.newBuilder()
                .addFilters(RowFilter.newBuilder()
                    .setFamilyNameRegexFilterBytes(ByteStringer.wrap(ID_FAMILY)))
                .addFilters(RowFilter.newBuilder()
                    .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier)))))
        .build();
    
    class ResultCB implements FutureCallback<List<Row>> {

      @Override
      public void onSuccess(final List<Row> results) {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        
        final List<byte[]> ids = Lists.newArrayListWithCapacity(results.size());
        int name_idx = 0;
        for (int i = 0; i < results.size(); i++) {
          if (results.get(i).getFamiliesCount() < 1 ||
              results.get(i).getFamilies(0).getColumnsCount() < 1) {
            continue;
          } else {
            while (!new String(results.get(i).getKey().toByteArray(), characterSet(type))
                .equals(names.get(name_idx++))) {
              ids.add(null);
            }
            ids.add(results.get(i).getFamilies(0).getColumns(0)
                .getCells(0).getValue().toByteArray());
          }
        }
        for (int i = ids.size(); i < names.size(); i++) {
          ids.add(null);
        }
        
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        deferred.callback(ids);
      }

      @Override
      public void onFailure(final Throwable t) {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", t)
            .finish();
        }
        deferred.callback(new StorageException("Failed to fetch id.", t));
      }
      
    }
    
    try {
      Futures.addCallback(data_store.executor().readRowsAsync(request), 
          new ResultCB(), data_store.pool());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
    return deferred;
  }
  
  /**
   * Implements the process to allocate a new UID.
   * This callback is re-used multiple times in a four step process:
   *   1. Allocate a new UID via atomic increment.
   *   2. Create the reverse mapping (ID to name).
   *   3. Create the forward mapping (name to ID).
   *   4. Return the new UID to the caller.
   */
  final class UniqueIdAllocator implements Callback<Object, Object>, TimerTask {
    private final UniqueIdType type;
    private final String name;  // What we're trying to allocate an ID for.
    private final Deferred<IdOrError> assignment; // deferred to call back
    private final boolean randomize_id; // Whether or not to randomize.
    private final byte[] qualifier;
    private final int id_width;
    private final short attempts;
    private short attempt;     // Give up when zero.
    private boolean run_timer = false;
    
    private Exception ex = null;  // Last exception caught.
    // TODO(manolama) - right now if we retry the assignment it will create a 
    // callback chain MAX_ATTEMPTS_* long and call the ErrBack that many times.
    // This can be cleaned up a fair amount but it may require changing the 
    // public behavior a bit. For now, the flag will prevent multiple attempts
    // to execute the callback.
    private boolean called = false; // whether we called the deferred or not

    private long id = -1;  // The ID we'll grab with an atomic increment.
    private byte row[];    // The same ID, as a byte array.

    private static final byte ALLOCATE_UID = 0;
    private static final byte CREATE_REVERSE_MAPPING = 1;
    private static final byte CREATE_FORWARD_MAPPING = 2;
    private static final byte DONE = 3;
    private byte state = ALLOCATE_UID;  // Current state of the process.

    UniqueIdAllocator(final UniqueIdType type, 
                      final String name, 
                      final Deferred<IdOrError> assignment) {
      this.type = type;
      this.name = name;
      this.assignment = assignment;
      
      switch (type) {
      case METRIC:
        randomize_id = randomize_metric_ids;
        qualifier = METRICS_QUAL;
        id_width = data_store.schema().metricWidth();
        attempts = attempt = randomize_metric_ids ?
            max_attempts_assign_random : max_attempts_assign;
        break;
      case TAGK:
        randomize_id = randomize_tagk_ids;
        qualifier = TAG_NAME_QUAL;
        id_width = data_store.schema().tagkWidth();
        attempts = attempt = randomize_tagk_ids ?
            max_attempts_assign_random : max_attempts_assign;
        break;
      case TAGV:
        randomize_id = randomize_tagv_ids;
        qualifier = TAG_VALUE_QUAL;
        id_width = data_store.schema().tagvWidth();
        attempts = attempt = randomize_tagv_ids ?
            max_attempts_assign_random : max_attempts_assign;
        break;
      default:
        purgeWaiting();
        throw new IllegalArgumentException("Unhandled type: " + type);
      }
    }

    Deferred<IdOrError> tryAllocate() {
      attempt--;
      state = ALLOCATE_UID;
      call(null);
      return assignment;
    }

    @SuppressWarnings("unchecked")
    public Object call(final Object arg) {
      if (attempt == 0) {
        final String err;
        switch (type) {
        case METRIC:
          err = "Failed to assign an ID for kind='" + type
            + "' name='" + name + "' after " + attempts + " attempts.";
          break;
        case TAGK:
          err = "Failed to assign an ID for kind='" + type
          + "' name='" + name + "' after " + attempts + " attempts.";
          break;
        case TAGV:
          err = "Failed to assign an ID for kind='" + type
          + "' name='" + name + "' after " + attempts + " attempts.";
          break;
        default:
          purgeWaiting();
          throw new IllegalArgumentException("Unhandled type: " + type);
        }
        
        if (ex != null) {
          LOG.error(err, ex);
        } else {
          LOG.error(err);
        }
        purgeWaiting();
        assignment.callback(IdOrError.wrapRetry(err));
        return null;
      }

      if (arg instanceof Exception) {
        final String msg = ("Failed attempt #" + (randomize_id
                         ? (max_attempts_assign_random - attempt) 
                         : (max_attempts_assign - attempt))
                         + " to assign an UID for " + type + ':' + name
                         + " at step #" + state);
        if (arg instanceof StorageException) {
          LOG.error(msg, (Exception) arg);
          ex = (StorageException) arg;
          attempt--;
          state = ALLOCATE_UID;  // Retry from the beginning.
        } else {
          purgeWaiting();
          LOG.error("WTF?  Unexpected exception!  " + msg, (Exception) arg);
          return arg;  // Unexpected exception, let it bubble up.
        }
      }

      if (state == ALLOCATE_UID && attempt < attempts - 1 && run_timer) {
        retryWithBackoff();
        return null;
      } else {
        run_timer = true;
      }
      
      class ErrBack implements Callback<Object, Exception> {
        public Object call(final Exception e) throws Exception {
          if (!called) {
            LOG.warn("Failed pending assignment for: " + name, e);
            purgeWaiting();
            assignment.callback(e);
            called = true;
          }
          return assignment;
        }
      }
      
      @SuppressWarnings("rawtypes")
      final Deferred d;
      switch (state) {
        case ALLOCATE_UID:
          d = allocateUid();
          break;
        case CREATE_REVERSE_MAPPING:
          d = createReverseMapping(arg);
          break;
        case CREATE_FORWARD_MAPPING:
          d = createForwardMapping(arg);
          break;
        case DONE:
          return done(arg);
        default:
          purgeWaiting();
          throw new AssertionError("Should never be here!");
      }
      return d.addBoth(this).addErrback(new ErrBack());
    }

    /** Generates either a random or a serial ID. If random, we need to
     * make sure that there isn't a UID collision.
     */
    private Deferred<Long> allocateUid() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating " + (randomize_id ? "a random " : "an ") + 
            "ID for kind='" + type + "' name='" + name + '\'');
      }

      state = CREATE_REVERSE_MAPPING;
      if (randomize_id) {
        return Deferred.fromResult(RandomUniqueId.getRandomUID(id_width));
      } else {
        // TODO - fix in AsyncHBase
        try {
          final ReadModifyWriteRowRequest request = 
              ReadModifyWriteRowRequest.newBuilder()
              .setTableNameBytes(ByteStringer.wrap(data_store.uidTable()))
              .setRowKey(ByteStringer.wrap(MAXID_ROW))
              .addRules(ReadModifyWriteRule.newBuilder()
                  .setIncrementAmount(1)
                  .setFamilyNameBytes(ByteStringer.wrap(ID_FAMILY))
                  .setColumnQualifier(ByteStringer.wrap(qualifier)))
              .build();
          final Deferred<Long> deferred = new Deferred<Long>();
          class IncrementCB implements FutureCallback<ReadModifyWriteRowResponse> {

            @Override
            public void onSuccess(final ReadModifyWriteRowResponse result) {
              // TODO - row check
              deferred.callback(Bytes.getLong(result.getRow()
                  .getFamilies(0).getColumns(0).getCells(0)
                  .getValue().toByteArray()));
            }

            @Override
            public void onFailure(final Throwable t) {
              deferred.callback(t);
            }
            
          }
          
          Futures.addCallback(
              data_store.executor().readModifyWriteRowAsync(request),
              new IncrementCB(), data_store.pool());
          return deferred;
        } catch (ArrayIndexOutOfBoundsException e) {
          purgeWaiting();
          throw new StorageException("Atomic increment "
              + "counter may be corrupt as it doesn't appear to be 8 "
              + "bytes long", e);
        } catch (InterruptedException e) {
          purgeWaiting();
          throw new StorageException("Atomic increment failed", e);
        }
      }
    }

    /**
     * Create the reverse mapping.
     * We do this before the forward one so that if we die before creating
     * the forward mapping we don't run the risk of "publishing" a
     * partially assigned ID.  The reverse mapping on its own is harmless
     * but the forward mapping without reverse mapping is bad as it would
     * point to an ID that cannot be resolved.
     */
    private Deferred<Boolean> createReverseMapping(final Object arg) {
      if (!(arg instanceof Long)) {
        purgeWaiting();
        throw new IllegalStateException("Expected a Long but got " + arg);
      }
      id = (Long) arg;
      if (id <= 0) {
        purgeWaiting();
        throw new IllegalStateException("Got a negative ID from storage: " + id);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got ID=" + id
                 + " for kind='" + type + "' name='" + name + "'");
      }
      row = Bytes.fromLong(id);
      // row.length should actually be 8.
      if (row.length < id_width) {
        purgeWaiting();
        throw new IllegalStateException("OMG, row.length = " + row.length
                                        + " which is less than " + id_width
                                        + " for id=" + id
                                        + " row=" + Arrays.toString(row));
      }
      // Verify that we're going to drop bytes that are 0.
      for (int i = 0; i < row.length - id_width; i++) {
        if (row[i] != 0) {
          final String message = "All Unique IDs for " + type
            + " on " + id_width + " bytes are already assigned!";
          LOG.error("OMG " + message);
          purgeWaiting();
          throw new IllegalStateException(message);
        }
      }
      // Shrink the ID on the requested number of bytes.
      row = Arrays.copyOfRange(row, row.length - id_width, row.length);

      state = CREATE_FORWARD_MAPPING;
      // We are CAS'ing the KV into existence -- the second argument is how
      // we tell HBase we want to atomically create the KV, so that if there
      // is already a KV in this cell, we'll fail.  Technically we could do
      // just a `put' here, as we have a freshly allocated UID, so there is
      // not reason why a KV should already exist for this UID, but just to
      // err on the safe side and catch really weird corruption cases, we do
      // a CAS instead to create the KV.
      final CheckAndMutateRowRequest request = 
          CheckAndMutateRowRequest.newBuilder()
          .setTableNameBytes(ByteStringer.wrap(data_store.uidTable()))
          .setRowKey(ByteStringer.wrap(row))
          .setPredicateFilter(RowFilter.Chain.newBuilder()
              .addFiltersBuilder()
                .setChain(Chain.newBuilder()
                    .addFilters(RowFilter.newBuilder()
                        .setFamilyNameRegexFilterBytes(ByteStringer.wrap(NAME_FAMILY)))
                    .addFilters(RowFilter.newBuilder()
                        .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier)))
                    .addFilters(RowFilter.newBuilder()
                        .setCellsPerColumnLimitFilter(1))
                )
          )
          .addFalseMutations(Mutation.newBuilder()
              .setSetCell(SetCell.newBuilder()
                  .setFamilyNameBytes(ByteStringer.wrap(NAME_FAMILY))
                  .setColumnQualifier(ByteStringer.wrap(qualifier))
                  .setValue(ByteStringer.wrap(name.getBytes(characterSet(type))))
                  .setTimestampMicros(-1))
              )
          .build();
      
      final Deferred<Boolean> deferred = new Deferred<Boolean>();
      class CasCB implements FutureCallback<CheckAndMutateRowResponse> {

        @Override
        public void onSuccess(final CheckAndMutateRowResponse result) {
          deferred.callback(Tsdb1xBigtableDataStore
              .wasMutationApplied(request, result));
        }

        @Override
        public void onFailure(final Throwable t) {
          deferred.callback(t);
        }
        
      }
      try {
        Futures.addCallback(
            data_store.executor().checkAndMutateRowAsync(request),
            new CasCB(), data_store.pool());
      } catch (InterruptedException e) {
        return Deferred.fromError(e);
      }
      return deferred;
    }

    private Deferred<?> createForwardMapping(final Object arg) {
      if (!(arg instanceof Boolean)) {
        purgeWaiting();
        throw new IllegalStateException("Expected a Boolean but got " + arg);
      }
      if (!((Boolean) arg)) {  // Previous CAS failed. 
        if (randomize_id) {
          // This random Id is already used by another row
          LOG.warn("Detected random id collision and retrying kind='" + 
              type + "' name='" + name + "'");
          //random_id_collisions++; // TODO
        } else {
          // something is really messed up then
          LOG.error("WTF!  Failed to CAS reverse mapping: " + 
              Bytes.pretty(Bytes.fromLong(id))
              + " => " + name 
              + " -- run an fsck against the UID table!");
        }
        attempt--;
        state = ALLOCATE_UID;
        //retryWithBackoff();
        return Deferred.fromResult(false);
      }

      state = DONE;
      final CheckAndMutateRowRequest request = 
          CheckAndMutateRowRequest.newBuilder()
          .setTableNameBytes(ByteStringer.wrap(data_store.uidTable()))
          .setRowKey(ByteStringer.wrap(name.getBytes(characterSet(type))))
          .setPredicateFilter(RowFilter.Chain.newBuilder()
              .addFiltersBuilder()
                .setChain(Chain.newBuilder()
                    .addFilters(RowFilter.newBuilder()
                        .setFamilyNameRegexFilterBytes(ByteStringer.wrap(ID_FAMILY)))
                    .addFilters(RowFilter.newBuilder()
                        .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier)))
                    .addFilters(RowFilter.newBuilder()
                        .setCellsPerColumnLimitFilter(1))
                )
          )
          .addFalseMutations(Mutation.newBuilder()
              .setSetCell(SetCell.newBuilder()
                  .setFamilyNameBytes(ByteStringer.wrap(ID_FAMILY))
                  .setColumnQualifier(ByteStringer.wrap(qualifier))
                  .setValue(ByteStringer.wrap(row))
                  .setTimestampMicros(-1))
              )
          .build();
      
      final Deferred<Boolean> deferred = new Deferred<Boolean>();
      class CasCB implements FutureCallback<CheckAndMutateRowResponse> {

        @Override
        public void onSuccess(final CheckAndMutateRowResponse result) {
          deferred.callback(Tsdb1xBigtableDataStore.wasMutationApplied(request, result));
        }

        @Override
        public void onFailure(Throwable t) {
          deferred.callback(new StorageException("Failed to "
              + "compare and set UID forward mapping.", t));
        }
        
      }
      try {
        Futures.addCallback(
            data_store.executor().checkAndMutateRowAsync(request),
            new CasCB(), data_store.pool());
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return deferred;
    }

    private Deferred<IdOrError> done(final Object arg) {
      if (!(arg instanceof Boolean)) {
        throw new IllegalStateException("Expected a Boolean but got " + arg);
      }
      if (!((Boolean) arg)) {  // Previous CAS failed.  We lost a race.
        LOG.warn("Race condition: tried to assign ID " + id + " to "
                 + type + ":" + name + ", but CAS failed on "
                 + name + " => " + Bytes.pretty(Bytes.fromLong(id)) 
                 + ", which indicates this UID must have"
                 + " been allocated concurrently by another TSD or thread. "
                 + "So ID " + id + " was leaked.");
        // If two TSDs attempted to allocate a UID for the same name at the
        // same time, they would both have allocated a UID, and created a
        // reverse mapping, and upon getting here, only one of them would
        // manage to CAS this KV into existence.  The one that loses the
        // race will retry and discover the UID assigned by the winner TSD,
        // and a UID will have been wasted in the process.  No big deal.
        if (randomize_id) {
          // This random Id is already used by another row
          LOG.warn("Detected random id collision between two tsdb "
              + "servers kind='" + type + "' name='" + name + "'");
          //random_id_collisions++;
        }
        
        class GetIdCB implements Callback<Object, byte[]> {
          public Object call(final byte[] id) throws Exception {
            purgeWaiting();
            assignment.callback(IdOrError.wrapId(id));
            return null;
          }
        }
        getId(type, name, null /* TODO */).addCallback(new GetIdCB());
        return assignment;
      }
      
      // TODO - UID meta
//      if (tsdb != null && tsdb.getConfig().enable_realtime_uid()) {
//        final UIDMeta meta = new UIDMeta(type, row, name);
//        meta.storeNew(tsdb);
//        LOG.info("Wrote UIDMeta for: " + name);
//        tsdb.indexUIDMeta(meta);
//      }
      purgeWaiting();
      assignment.callback(IdOrError.wrapId(row));
      return assignment;
    }

    private void purgeWaiting() {
      final Map<String, Deferred<IdOrError>> ids_pending = 
          pending_assignments.get(type);
      synchronized(ids_pending) {
        if (ids_pending.remove(name) != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Completed pending assignment for: " + name);
          }
        }
      }
    }

    private void retryWithBackoff() {
      if (state != ALLOCATE_UID) {
        throw new IllegalStateException("We can't retry with a backoff "
            + "if the state isn't ALLOCATE_UID: " + state);
      }
      // backoff
      final int diff = (attempts - attempt);
      final int delay = diff < 4
          ? 200 * (diff)     // 200, 400, 600, 800
              : 1000 + (1 << diff);  // 1016, 1032, 1064, 1128, 1256, 1512, ..
      run_timer = false;
      data_store.tsdb().getMaintenanceTimer().newTimeout(this, 
          delay, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void run(final Timeout ignored) throws Exception {
      try {
        call(null);
      } catch (Exception e) {
        LOG.error("Unexpected exception caught in timmer handler", e);
        purgeWaiting();
        assignment.callback(e);
      }
    }
  }
  
  @Override
  public Deferred<IdOrError> getOrCreateId(AuthState auth, UniqueIdType type,
      String name, TimeSeriesDatumId id, Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Name cannot be null or empty.");
    }
    if (id == null) {
      throw new IllegalArgumentException("The ID cannot be null.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getSimpleName() + ".getOrCreateId")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("name", name)
          .withTag("id", id.toString())
          .start();
    } else {
      child = null;
    }
    
    /** Triggers the assignment if allowed through the filter */
    class AssignmentAllowedCB implements  Callback<Deferred<IdOrError>, String> {
      @Override
      public Deferred<IdOrError> call(final String error) throws Exception {
        if (!Strings.isNullOrEmpty(error)) {
          return Deferred.fromResult(IdOrError.wrapRejected(error));
        }
        
        Deferred<IdOrError> assignment = null;
        final Map<String, Deferred<IdOrError>> typed_assignments = 
            pending_assignments.get(type);
        synchronized (typed_assignments) {
          assignment = typed_assignments.get(name);
          if (assignment == null) {
            // to prevent UID leaks that can be caused when multiple time
            // series for the same metric or tags arrive, we need to write a 
            // deferred to the pending map as quickly as possible. Then we can 
            // start the assignment process after we've stashed the deferred 
            // and released the lock
            assignment = new Deferred<IdOrError>();
            typed_assignments.put(name, assignment);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.info("Already waiting for UID assignment: '" + name + "'");
            }
            if (assign_and_retry) {
              // aww, still pending.
              return Deferred.fromResult(IdOrError.wrapRetry(ASSIGN_AND_RETRY));
            } else {
              return assignment;
            }
          }
        }
        
        // start the assignment dance after stashing the deferred
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning UID for '" + name + "' of type '" + type + 
              "' for series '" + id + "'");
        }
        
        // start the assignment dance after stashing the deferred
        if (assign_and_retry) {
          new UniqueIdAllocator(type, name, assignment).tryAllocate();
          return Deferred.fromResult(IdOrError.wrapRetry(ASSIGN_AND_RETRY));
        }
        return new UniqueIdAllocator(type, name, assignment).tryAllocate();
      }
      @Override
      public String toString() {
        return "AssignmentAllowedCB";
      }
    }
    
    /** Triggers the assignment path if the response was null. */
    class IdCB implements Callback<Deferred<IdOrError>, byte[]> {
      public Deferred<IdOrError> call(final byte[] uid) {
        if (uid != null) {
          return Deferred.fromResult(IdOrError.wrapId(uid));
        }

        if (authorizer != null && authorizer.fillterUIDAssignments()) {
          return authorizer.allowUIDAssignment(auth, type, name, id)
              .addCallbackDeferring(new AssignmentAllowedCB());
        } else {
          return Deferred.fromResult((String) null)
              .addCallbackDeferring(new AssignmentAllowedCB());
        }
      }
    }

    // Kick off the lookup, and if we don't find it there either, start
    // the process to allocate a UID.
    return getId(type, name, child).addCallbackDeferring(new IdCB());
 
  }

  @Override
  public Deferred<List<IdOrError>> getOrCreateIds(AuthState auth,
      UniqueIdType type, List<String> names, TimeSeriesDatumId id, Span span) {
    final List<IdOrError> results = 
        Lists.newArrayListWithExpectedSize(names.size());
    for (int i = 0; i < names.size(); i++) {
      results.add(null);
    }
    
    final List<Deferred<Object>> deferreds =
        Lists.newArrayListWithExpectedSize(names.size());
    
    class ResponseCB implements Callback<Object, Object> {
      final int idx;
      
      ResponseCB(final int idx) {
        this.idx = idx;
      }
      
      @Override
      public Object call(final Object response) throws Exception {
        
        if (response instanceof IdOrError) {
          results.set(idx, (IdOrError) response);
        } else {
          if (response instanceof Exception) {
            results.set(idx, IdOrError.wrapError(
                ((Exception) response).getMessage(), (Exception) response));
          } else {
            results.set(idx, IdOrError.wrapError(
                "Unknown response type: " + response));
          }
        }
        return null;
      }
    }
    
    for (int i = 0; i < names.size(); i++) {
      try {
        @SuppressWarnings("rawtypes")
        final Deferred d = getOrCreateId(auth, type, names.get(i), id, span);
        deferreds.add(d.addBoth(new ResponseCB(i)));
      } catch (Exception e) {
        results.set(i, IdOrError.wrapError(e.getMessage()));
      }
    }
    
    class GroupCB implements Callback<List<IdOrError>, ArrayList<Object>> {
      @Override
      public List<IdOrError> call(ArrayList<Object> arg) throws Exception {
        return results;
      }
    }
    
    return Deferred.groupInOrder(deferreds).addCallback(new GroupCB());
  
  }

  @Override
  public Charset characterSet(final UniqueIdType type) {
    switch (type) {
    case METRIC:
      return metric_character_set;
    case TAGK:
      return tagk_character_set;
    case TAGV:
      return tagv_character_set;
    default:
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  @VisibleForTesting
  void registerConfigs(final TSDB tsdb) {
    for (final Entry<UniqueIdType, String> entry : CONFIG_PREFIX.entrySet()) {
      if (!data_store.tsdb().getConfig().hasProperty(
          Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), entry.getValue() + CHARACTER_SET_KEY))) {
        data_store.tsdb().getConfig().register(
            Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), entry.getValue() + CHARACTER_SET_KEY), 
            CHARACTER_SET_DEFAULT, 
            false, 
            "The character set used for encoding/decoding UID "
            + "strings for " + entry.getKey().toString().toLowerCase() 
            + " entries.");
      }
      
      if (!data_store.tsdb().getConfig().hasProperty(
          Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), entry.getValue() + RANDOM_ASSIGNMENT_KEY))) {
        data_store.tsdb().getConfig().register(
            Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), entry.getValue() + RANDOM_ASSIGNMENT_KEY), 
            false, 
            false, 
            "Whether or not to randomly assign UIDs for " 
            + entry.getKey().toString().toLowerCase() + " entries "
                + "instead of incrementing a counter in storage.");
      }
    }
    
    if (!data_store.tsdb().getConfig().hasProperty(
        Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), ASSIGN_AND_RETRY_KEY))) {
      data_store.tsdb().getConfig().register(
          Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), ASSIGN_AND_RETRY_KEY), 
          false, 
          false, 
          "Whether or not to return with a RETRY write state immediately "
          + "when a UID needs to be assigned and being the assignment "
          + "process in the background. The next time the same string "
          + "is looked up it should be assigned.");
    }
    
    if (!data_store.tsdb().getConfig().hasProperty(
        Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), RANDOM_ATTEMPTS_KEY))) {
      data_store.tsdb().getConfig().register(
          Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), RANDOM_ATTEMPTS_KEY), 
          DEFAULT_ATTEMPTS_ASSIGN_RANDOM_ID, 
          false, 
          "The maximum number of attempts to make before giving up on "
          + "a UID assignment and return a RETRY error when in the "
          + "random ID mode. (This is usually higher than the normal "
          + "mode as random IDs can collide more often.)");
    }
    
    if (!data_store.tsdb().getConfig().hasProperty(
        Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), ATTEMPTS_KEY))) {
      data_store.tsdb().getConfig().register(
          Tsdb1xBigtableDataStore.getConfigKey(data_store.id(), ATTEMPTS_KEY), 
          DEFAULT_ATTEMPTS_ASSIGN_ID, 
          false, 
          "The maximum number of attempts to make before giving up on "
          + "a UID assignment and return a RETRY error.");
    }
  }
  
}
