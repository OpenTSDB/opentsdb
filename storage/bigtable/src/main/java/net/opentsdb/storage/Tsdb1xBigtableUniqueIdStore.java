// This file is part of OpenTSDB.
// Copyright (C) 2010-2021  The OpenTSDB Authors.
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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import com.google.bigtable.v2.ReadRowsRequest.Builder;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.UnsafeByteOperations;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import net.opentsdb.uid.Base1xUniqueIdStore;
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
public class Tsdb1xBigtableUniqueIdStore extends Base1xUniqueIdStore {
  private static final Logger LOG = LoggerFactory.getLogger(
      Tsdb1xBigtableUniqueIdStore.class);

  private final Tsdb1xBigtableDataStore data_store; 

  public Tsdb1xBigtableUniqueIdStore(final Tsdb1xBigtableDataStore data_store,
                                     final String id) {
    super(data_store.schema(), data_store.tsdb(), id);
    this.data_store = data_store;
  }

  @Override
  public Deferred<List<String>> suggest(final UniqueIdType type,
                                        final String query,
                                        final int max) {
    final byte[] kind;
    switch(type) {
      case METRIC:
        kind = METRICS_QUAL;
        break;
      case TAGK:
        kind = TAG_NAME_QUAL;
        break;
      case TAGV:
        kind = TAG_VALUE_QUAL;
        break;
      default:
        throw new IllegalArgumentException("This data store does not "
                + "handle Unique IDs of type: " + type);
    }

    final Builder requestBuilder =
            ReadRowsRequest.newBuilder()
                    .setTableNameBytes(UnsafeByteOperations.unsafeWrap(data_store.uidTable()))
                    .setRowsLimit(max)
                    .setFilter(RowFilter.newBuilder()
                            .setChain(Chain.newBuilder()
                                    .addFilters(RowFilter.newBuilder()
                                            .setFamilyNameRegexFilterBytes(UnsafeByteOperations.unsafeWrap(NAME_FAMILY)))
                                    .addFilters(RowFilter.newBuilder()
                                            .setColumnQualifierRegexFilter(UnsafeByteOperations.unsafeWrap(kind)))));

    if (Strings.isNullOrEmpty(query)) {
      requestBuilder.setRows(RowSet.newBuilder()
              .addRowRanges(RowRange.newBuilder()
                      .setStartKeyClosed(UnsafeByteOperations.unsafeWrap(START_ROW))
                      .setEndKeyOpen(UnsafeByteOperations.unsafeWrap(END_ROW))));
    } else {
      byte[] start_row = query.getBytes(StandardCharsets.UTF_8); // NOTE ASCII only for suggest
      byte[] end_row = Arrays.copyOf(start_row, start_row.length);
      end_row[start_row.length - 1]++; // fine to go into unprintable.
      requestBuilder.setRows(RowSet.newBuilder()
              .addRowRanges(RowRange.newBuilder()
                      .setStartKeyClosed(UnsafeByteOperations.unsafeWrap(start_row))
                      .setEndKeyOpen(UnsafeByteOperations.unsafeWrap(end_row))));
    }

    final ResultScanner<FlatRow> scnr = data_store.session().getDataClient()
            .readFlatRows(requestBuilder.build());
    try {
      FlatRow row;
      List<String> results = Lists.newArrayList();
      while ((row = scnr.next()) != null) {
        results.add(row.getRowKey().toString(StandardCharsets.ISO_8859_1));
      }
      return Deferred.fromResult(results);
    } catch (IOException e) {
      return Deferred.fromError(e);
    }
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
      rows.addRowKeys(UnsafeByteOperations.unsafeWrap(id));
    }
    
    final Deferred<List<String>> deferred = new Deferred<List<String>>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(UnsafeByteOperations.unsafeWrap(data_store.uidTable()))
        .setRows(rows.build())
        .setFilter(RowFilter.newBuilder()
            .setChain(RowFilter.Chain.newBuilder()
                .addFilters(RowFilter.newBuilder()
                    .setFamilyNameRegexFilterBytes(UnsafeByteOperations.unsafeWrap(NAME_FAMILY)))
                .addFilters(RowFilter.newBuilder()
                    .setColumnQualifierRegexFilter(UnsafeByteOperations.unsafeWrap(qualifier)))))
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
      rows.addRowKeys(UnsafeByteOperations.unsafeWrap(name.getBytes(characterSet(type))));
    }
    
    final Deferred<List<byte[]>> deferred = new Deferred<List<byte[]>>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(UnsafeByteOperations.unsafeWrap(data_store.uidTable()))
        .setRows(rows.build())
        .setFilter(RowFilter.newBuilder()
            .setChain(RowFilter.Chain.newBuilder()
                .addFilters(RowFilter.newBuilder()
                    .setFamilyNameRegexFilterBytes(UnsafeByteOperations.unsafeWrap(ID_FAMILY)))
                .addFilters(RowFilter.newBuilder()
                    .setColumnQualifierRegexFilter(UnsafeByteOperations.unsafeWrap(qualifier)))))
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

  @Override
  protected Deferred<byte[]> get(final UniqueIdType type,
                                 final byte[] key,
                                 final byte[] family) {
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
    final ReadRowsRequest request = ReadRowsRequest.newBuilder()
            .setTableNameBytes(UnsafeByteOperations.unsafeWrap(data_store.uidTable()))
            .setRows(RowSet.newBuilder()
                    .addRowKeys(UnsafeByteOperations.unsafeWrap(key)))
            .setFilter(RowFilter.newBuilder()
                    .setChain(RowFilter.Chain.newBuilder()
                            .addFilters(RowFilter.newBuilder()
                                    .setFamilyNameRegexFilterBytes(UnsafeByteOperations.unsafeWrap(family)))
                            .addFilters(RowFilter.newBuilder()
                                    .setColumnQualifierRegexFilter(UnsafeByteOperations.unsafeWrap(qualifier)))))
            .build();
    class ResultCB implements FutureCallback<List<Row>> {

      @Override
      public void onSuccess(final List<Row> results) {
        if (results == null) {
          throw new StorageException("Result list returned was null for key "
                  + Arrays.toString(key));
        }

        deferred.callback(results.isEmpty() ? null :
                results.get(0).getFamilies(0).getColumns(0)
                        .getCells(0).getValue().toByteArray());
      }

      @Override
      public void onFailure(final Throwable t) {
        deferred.callback(new StorageException("Failed to fetch key: "
                + Arrays.toString(key), t));
      }

    }

    try {
      Futures.addCallback(data_store.executor().readRowsAsync(request),
              new ResultCB(), data_store.pool());
    } catch (InterruptedException e) {
      return Deferred.fromError(new StorageException(
              "Unexpected exception from storage", e));
    }
    return deferred;
  }

  @Override
  protected Deferred<Long> increment(final byte[] key,
                                     final byte[] family,
                                     final byte[] qualifier) {
    final ReadModifyWriteRowRequest request =
        ReadModifyWriteRowRequest.newBuilder()
          .setTableNameBytes(UnsafeByteOperations.unsafeWrap(data_store.uidTable()))
          .setRowKey(UnsafeByteOperations.unsafeWrap(key))
          .addRules(ReadModifyWriteRule.newBuilder()
              .setIncrementAmount(1)
              .setFamilyNameBytes(UnsafeByteOperations.unsafeWrap(family))
              .setColumnQualifier(UnsafeByteOperations.unsafeWrap(qualifier)))
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

    try {
      Futures.addCallback(
          data_store.executor().readModifyWriteRowAsync(request),
          new IncrementCB(), data_store.pool());
    } catch (InterruptedException e) {
      return Deferred.fromError(new StorageException(
              "Unexpected exception from storage", e));
    }
    return deferred;
  }

  @Override
  protected Deferred<Boolean> cas(final byte[] key,
                                  final byte[] family,
                                  final byte[] qualifier,
                                  final byte[] value,
                                  final byte[] expected) {
    final CheckAndMutateRowRequest request =
            CheckAndMutateRowRequest.newBuilder()
                    .setTableNameBytes(UnsafeByteOperations.unsafeWrap(data_store.uidTable()))
                    .setRowKey(UnsafeByteOperations.unsafeWrap(key))
                    .setPredicateFilter(RowFilter.Chain.newBuilder()
                            .addFiltersBuilder()
                            .setChain(Chain.newBuilder()
                                    .addFilters(RowFilter.newBuilder()
                                            .setFamilyNameRegexFilterBytes(UnsafeByteOperations.unsafeWrap(family)))
                                    .addFilters(RowFilter.newBuilder()
                                            .setColumnQualifierRegexFilter(UnsafeByteOperations.unsafeWrap(qualifier)))
                                    .addFilters(RowFilter.newBuilder()
                                            .setCellsPerColumnLimitFilter(1))
                            )
                    )
                    .addFalseMutations(Mutation.newBuilder()
                            .setSetCell(SetCell.newBuilder()
                                    .setFamilyNameBytes(UnsafeByteOperations.unsafeWrap(family))
                                    .setColumnQualifier(UnsafeByteOperations.unsafeWrap(qualifier))
                                    .setValue(UnsafeByteOperations.unsafeWrap(value))
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


}
