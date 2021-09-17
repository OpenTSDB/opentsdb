/*
 * // This file is part of OpenTSDB.
 * // Copyright (C) 2021  The OpenTSDB Authors.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //   http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package net.opentsdb.storage;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.Const;
import net.opentsdb.stats.Span;
import net.opentsdb.uid.Base1xUniqueIdStore;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.GetResultOrException;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class Tsdb1xUniqueIdStore extends Base1xUniqueIdStore {
  private static final Logger LOG = LoggerFactory.getLogger(Tsdb1xUniqueIdStore.class);

  private final Tsdb1xHBaseDataStore data_store;

  public Tsdb1xUniqueIdStore(final Tsdb1xHBaseDataStore data_store, final String id) {
    super(data_store.schema(), data_store.tsdb(), id);
    this.data_store = data_store;
  }

  @Override
  public Deferred<List<String>> suggest(UniqueIdType type, String query, int max) {
    return new SuggestCB(type, query, max).search();
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
              .withTag("dataStore", id)
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

    final List<GetRequest> requests = Lists.newArrayListWithCapacity(ids.size());
    for (final byte[] id : ids) {
      if (Bytes.isNullOrEmpty(id)) {
        throw new IllegalArgumentException("A null or empty ID was "
                + "found in the list.");
      }
      requests.add(new GetRequest(data_store.uidTable(),
              id,
              NAME_FAMILY,
              qualifier));
    }

    class ErrorCB implements Callback<List<byte[]>, Exception> {
      @Override
      public List<byte[]> call(Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
                  .log("Exception", ex)
                  .finish();
        }
        throw new StorageException("Failed to fetch names.", ex);
      }
    }

    class ResultCB implements Callback<List<String>, List<GetResultOrException>> {
      @Override
      public List<String> call(final List<GetResultOrException> results)
              throws Exception {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        if (results.size() != ids.size()) {
          throw new StorageException("Result size was: "
                  + results.size() + " when the names size was: "
                  + ids.size() + ". Should never happen!");
        }

        final List<String> names = Lists.newArrayListWithCapacity(results.size());
        for (int i = 0; i < results.size(); i++) {
          if (results.get(i).getException() != null) {
            if (child != null) {
              child.setErrorTags()
                      .log("Exception", results.get(i).getException())
                      .finish();
            }
            throw new StorageException("UID resolution failed for ID "
                    + UniqueId.uidToString(ids.get(i)), results.get(i).getException());
          } else if (results.get(i).getCells() == null ||
                  results.get(i).getCells().isEmpty()) {
            names.add(null);
          } else {
            names.add(new String(results.get(i).getCells().get(0).value(),
                    characterSet(type)));
          }
        }

        if (child != null) {
          child.setSuccessTags()
                  .finish();
        }
        return names;
      }
    }

    try {
      return data_store.client().get(requests)
              .addCallbacks(new ResultCB(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
                .log("Exception", e)
                .finish();
      }
      return Deferred.fromError(new StorageException(
              "Unexpected exception from storage", e));
    }
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
              .withTag("dataStore", id)
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

    final List<GetRequest> requests = Lists.newArrayListWithCapacity(names.size());
    for (final String name : names) {
      if (Strings.isNullOrEmpty(name)) {
        throw new IllegalArgumentException("A null or empty name was "
                + "found in the list.");
      }
      requests.add(new GetRequest(data_store.uidTable(),
              name.getBytes(characterSet(type)),
              ID_FAMILY,
              qualifier));
    }

    class ErrorCB implements Callback<List<byte[]>, Exception> {
      @Override
      public List<byte[]> call(Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags()
                  .log("Exception", ex)
                  .finish();
        }
        throw new StorageException("Failed to fetch ID.", ex);
      }
    }

    class ResultCB implements Callback<List<byte[]>, List<GetResultOrException>> {
      @Override
      public List<byte[]> call(final List<GetResultOrException> results)
              throws Exception {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        if (results.size() != names.size()) {
          throw new StorageException("Result size was: "
                  + results.size() + " when the names size was: "
                  + names.size() + ". Should never happen!");
        }

        final List<byte[]> uids = Lists.newArrayListWithCapacity(results.size());
        for (int i = 0; i < results.size(); i++) {
          if (results.get(i).getException() != null) {
            if (child != null) {
              child.setErrorTags()
                      .log("Exception", results.get(i).getException())
                      .finish();
            }
            throw new StorageException("UID resolution failed for name "
                    + names.get(i), results.get(i).getException());
          } else if (results.get(i).getCells() != null &&
                  !results.get(i).getCells().isEmpty()) {
            uids.add(results.get(i).getCells().get(0).value());
          } else {
            uids.add(null);
          }
        }

        if (child != null) {
          child.setSuccessTags()
                  .finish();
        }
        return uids;
      }
    }

    try {
      return data_store.client().get(requests)
              .addCallbacks(new ResultCB(), new ErrorCB());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
                .log("Exception", e)
                .finish();
      }
      return Deferred.fromError(new StorageException(
              "Unexpected exception from storage", e));
    }
  }

  @Override
  protected Deferred<byte[]> get(final UniqueIdType type,
                               final byte[] key,
                               final byte[] family) {
    final GetRequest get = new GetRequest(
            data_store.uidTable(), key, family);

    switch(type) {
      case METRIC:
        get.qualifier(METRICS_QUAL);
        break;
      case TAGK:
        get.qualifier(TAG_NAME_QUAL);
        break;
      case TAGV:
        get.qualifier(TAG_VALUE_QUAL);
        break;
      default:
        throw new IllegalArgumentException("This data store does not "
                + "handle Unique IDs of type: " + type);
    }

    class GetCB implements Callback<byte[], ArrayList<KeyValue>> {
      public byte[] call(final ArrayList<KeyValue> row) {
        if (row == null || row.isEmpty()) {
          return null;
        }
        return row.get(0).value();
      }

      @Override
      public String toString() {
        return "HBase UniqueId Get Request Callback";
      }
    }
    return data_store.client().get(get).addCallback(new GetCB());
  }

  @Override
  protected Deferred<Long> increment(byte[] key, byte[] family, byte[] qualifier) {
    final AtomicIncrementRequest inc = new AtomicIncrementRequest(data_store.uidTable(), key, family, qualifier);
    return data_store.client().atomicIncrement(inc);
  }

  @Override
  protected Deferred<Boolean> cas(byte[] key, byte[] family, byte[] qualifier, byte[] value, byte[] expected) {
    final PutRequest put = new PutRequest(data_store.uidTable(), key, family, qualifier, value);
    return data_store.client().compareAndSet(put, expected);
  }

  /**
   * Creates a scanner that scans the right range of rows for suggestions.
   * @param client The HBase client to use.
   * @param tsd_uid_table Table where IDs are stored.
   * @param search The string to start searching at
   * @param kind_or_null The kind of UID to search or null for any kinds.
   * @param max_results The max number of results to return
   */
  private static Scanner getSuggestScanner(final HBaseClient client,
                                           final byte[] tsd_uid_table,
                                           final String search,
                                           final byte[] kind_or_null,
                                           final int max_results) {
    final byte[] start_row;
    final byte[] end_row;
    if (search == null || search.isEmpty()) {
      start_row = START_ROW;
      end_row = END_ROW;
    } else {
      start_row = search.getBytes(StandardCharsets.US_ASCII);
      end_row = Arrays.copyOf(start_row, start_row.length);
      end_row[start_row.length - 1]++;
    }
    final Scanner scanner = client.newScanner(tsd_uid_table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(ID_FAMILY);
    if (kind_or_null != null) {
      scanner.setQualifier(kind_or_null);
    }
    scanner.setMaxNumRows(max_results <= 4096 ? max_results : 4096);
    return scanner;
  }

  /**
   * Helper callback to asynchronously scan HBase for suggestions.
   */
  private final class SuggestCB
    implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
    private final LinkedList<String> suggestions = new LinkedList<String>();
    private final Scanner scanner;
    private final int max_results;
    private final UniqueIdType type;

    SuggestCB(final UniqueIdType type,
              final String search,
              final int max_results) {
      this.max_results = max_results;
      this.type = type;
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
      this.scanner = getSuggestScanner(
              data_store.client(),
              data_store.uidTable(),
              search,
              kind,
              max_results);
    }

    @SuppressWarnings("unchecked")
    Deferred<List<String>> search() {
      return (Deferred) scanner.nextRows().addCallback(this);
    }

    public Object call(final ArrayList<ArrayList<KeyValue>> rows) {
      if (rows == null) {  // We're done scanning.
        return suggestions;
      }

      for (final ArrayList<KeyValue> row : rows) {
        if (row.size() != 1) {
          LOG.error("WTF shouldn't happen!  Scanner " + scanner + " returned"
                    + " a row that doesn't have exactly 1 KeyValue: " + row);
          if (row.isEmpty()) {
            continue;
          }
        }
        final byte[] key = row.get(0).key();
        final String name = new String(key, Const.ASCII_CHARSET);
        final byte[] id = row.get(0).value();
        schema.addToCache(type, name, id);
        suggestions.add(name);
        if ((short) suggestions.size() >= max_results) {  // We have enough.
          return scanner.close().addCallback(new Callback<Object, Object>() {
            @Override
            public Object call(Object ignored) throws Exception {
              return suggestions;
            }
          });
        }
      }
      return search();  // Get more suggestions.
    }
  }
}
