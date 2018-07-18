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
package net.opentsdb.uid;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.StorageException;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * A class to help test UID schemas.
 */
public class MockUIDStore implements UniqueIdStore {

  private final Charset charset;
  private final Map<UniqueIdType, ByteSet> id_to_ex;
  private final Map<UniqueIdType, ByteMap<String>> id_to_string;
  private final Map<UniqueIdType, Set<String>> name_to_ex;
  private final Map<UniqueIdType, Map<String, byte[]>> name_to_id;
  
  /**
   * Default ctor.
   * @param charset a non-null character set.
   */
  public MockUIDStore(final Charset charset) {
    this.charset = charset;
    id_to_ex = Maps.newHashMapWithExpectedSize(UniqueIdType.values().length);
    id_to_string = Maps.newHashMapWithExpectedSize(UniqueIdType.values().length);
    name_to_ex = Maps.newHashMapWithExpectedSize(UniqueIdType.values().length);
    name_to_id = Maps.newHashMapWithExpectedSize(UniqueIdType.values().length);
    
    for (final UniqueIdType type : UniqueIdType.values()) {
      id_to_ex.put(type, new ByteSet());
      id_to_string.put(type, new ByteMap<String>());
      name_to_ex.put(type, Sets.newHashSet());
      name_to_id.put(type, Maps.newHashMap());
    }
  }
  
  /**
   * Sets an exception to be returned when the name is fetched.
   * @param type The type to store the exception under.
   * @param name The name of the string to fail on.
   */
  public void addException(final UniqueIdType type, final String name) {
    name_to_ex.get(type).add(name);
  }
  
  /**
   * Sets an exception to be returned when the id is fetched.
   * @param type The type to store the exception under.
   * @param id The id of the UID to fail on.
   */
  public void addException(final UniqueIdType type, final byte[] id) {
    id_to_ex.get(type).add(id);
  }
  
  /**
   * Adds forward and reverse mappings for the name/id pair.
   * @param type The type to store the exception under.
   * @param name The name to store.
   * @param id The ID to store.
   */
  public void addBoth(final UniqueIdType type, final String name, final byte[] id) {
    id_to_string.get(type).put(id, name);
    name_to_id.get(type).put(name, id);
  }
  
  /**
   * Adds just the forward mapping of name to ID.
   * @param type The type to store the exception under.
   * @param name The name to store.
   * @param id The ID to store.
   */
  public void add(final UniqueIdType type, final String name, final byte[] id) {
    name_to_id.get(type).put(name, id);
  }
  
  /**
   * Adds just the reverse mapping of the ID to name.
   * @param type The type to store the exception under.
   * @param id The ID to store.
   * @param name The name to store.
   */
  public void add(final UniqueIdType type, final byte[] id, final String name) {
    id_to_string.get(type).put(id, name);
  }
  
  @Override
  public Deferred<byte[]> getId(UniqueIdType type, String name, Span span) {
    if (name_to_ex.get(type).contains(name)) {
      return Deferred.fromError(new StorageException("Boo!"));
    }
    return Deferred.fromResult(name_to_id.get(type).get(name));
  }
  
  @Override
  public Deferred<List<byte[]>> getIds(UniqueIdType type, List<String> names,
      Span span) {
    final List<byte[]> uids = Lists.newArrayListWithCapacity(names.size());
    for (final String name : names) {
      if (name_to_ex.get(type).contains(name)) {
        return Deferred.fromError(new StorageException("Boo!"));
      }
      uids.add(name_to_id.get(type).get(name));
    }
    return Deferred.fromResult(uids);
  }
  
  @Override
  public Deferred<IdOrError> getOrCreateId(final AuthState auth,
      final UniqueIdType type, 
      final String name,
      final TimeSeriesDatumId id,
      final Span span) {
    if (name_to_ex.get(type).contains(name)) {
      return Deferred.fromError(new StorageException("Boo!"));
    }
    
    final byte[] uid = name_to_id.get(type).get(name);
    if (uid != null) {
      return Deferred.fromResult(IdOrError.wrapId(uid));
    }
    
    return Deferred.fromResult(IdOrError.wrapRejected("Mock can't assign: " + name));
  }
  
  @Override
  public Deferred<List<IdOrError>> getOrCreateIds(final AuthState auth,
      final UniqueIdType type, 
      final List<String> names,
      final TimeSeriesDatumId id,
      final Span span) {
    final List<IdOrError> uids = Lists.newArrayListWithCapacity(names.size());
    for (final String name : names) {
      if (name_to_ex.get(type).contains(name)) {
        return Deferred.fromError(new StorageException("Boo!"));
      }
      final byte[] uid = name_to_id.get(type).get(name);
      if (uid != null) {
        uids.add(IdOrError.wrapId(uid));
      } else {
        uids.add(IdOrError.wrapRejected("Mock can't assign: " + name));
      }
    }
    return Deferred.fromResult(uids);
  }
  
  @Override
  public Deferred<String> getName(UniqueIdType type, byte[] id, Span span) {
    if (id_to_ex.get(type).contains(id)) {
      return Deferred.fromError(new StorageException("Boo!"));
    }
    return Deferred.fromResult(id_to_string.get(type).get(id));
  }
  
  @Override
  public Deferred<List<String>> getNames(UniqueIdType type, List<byte[]> ids,
      Span span) {
    final List<String> names = Lists.newArrayListWithCapacity(ids.size());
    for (final byte[] id : ids) {
      if (id_to_ex.get(type).contains(id)) {
        return Deferred.fromError(new StorageException("Boo!"));
      }
      names.add(id_to_string.get(type).get(id));
    }
    return Deferred.fromResult(names);
  }
  
  @Override
  public Charset characterSet(final UniqueIdType type) {
    return charset;
  }
  
}
