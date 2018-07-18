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

import java.util.Arrays;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Bytes;

/**
 * Provides a cache for unique IDs mapping them to and from strings.
 * 
 * @since 3.0
 */
public interface UniqueId {

  public static enum CacheMode {
    /** Populate the string to UID cache only for writers. */
    WRITE_ONLY("write_only"),
    
    /** Populate the UID to string cache only for readers. */
    READ_ONLY("read_only"),
    
    /** Populate both forward and reverse caches. */
    READ_WRITE("read_write");
    
    /** User friendly name of this enum. */
    private final String name;
    
    /**
     * Ctor package private for ^^.
     * @param name A non-null and non-empty name.
     */
    CacheMode(final String name) {
      this.name = name;
    }
    
    /** @return The user friendly name of the cache mode. */
    public String getName() {
      return name;
    }
    
    /**
     * Converts a string to the proper cache mode. 
     * @param name A non-null and non-empty name.
     * @return The cache mode enum if found.
     * @throws IllegalArgumentException if the name was null or empty or
     * the string didn't match an existing mode.
     */
    public static CacheMode fromString(final String name) {
      if (Strings.isNullOrEmpty(name)) {
        throw new IllegalArgumentException("Cache mode cannot be null "
            + "or empty.");
      }
      final String lc = name.trim().toLowerCase();
      if (lc.equals("w") || lc.equals("write") || lc.equals("write_only")) {
        return WRITE_ONLY;
      } else if (lc.equals("r") || lc.equals("read") || 
                 lc.equals("read_only")) {
        return READ_ONLY;
      } else if (lc.equals("rw") || lc.equals("wr") || 
                 lc.equals("readwrite") || lc.equals("writeread") || 
                 lc.equals("read_write") || lc.equals("write_read")) {
        return READ_WRITE;
      }
      throw new IllegalArgumentException("Unrecognized cache mode name.");
    }
  }
  
  /** 
   * @return The type of Unique ID this implementation works with.
   * @since 2.0 
   */
  public UniqueIdType type();
  
  /**
   * Causes this instance to discard all its in-memory caches.
   * 
   * @param span An optional tracing span.
   * @since 1.1
   */
  public void dropCaches(final Span span);
  
  /**
   * Attempts to match the UID to a string, checking the cache first then storage.
   * 
   * @param id A non-null and non-empty byte array ID associated with that name.
   * @param span An optional tracing span.
   * @return A deferred resolving to the string if found, {@link NoSuchUniqueId}
   * if not found (or null) or an exception from the data store.
   * @see #getOrCreateId(String, TimeSeriesStringId)
   * @see #throwsNoSuchUniques()
   * @throws IllegalArgumentException if the ID given in argument is encoded
   * on the wrong number of bytes or it was null or empty.
   * @since 3.0
   */
  public Deferred<String> getName(final byte[] uid, final Span span);
  
  /**
   * Finds the names associated with a given list of UIDs.
   * 
   * @param ids A non-null and non-empty list of non-null and non-empty byte 
   * array UIDs.
   * @param span An optional tracing span.
   * @return A deferred resolving to an array of strings if found, 
   * {@link NoSuchUniqueId} if one or more were not found (or null) or an 
   * exception from the data store.
   * @see #getOrCreateId(List, List)
   * @see #throwsNoSuchUniques()
   * @throws IllegalArgumentException if the ID given in argument is encoded
   * on the wrong number of bytes or it was null or empty.
   * @since 3.0
   */
  public Deferred<List<String>> getNames(final List<byte[]> uids, 
                                         final Span span);
  
  /**
   * Finds the Unique ID associated with the given name.
   * 
   * @param name A non-null and non-empty name.
   * @param span An optional tracing span.
   * @return A deferred resolving to the UID if found, a {@link NoSuchUniqueName} 
   * exception if the name did not exist (or null) or a data store exception.
   * @see #throwsNoSuchUniques()
   * @throws IllegalArgumentException if the name was null or empty.
   * @since 3.0
   */
  public Deferred<byte[]> getId(final String name, final Span span);
  
  /**
   * Finds the Unique IDs associated with the given names.
   * 
   * @param names A non-null and non-empty list of non-null and non-empty names.
   * @param span An optional tracing span
   * @return A deferred resolving to the UIDs if found, a {@link NoSuchUniqueName} 
   * exception if the name did not exist (or null) or a data store exception.
   * Note that the returned UID array will be the same length and in the same 
   * order as the given names list.
   * @see #throwsNoSuchUniques()
   * @throws IllegalArgumentException if the names were null or empty.
   * @since 3.0
   */
  public Deferred<List<byte[]>> getIds(final List<String> names, 
                                       final Span span);
  
  /**
   * Finds the ID associated with a given name or creates it.
   * <p>
   * The length of the byte array is fixed in advance by the implementation.
   *
   * @param auth A non-null auth state to perform authorization with.
   * @param name The name to lookup in the table or to assign an ID to.
   * @param id A non-null datum ID to use in debugging.
   * @param span An optional tracing span.
   * @return A deferred resolving to an IdOrError object reflecting if 
   * the resolution/creation was successful or not.
   * @since 3.0
   */
  public Deferred<IdOrError> getOrCreateId(final AuthState auth,
                                           final String name, 
                                           final TimeSeriesDatumId id,
                                           final Span span);
  
  /**
   * Finds the IDs associated with an array of names or creates them.
   * <p>
   * The length of the byte array is fixed in advance by the implementation.
   *
   * @param auth A non-null auth state to perform authorization with.
   * @param names The names to lookup in the table or to assign IDs to.
   * @param id A non-null datum ID to use in debugging.
   * @param span An optional tracing span.
   * @return A deferred resolving to IdOrError objects reflecting if 
   * the resolution/creation was successful or not.
   * @since 3.0
   */
  public Deferred<List<IdOrError>> getOrCreateIds(final AuthState auth,
                                                  final List<String> names, 
                                                  final TimeSeriesDatumId id,
                                                  final Span span);
  
  /**
   * Attempts to find suggestions of names given a search term.
   * 
   * @param search The search term (possibly empty).
   * @param max_results The maximum number of results to return. Must be a 
   * positive integer from 1 to {@link Integer#MAX_VALUE}.
   * @param span An optional tracing span.
   * @return A list of known valid names that have UIDs that sort of match
   * the search term.  If the search term is empty, returns the first few
   * terms. May resolve to an exception or an empty list.
   * @since 1.1
   */
  public Deferred<List<String>> suggest(final String search, 
                                        final int max_results,
                                        final Span span);
  
  /**
   * Reassigns the UID to a different name.
   * <p>
   * Whatever was the UID of {@code oldname} will be given to {@code newname}.
   * {@code oldname} will no longer be assigned a UID.
   * <p>
   * <b>Warning:</b> This operation does NOT update the cache of other TSD hosts
   * so it should be used only when the system is offline or all other TSDs have
   * had their cache's flushed.
   * Beware that the assignment change is <b>not atommic</b>. If two threads
   * or processes attempt to rename the same UID differently, the result is
   * unspecified and might even be inconsistent. This API is only here for
   * administrative purposes, not for normal programmatic interactions.
   * 
   * @param oldname The old name to rename.
   * @param newname The new name.
   * @return A deferred resolving to a null or a {@link NoSuchUniqueName} if 
   * {@code oldname} wasn't assigned or {@link IllegalStateException} if 
   * {@code newname} was already assigned (or another exception if the data
   * store ran into trouble).
   * @since 1.1
   */
  public Deferred<Object> rename(final String oldname, 
                                 final String newname, 
                                 final Span span);
  
  /**
   * Attempts to remove the mappings for the given string from the UID table
   * as well as the cache. If used, the caller should remove the entry from all
   * TSD caches as well.
   * <p>
   * WARNING: This is a best attempt only method in that we'll lookup the UID
   * for the given string, then issue two delete requests, one for each mapping.
   * If either mapping fails then the cache can be re-populated later on with
   * stale data. In that case, please run the FSCK utility.
   * <p>
   * WARNING 2: This method will NOT delete time series data or TSMeta data 
   * associated with the UIDs. It only removes them from the UID table. Deleting
   * a metric is generally safe as you won't query over it in the future. But
   * deleting tag keys or values can cause queries to fail if they find data
   * without a corresponding name.
   * 
   * @param name The name of the UID to delete
   * @param span An optional tracing span.
   * @return A deferred to wait on for completion. The result will be null if
   * successful, an exception otherwise such as {@link NoSuchUniqueName} if the 
   * UID string did not exist in storage
   * @since 2.2
   */
  public Deferred<Object> delete(final String name, final Span span);

  /**
   * Converts a byte array to a hex encoded, upper case string with padding
   * @param uid The ID to convert
   * @return the UID as a hex string
   * @throws NullPointerException if the ID was null
   * @since 2.0
   */
  public static String uidToString(final byte[] uid) {
    return DatatypeConverter.printHexBinary(uid);
  }
  
  /**
   * Converts a hex string to a byte array
   * If the {@code uid} is less than {@code uid_length * 2} characters wide, it
   * will be padded with 0s to conform to the spec. E.g. if the tagk width is 3
   * and the given {@code uid} string is "1", the string will be padded to 
   * "000001" and then converted to a byte array to reach 3 bytes. 
   * All {@code uid}s are padded to 1 byte. If given "1", and {@code uid_length}
   * is 0, the uid will be padded to "01" then converted.
   * @param uid The UID to convert
   * @return The UID as a byte array
   * @throws NullPointerException if the ID was null
   * @throws IllegalArgumentException if the string is not valid hex
   * @since 2.0
   */
  public static byte[] stringToUid(final String uid) {
    return stringToUid(uid, (short)0);
  }

  /**
   * Converts a UID to an integer value. The array must be the same length as
   * uid_length or an exception will be thrown.
   * @param uid The hex encoded UID to convert
   * @param uid_length Length the array SHOULD be according to the UID config
   * @return The UID converted to an integer
   * @throws IllegalArgumentException if the length of the byte array does not
   * match the uid_length value
   * @since 2.1
   */
  public static long uidToLong(final String uid, final short uid_length) {
    return uidToLong(stringToUid(uid), uid_length);
  }
  
  /**
   * Converts a UID to an integer value. The array must be the same length as
   * uid_length or an exception will be thrown.
   * @param uid The byte array to convert
   * @param uid_length Length the array SHOULD be according to the UID config
   * @return The UID converted to an integer
   * @throws IllegalArgumentException if the length of the byte array does not
   * match the uid_length value
   * @since 2.1
   */
  public static long uidToLong(final byte[] uid, final short uid_length) {
    if (uid.length != uid_length) {
      throw new IllegalArgumentException("UID was " + uid.length 
          + " bytes long but expected to be " + uid_length);
    }
    
    final byte[] uid_raw = new byte[8];
    System.arraycopy(uid, 0, uid_raw, 8 - uid_length, uid_length);
    return Bytes.getLong(uid_raw);
  }
 
  /**
   * Converts a Long to a byte array with the proper UID width
   * @param uid The UID to convert
   * @param width The width of the UID in bytes
   * @return The UID as a byte array
   * @throws IllegalStateException if the UID is larger than the width would
   * allow
   * @since 2.1
   */
  public static byte[] longToUID(final long uid, final short width) {
    // Verify that we're going to drop bytes that are 0.
    final byte[] padded = Bytes.fromLong(uid);
    for (int i = 0; i < padded.length - width; i++) {
      if (padded[i] != 0) {
        final String message = "UID " + Long.toString(uid) + 
          " was too large for " + width + " bytes";
        throw new IllegalStateException(message);
      }
    }
    // Shrink the ID on the requested number of bytes.
    return Arrays.copyOfRange(padded, padded.length - width, padded.length);
  }
  
  /**
   * Appends the given UID to the given string buffer, followed by "\\E".
   * @param buf The buffer to append
   * @param id The UID to add as a binary regex pattern
   * @since 2.1
   */
  public static void addIdToRegexp(final StringBuilder buf, final byte[] id) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
        // So we just terminated the quoted section because we just added \E
        // to `buf'.  So let's put a literal \E now and start quoting again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
    buf.append("\\E");
  }
  
  /**
   * Attempts to convert the given string to a type enumerator
   * @param type The string to convert
   * @return a valid UniqueIdType if matched
   * @throws IllegalArgumentException if the string did not match a type
   * @since 2.0
   */
  public static UniqueIdType stringToUniqueIdType(final String type) {
    if (type.toLowerCase().equals("metric") || 
        type.toLowerCase().equals("metrics")) {
      return UniqueIdType.METRIC;
    } else if (type.toLowerCase().equals("tagk")) {
      return UniqueIdType.TAGK;
    } else if (type.toLowerCase().equals("tagv")) {
      return UniqueIdType.TAGV;
    } else {
      throw new IllegalArgumentException("Invalid type requested: " + type);
    }
  }
  
  /**
   * Converts a hex string to a byte array
   * If the {@code uid} is less than {@code uid_length * 2} characters wide, it
   * will be padded with 0s to conform to the spec. E.g. if the tagk width is 3
   * and the given {@code uid} string is "1", the string will be padded to 
   * "000001" and then converted to a byte array to reach 3 bytes. 
   * All {@code uid}s are padded to 1 byte. If given "1", and {@code uid_length}
   * is 0, the uid will be padded to "01" then converted.
   * @param uid The UID to convert
   * @param uid_length An optional length, in bytes, that the UID must conform
   * to. Set to 0 if not used.
   * @return The UID as a byte array
   * @throws NullPointerException if the ID was null
   * @throws IllegalArgumentException if the string is not valid hex
   * @since 2.0
   */
  public static byte[] stringToUid(final String uid, final short uid_length) {
    if (uid == null || uid.isEmpty()) {
      throw new IllegalArgumentException("UID was empty");
    }
    String id = uid;
    if (uid_length > 0) {
      while (id.length() < uid_length * 2) {
        id = "0" + id;
      }
    } else {
      if (id.length() % 2 > 0) {
        id = "0" + id;
      }
    }
    return DatatypeConverter.parseHexBinary(id);
  }

}
