// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.meta;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

/**
 * UIDMeta objects are associated with the UniqueId of metrics, tag names
 * or tag values. When a new metric, tagk or tagv is generated, a UIDMeta object
 * will also be written to storage with only the uid, type and name filled out. 
 * <p>
 * Users are allowed to edit the following fields:
 * <ul><li>display_name</li>
 * <li>description</li>
 * <li>notes</li>
 * <li>custom</li></ul>
 * The {@code name}, {@code uid}, {@code type} and {@code created} fields can 
 * only be modified by the system and are usually done so on object creation.
 * <p>
 * When you call {@link #syncToStorage} on this object, it will verify that the
 * UID object this meta data is linked with still exists. Then it will lock the 
 * row in the UID table, fetch the existing data and copy changes, overwriting
 * the user fields if specific (e.g. via a PUT command). If overwriting is not
 * called for (e.g. a POST was issued), then only the fields provided by the 
 * user will be saved, preserving all of the other fields in storage. Hence the
 * need for the {@code changed} hash map and the {@link #syncMeta} method. 
 * <p>
 * Note that the HBase specific storage code will be removed once we have a DAL
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public final class UIDMeta {
  private static final Logger LOG = LoggerFactory.getLogger(UIDMeta.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  
  /** The single column family used by this class. */
  private static final byte[] FAMILY = "name".getBytes(CHARSET);
  
  /** A hexadecimal representation of the UID this metadata is associated with */
  private String uid = "";
  
  /** The type of UID this metadata represents */
  @JsonDeserialize(using = JSON.UniqueIdTypeDeserializer.class)
  private UniqueIdType type = null;
  
  /** 
   * This is the identical name of what is stored in the UID table
   * It cannot be overridden 
   */
  private String name = "";
  
  /** 
   * An optional, user supplied name used for display purposes only
   * If this field is empty, the {@link name} field should be used
   */
  private String display_name = "";
  
  /** A short description of what this object represents */
  private String description = "";
  
  /** Optional, detailed notes about what the object represents */
  private String notes = "";
  
  /** A timestamp of when this UID was first recorded by OpenTSDB in seconds */
  private long created = 0;
  
  /** Optional user supplied key/values */
  private HashMap<String, String> custom = null;
  
  /** Tracks fields that have changed by the user to avoid overwrites */
  private final HashMap<String, Boolean> changed = 
    new HashMap<String, Boolean>();
  
  /**
   * Default constructor
   * Initializes the the changed map
   */
  public UIDMeta() {
    initializeChangedMap();
  }
 
  /**
   * Constructor used for overwriting. Will not reset the name or created values
   * in storage.
   * @param type Type of UID object
   * @param uid UID of the object
   */
  public UIDMeta(final UniqueIdType type, final String uid) {
    this.type = type;
    this.uid = uid;
    initializeChangedMap();
  }
  
  /**
   * Constructor used by TSD only to create a new UID with the given data and 
   * the current system time for {@code createdd}
   * @param type Type of UID object
   * @param uid UID of the object
   * @param name Name of the UID
   */
  public UIDMeta(final UniqueIdType type, final byte[] uid, final String name) {
    this.type = type;
    this.uid = UniqueId.uidToString(uid);
    this.name = name;
    created = System.currentTimeMillis() / 1000;
    initializeChangedMap();
  }
  
  /** @return a string with details about this object */
  @Override
  public String toString() {
    return "'" + type.toString() + ":" + uid + "'";
  }
  
  /**
   * Attempts an atomic write to storage, loading the object first and copying
   * any changes while holding a lock on the row. After calling, this object
   * will have data loaded from storage.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * then the data will not be written.
   * @param tsdb The TSDB to use for storage access
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws JSONException if the object could not be serialized
   */
  public void syncToStorage(final TSDB tsdb, final boolean overwrite) {
    if (uid == null || uid.isEmpty()) {
      throw new IllegalArgumentException("Missing UID");
    }
    if (type == null) {
      throw new IllegalArgumentException("Missing type");
    }

    // verify that the UID is still in the map before bothering with meta
    final String name = tsdb.getUidName(type, UniqueId.stringToUid(uid));
    
    boolean has_changes = false;
    for (Map.Entry<String, Boolean> entry : changed.entrySet()) {
      if (entry.getValue()) {
        has_changes = true;
        break;
      }
    }
    if (!has_changes) {
      LOG.debug(this + " does not have changes, skipping sync to storage");
      throw new IllegalStateException("No changes detected in UID meta data");
    }
    
    final RowLock lock = tsdb.hbaseAcquireLock(tsdb.uidTable(), 
        UniqueId.stringToUid(uid), (short)3);
    try {
      final UIDMeta stored_meta = 
        getFromStorage(tsdb, type, UniqueId.stringToUid(uid), lock);
      if (stored_meta != null) {
        syncMeta(stored_meta, overwrite);
      }
      
      // verify the name is set locally just to be safe
      if (name == null || name.isEmpty()) {
        this.name = name;
      }
      final PutRequest put = new PutRequest(tsdb.uidTable(), 
          UniqueId.stringToUid(uid), FAMILY, 
          (type.toString().toLowerCase() + "_meta").getBytes(CHARSET), 
          JSON.serializeToBytes(this), lock);
      tsdb.hbasePutWithRetry(put, (short)3, (short)800);
      
    } finally {
      // release the lock!
      try {
        tsdb.getClient().unlockRow(lock);
      } catch (HBaseException e) {
        LOG.error("Error while releasing the lock on row: " + uid, e);
      }
    }
  }
  
  /**
   * Attempts to delete the meta object from storage
   * @param tsdb The TSDB to use for access to storage
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if data was missing (uid and type)
   */
  public void delete(final TSDB tsdb) {
    if (uid == null || uid.isEmpty()) {
      throw new IllegalArgumentException("Missing UID");
    }
    if (type == null) {
      throw new IllegalArgumentException("Missing type");
    }

    final DeleteRequest delete = new DeleteRequest(tsdb.uidTable(), 
        UniqueId.stringToUid(uid), FAMILY, 
        (type.toString().toLowerCase() + "_meta").getBytes(CHARSET));
    try {
      tsdb.getClient().delete(delete);
    } catch (Exception e) {
      throw new RuntimeException("Unable to delete UID", e);
    }
  }
  
  /**
   * Verifies the UID object exists, then attempts to return the meta from 
   * storage and if not found, returns a default object.
   * <p>
   * The reason for returning a default object (with the type, uid and name set)
   * is due to users who may have just enabled meta data or have upgraded we 
   * want to return valid data. If they modify the entry, it will write to 
   * storage. You can tell it's a default if the {@code created} value is 0. If
   * the meta was generated at UID assignment or updated by the meta sync CLI
   * command, it will have a valid timestamp.
   * @param tsdb The TSDB to use for storage access
   * @param type The type of UID to fetch
   * @param uid The ID of the meta to fetch
   * @return A UIDMeta from storage or a default
   * @throws HBaseException if there was an issue fetching
   */
  public static UIDMeta getUIDMeta(final TSDB tsdb, final UniqueIdType type, 
      final String uid) {
    return getUIDMeta(tsdb, type, UniqueId.stringToUid(uid));
  }
  
  /**
   * Verifies the UID object exists, then attempts to return the meta from 
   * storage and if not found, returns a default object.
   * <p>
   * The reason for returning a default object (with the type, uid and name set)
   * is due to users who may have just enabled meta data or have upgraded we 
   * want to return valid data. If they modify the entry, it will write to 
   * storage. You can tell it's a default if the {@code created} value is 0. If
   * the meta was generated at UID assignment or updated by the meta sync CLI
   * command, it will have a valid timestamp.
   * @param tsdb The TSDB to use for storage access
   * @param type The type of UID to fetch
   * @param uid The ID of the meta to fetch
   * @return A UIDMeta from storage or a default
   * @throws HBaseException if there was an issue fetching
   */
  public static UIDMeta getUIDMeta(final TSDB tsdb, final UniqueIdType type, 
      final byte[] uid) {
    // verify that the UID is still in the map before bothering with meta
    final String name = tsdb.getUidName(type, uid);
    
    UIDMeta meta;
    try {
      meta = getFromStorage(tsdb, type, uid, null);
      if (meta != null) {
        meta.initializeChangedMap();
        return meta;
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Unable to parse meta for '" + type + ":" + uid + 
          "', returning default", e);
    } catch (JSONException e) {
      LOG.error("Unable to parse meta for '" + type + ":" + uid + 
          "', returning default", e);
    }
    
    meta = new UIDMeta();
    meta.uid = UniqueId.uidToString(uid);
    meta.type = type;
    meta.name = name;
    return meta;
  }
    
  /**
   * Attempts to fetch metadata from storage for the given type and UID
   * @param tsdb The TSDB to use for storage access
   * @param type The UIDMeta type, either "metric", "tagk" or "tagv"
   * @param uid The UID of the meta to fetch
   * @param lock An optional lock when performing an atomic update, pass null
   * if not needed.
   * @return A UIDMeta object if found, null if the data was not found
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the data was corrupted
   */
  private static UIDMeta getFromStorage(final TSDB tsdb, 
      final UniqueIdType type, final byte[] uid, final RowLock lock) {
    
    final GetRequest get = new GetRequest(tsdb.uidTable(), uid);
    get.family(FAMILY);
    get.qualifier((type.toString().toLowerCase() + "_meta").getBytes(CHARSET));
    if (lock != null) {
      get.withRowLock(lock);
    }
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return null;
      }
      return JSON.parseToObject(row.get(0).value(), UIDMeta.class);
    } catch (HBaseException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (JSONException e) {
        throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Syncs the local object with the stored object for atomic writes, 
   * overwriting the stored data if the user issued a PUT request
   * <b>Note:</b> This method also resets the {@code changed} map to false
   * for every field
   * @param meta The stored object to sync from
   * @param overwrite Whether or not all user mutable data in storage should be
   * replaced by the local object
   */
  private void syncMeta(final UIDMeta meta, final boolean overwrite) {
    // copy non-user-accessible data first
    uid = meta.uid;
    if (meta.name != null && !meta.name.isEmpty()) {
      name = meta.name;
    }
    if (meta.type != null) {
      type = meta.type;
    }
    created = meta.created;
    
    // handle user-accessible stuff
    if (!overwrite && !changed.get("display_name")) {
      display_name = meta.display_name;
    }
    if (!overwrite && !changed.get("description")) {
      description = meta.description;
    }
    if (!overwrite && !changed.get("notes")) {
      notes = meta.notes;
    }
    if (!overwrite && !changed.get("custom")) {
      custom = meta.custom;
    }

    // reset changed flags
    initializeChangedMap();
  }
  
  /**
   * Sets or resets the changed map flags
   */
  private void initializeChangedMap() {
    // set changed flags
    changed.put("display_name", false);
    changed.put("description", false);
    changed.put("notes", false);
    changed.put("custom", false);
  }
  
  // Getters and Setters --------------
  
  /** @return the uid as a hex encoded string */
  public String getUID() {
    return uid;
  }

  /** @return the type of UID represented */
  public UniqueIdType getType() {
    return type;
  }

  /** @return the name of the UID object */
  public String getName() {
    return name;
  }

  /** @return optional display name, use {@code name} if empty */
  public String getDisplayName() {
    return display_name;
  }

  /** @return optional description */
  public String getDescription() {
    return description;
  }

  /** @return optional notes */
  public String getNotes() {
    return notes;
  }

  /** @return when the UID was first assigned, may be 0 if unknown */
  public long getCreated() {
    return created;
  }

  /** @return optional map of custom values from the user */
  public Map<String, String> getCustom() {
    return custom;
  }

  /** @param display_name an optional descriptive name for the UID */
  public void setDisplayName(final String display_name) {
    if (!this.display_name.equals(display_name)) {
      changed.put("display_name", true);
      this.display_name = display_name;
    }
  }

  /** @param description an optional description of the UID */
  public void setDescription(final String description) {
    if (!this.description.equals(description)) {
      changed.put("description", true);
      this.description = description;
    }
  }

  /** @param notes optional notes */
  public void setNotes(final String notes) {
    if (!this.notes.equals(notes)) {
      changed.put("notes", true);
      this.notes = notes;
    }
  }

  /** @param custom the custom to set */
  public void setCustom(final HashMap<String, String> custom) {
    // equivalency of maps is a pain, users have to submit the whole map
    // anyway so we'll just mark it as changed every time we have a non-null
    // value
    if (this.custom != null || custom != null) {
      changed.put("custom", true);
      this.custom = custom;
    }
  }
}
