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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueId;
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
 * UID object this meta data is linked with still exists. Then it will fetch the 
 * existing data and copy changes, overwriting the user fields if specific 
 * (e.g. via a PUT command). If overwriting is not called for (e.g. a POST was 
 * issued), then only the fields provided by the user will be saved, preserving 
 * all of the other fields in storage. Hence the need for the {@code changed} 
 * hash map and the {@link #syncMeta} method. 
 * <p>
 * Note that the HBase specific storage code will be removed once we have a DAL
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public class UIDMeta {
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
    changed.put("created", true);
  }
  
  /** @return a string with details about this object */
  @Override
  public String toString() {
    return "'" + type.toString() + ":" + uid + "'";
  }
  
  /**
   * Attempts a CompareAndSet storage call, loading the object from storage, 
   * synchronizing changes, and attempting a put.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * then the data will not be written.
   * @param tsdb The TSDB to use for storage access
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * @return True if the storage call was successful, false if the object was
   * modified in storage during the CAS call. If false, retry the call. Other 
   * failures will result in an exception being thrown.
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws NoSuchUniqueId If the UID does not exist
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws JSONException if the object could not be serialized
   */
  public Deferred<Boolean> syncToStorage(final TSDB tsdb, 
      final boolean overwrite) {
    if (uid == null || uid.isEmpty()) {
      throw new IllegalArgumentException("Missing UID");
    }
    if (type == null) {
      throw new IllegalArgumentException("Missing type");
    }

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
    
    /**
     * Callback used to verify that the UID to name mapping exists. Uses the TSD
     * for verification so the name may be cached. If the name does not exist
     * it will throw a NoSuchUniqueId and the meta data will not be saved to
     * storage
     */
    final class NameCB implements Callback<Deferred<Boolean>, String> {
      private final UIDMeta local_meta;
      
      public NameCB(final UIDMeta meta) {
        local_meta = meta;
      }
      
      /**
       *  Nested callback used to merge and store the meta data after verifying
       *  that the UID mapping exists. It has to access the {@code local_meta} 
       *  object so that's why it's nested within the NameCB class
       */
      final class StoreUIDMeta implements Callback<Deferred<Boolean>, 
        ArrayList<KeyValue>> {

        /**
         * Executes the CompareAndSet after merging changes
         * @return True if the CAS was successful, false if the stored data
         * was modified during flight.
         */
        @Override
        public Deferred<Boolean> call(final ArrayList<KeyValue> row) 
          throws Exception {
          
          final UIDMeta stored_meta;
          if (row == null || row.isEmpty()) {
            stored_meta = null;
          } else {
            stored_meta = JSON.parseToObject(row.get(0).value(), UIDMeta.class);
            stored_meta.initializeChangedMap();
          }
          
          final byte[] original_meta = row == null || row.isEmpty() ? 
              new byte[0] : row.get(0).value();

          if (stored_meta != null) {
            local_meta.syncMeta(stored_meta, overwrite);
          }
       
          // verify the name is set locally just to be safe
          if (name == null || name.isEmpty()) {
            local_meta.name = name;
          }
          
          final PutRequest put = new PutRequest(tsdb.uidTable(), 
              UniqueId.stringToUid(uid), FAMILY, 
              (type.toString().toLowerCase() + "_meta").getBytes(CHARSET), 
              local_meta.getStorageJSON());
          return tsdb.getClient().compareAndSet(put, original_meta);
        }
        
      }
      
      /**
       * NameCB method that fetches the object from storage for merging and
       * use in the CAS call
       * @return The results of the {@link #StoreUIDMeta} callback
       */
      @Override
      public Deferred<Boolean> call(final String name) throws Exception {
        
        final GetRequest get = new GetRequest(tsdb.uidTable(), 
            UniqueId.stringToUid(uid));
        get.family(FAMILY);
        get.qualifier((type.toString().toLowerCase() + "_meta").getBytes(CHARSET));
        
        // #2 deferred
        return tsdb.getClient().get(get)
          .addCallbackDeferring(new StoreUIDMeta());
      }
      
    }

    // start the callback chain by veryfing that the UID name mapping exists
    return tsdb.getUidName(type, UniqueId.stringToUid(uid))
      .addCallbackDeferring(new NameCB(this));
  }
  
  /**
   * Attempts to store a blank, new UID meta object in the proper location.
   * <b>Warning:</b> This should not be called by user accessible methods as it 
   * will overwrite any data already in the column. This method does not use 
   * a CAS, instead it uses a PUT to overwrite anything in the column.
   * @param tsdb The TSDB to use for calls
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws HBaseException if there was an issue writing to storage
   * @throws IllegalArgumentException if data was missing
   * @throws JSONException if the object could not be serialized
   */
  public Deferred<Object> storeNew(final TSDB tsdb) {
    if (uid == null || uid.isEmpty()) {
      throw new IllegalArgumentException("Missing UID");
    }
    if (type == null) {
      throw new IllegalArgumentException("Missing type");
    }
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Missing name");
    }

    final PutRequest put = new PutRequest(tsdb.uidTable(), 
        UniqueId.stringToUid(uid), FAMILY, 
        (type.toString().toLowerCase() + "_meta").getBytes(CHARSET), 
        UIDMeta.this.getStorageJSON());
    return tsdb.getClient().put(put);
  }
  
  /**
   * Attempts to delete the meta object from storage
   * @param tsdb The TSDB to use for access to storage
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if data was missing (uid and type)
   */
  public Deferred<Object> delete(final TSDB tsdb) {
    if (uid == null || uid.isEmpty()) {
      throw new IllegalArgumentException("Missing UID");
    }
    if (type == null) {
      throw new IllegalArgumentException("Missing type");
    }

    final DeleteRequest delete = new DeleteRequest(tsdb.uidTable(), 
        UniqueId.stringToUid(uid), FAMILY, 
        (type.toString().toLowerCase() + "_meta").getBytes(CHARSET));
    return tsdb.getClient().delete(delete);
  }
  
  /**
   * Convenience overload of {@code getUIDMeta(TSDB, UniqueIdType, byte[])}
   * @param tsdb The TSDB to use for storage access
   * @param type The type of UID to fetch
   * @param uid The ID of the meta to fetch
   * @return A UIDMeta from storage or a default
   * @throws HBaseException if there was an issue fetching
   * @throws NoSuchUniqueId If the UID does not exist
   */
  public static Deferred<UIDMeta> getUIDMeta(final TSDB tsdb, 
      final UniqueIdType type, final String uid) {
    return getUIDMeta(tsdb, type, UniqueId.stringToUid(uid));
  }
  
  /**
   * Verifies the UID object exists, then attempts to fetch the meta from 
   * storage and if not found, returns a default object.
   * <p>
   * The reason for returning a default object (with the type, uid and name set)
   * is due to users who may have just enabled meta data or have upgraded; we 
   * want to return valid data. If they modify the entry, it will write to 
   * storage. You can tell it's a default if the {@code created} value is 0. If
   * the meta was generated at UID assignment or updated by the meta sync CLI
   * command, it will have a valid created timestamp.
   * @param tsdb The TSDB to use for storage access
   * @param type The type of UID to fetch
   * @param uid The ID of the meta to fetch
   * @return A UIDMeta from storage or a default
   * @throws HBaseException if there was an issue fetching
   * @throws NoSuchUniqueId If the UID does not exist
   */
  public static Deferred<UIDMeta> getUIDMeta(final TSDB tsdb, 
      final UniqueIdType type, final byte[] uid) {
    
    /**
     * Callback used to verify that the UID to name mapping exists. Uses the TSD
     * for verification so the name may be cached. If the name does not exist
     * it will throw a NoSuchUniqueId and the meta data will not be returned. 
     * This helps in case the user deletes a UID but the meta data is still 
     * stored. The fsck utility can be used later to cleanup orphaned objects.
     */
    class NameCB implements Callback<Deferred<UIDMeta>, String> {

      /**
       * Called after verifying that the name mapping exists
       * @return The results of {@link #FetchMetaCB}
       */
      @Override
      public Deferred<UIDMeta> call(final String name) throws Exception {
        
        /**
         * Inner class called to retrieve the meta data after verifying that the
         * name mapping exists. It requires the name to set the default, hence
         * the reason it's nested.
         */
        class FetchMetaCB implements Callback<Deferred<UIDMeta>, 
          ArrayList<KeyValue>> {
  
          /**
           * Called to parse the response of our storage GET call after 
           * verification
           * @return The stored UIDMeta or a default object if the meta data
           * did not exist 
           */
          @Override
          public Deferred<UIDMeta> call(ArrayList<KeyValue> row) 
            throws Exception {
            
            if (row == null || row.isEmpty()) {
              // return the default
              final UIDMeta meta = new UIDMeta();
              meta.uid = UniqueId.uidToString(uid);
              meta.type = type;
              meta.name = name;
              return Deferred.fromResult(meta);
            }
            final UIDMeta meta = JSON.parseToObject(row.get(0).value(), 
                UIDMeta.class);
            
            // overwrite the name and UID
            meta.name = name;
            meta.uid = UniqueId.uidToString(uid);
            
            // fix missing types
            if (meta.type == null) {
              final String qualifier = 
                new String(row.get(0).qualifier(), CHARSET);
              meta.type = UniqueId.stringToUniqueIdType(qualifier.substring(0, 
                  qualifier.indexOf("_meta")));
            }
            meta.initializeChangedMap();
            return Deferred.fromResult(meta);
          }
          
        }
        
        final GetRequest get = new GetRequest(tsdb.uidTable(), uid);
        get.family(FAMILY);
        get.qualifier((type.toString().toLowerCase() + "_meta").getBytes(CHARSET));
        return tsdb.getClient().get(get).addCallbackDeferring(new FetchMetaCB());
      }
    }
    
    // verify that the UID is still in the map before fetching from storage
    return tsdb.getUidName(type, uid).addCallbackDeferring(new NameCB());
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
    if (meta.uid != null && !meta.uid.isEmpty()) {
      uid = meta.uid;
    }
    if (meta.name != null && !meta.name.isEmpty()) {
      name = meta.name;
    }
    if (meta.type != null) {
      type = meta.type;
    }
    if (meta.created > 0 && (meta.created < created || created == 0)) {
      created = meta.created;
    }
    
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
    changed.put("created", false); 
  }
  
  /**
   * Formats the JSON output for writing to storage. It drops objects we don't
   * need or want to store (such as the UIDMeta objects or the total dps) to
   * save space. It also serializes in order so that we can make a proper CAS
   * call. Otherwise the POJO serializer may place the fields in any order
   * and CAS calls would fail all the time.
   * @return A byte array to write to storage
   */
  private byte[] getStorageJSON() {
    // 256 bytes is a good starting value, assumes default info
    final ByteArrayOutputStream output = new ByteArrayOutputStream(256);
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output); 
      json.writeStartObject();
      json.writeStringField("type", type.toString());
      json.writeStringField("displayName", display_name);
      json.writeStringField("description", description);
      json.writeStringField("notes", notes);
      json.writeNumberField("created", created);
      if (custom == null) {
        json.writeNullField("custom");
      } else {
        json.writeObjectFieldStart("custom");
        for (Map.Entry<String, String> entry : custom.entrySet()) {
          json.writeStringField(entry.getKey(), entry.getValue());
        }
        json.writeEndObject();
      }
      
      json.writeEndObject(); 
      json.close();
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Unable to serialize UIDMeta", e);
    }
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
  public void setCustom(final Map<String, String> custom) {
    // equivalency of maps is a pain, users have to submit the whole map
    // anyway so we'll just mark it as changed every time we have a non-null
    // value
    if (this.custom != null || custom != null) {
      changed.put("custom", true);
      this.custom = new HashMap<String, String>(custom);
    }
  }

  /** @param created the created timestamp Unix epoch in seconds */
  public final void setCreated(final long created) {
    if (this.created != created) {
      changed.put("created", true);
      this.created = created;
    }
  }
}
