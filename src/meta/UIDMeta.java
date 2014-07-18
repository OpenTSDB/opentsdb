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
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;

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
public final class UIDMeta {
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

  /**
   * Constructor used by TSD only to create a new UID with the given data and
   * the current system time for {@code createdd}
   * @param type Type of UID object
   * @param uid UID of the object
   * @param name Name of the UID
   * @param created If this object should be considered newly created.
   */
  public UIDMeta(final UniqueIdType type, final byte[] uid,
                 final String name, final boolean created) {
    this.type = type;
    this.uid = UniqueId.uidToString(uid);
    this.name = name;
    initializeChangedMap();

    if (created)
      this.created = System.currentTimeMillis() / 1000;

    changed.put("created", created);
  }
  /**
   * Constructor used by TSD only to create a new UID with the given data and
   * the current system time for {@code createdd}
   * @param type Type of UID object
   * @param uid UID of the object
   * @param name Name of the UID
   * @param created If this object should be considered newly created.
   */
  public static UIDMeta buildFromJSON(byte[] json, UniqueIdType type, byte[] uid,
                              String name) {
    UIDMeta meta = JSON.parseToObject(json, UIDMeta.class);

    meta.type = type;
    meta.uid = UniqueId.uidToString(uid);
    meta.name = name;

    meta.initializeChangedMap();

    return meta;
  }


  /**
   * @return a string with details about this object
   */
  @Override
  public String toString() {
    return "'" + type.toString() + ":" + uid + "'";
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
  public void syncMeta(final UIDMeta meta, final boolean overwrite) {
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
  public void initializeChangedMap() {
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
  public byte[] getStorageJSON() {
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

  public Map<String,Boolean> getChanged() {
    return ImmutableMap.copyOf(changed);
  }

  /**
   * @param display_name an optional descriptive name for the UID
   */
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
