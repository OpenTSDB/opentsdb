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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import net.opentsdb.uid.UniqueIdType;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
public final class UIDMeta {
  /** A hexadecimal representation of the UID this metadata is associated with */
  private byte[] uid;

  private UniqueIdType type;
  
  /** 
   * This is the identical name of what is stored in the UID table
   * It cannot be overridden 
   */
  private String name;
  
  /** 
   * An optional, user supplied name used for display purposes only
   * If this field is empty, the {@link name} field should be used
   */
  private String display_name;
  
  /** A short description of what this object represents */
  private String description;
  
  /** Optional, detailed notes about what the object represents */
  private String notes;
  
  /** A timestamp of when this UID was first recorded by OpenTSDB in seconds */
  private long created = 0;
  
  /** Optional user supplied key/values */
  private Map<String, String> custom;
  
  /** Tracks fields that have changed by the user to avoid overwrites */
  private final Set<String> changed = Sets.newHashSetWithExpectedSize(5);

  /**
   * Constructor used for overwriting. Will not reset the name or created values
   * in storage.
   * @param type Type of UID object
   * @param uid UID of the object
   */
  public UIDMeta(final UniqueIdType type, final byte[] uid) {
    this(type, uid, null, false);
  }
  
  /**
   * Constructor used by TSD only to create a new UID with the given data and 
   * the current system time for {@code createdd}
   * @param type Type of UID object
   * @param uid UID of the object
   * @param name Name of the UID
   */
  public UIDMeta(final UniqueIdType type, final byte[] uid, final String name) {
    this(uid, type, name, null, null, null, null, System.currentTimeMillis() / 1000);
    changed.add("created");
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
    this.type = checkNotNull(type);

    checkArgument(type.width == uid.length, "UID length must match the UID " +
            "type width");
    this.uid = uid;

    this.name = name;

    if (created) {
      this.created = System.currentTimeMillis() / 1000;
      changed.add("created");
    }
  }

  public UIDMeta(final byte[] uid,
                 final UniqueIdType type,
                 final String name,
                 final String display_name,
                 final String description,
                 final String notes,
                 final Map<String, String> custom,
                 final long created) {
    this.type = checkNotNull(type);

    checkArgument(type.width == uid.length, "UID length must match the UID " +
            "type width");
    this.uid = uid;

    this.name = name;
    this.display_name = display_name;
    this.description = description;
    this.notes = notes;
    this.custom = custom;
    this.created = created;
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
    if (meta.name != null && !meta.name.isEmpty()) {
      name = meta.name;
    }
    if (meta.created > 0 && (meta.created < created || created == 0)) {
      created = meta.created;
    }
    
    // handle user-accessible stuff
    if (!overwrite && !changed.contains("display_name")) {
      display_name = meta.display_name;
    }
    if (!overwrite && !changed.contains("description")) {
      description = meta.description;
    }
    if (!overwrite && !changed.contains("notes")) {
      notes = meta.notes;
    }
    if (!overwrite && !changed.contains("custom")) {
      custom = meta.custom;
    }

    // reset changed flags
    resetChangedMap();
  }
  
  /**
   * Sets or resets the changed map flags
   */
  public void resetChangedMap() {
    changed.clear();
  }
  
  // Getters and Setters --------------
  
  /** @return the uid as a hex encoded string */
  public byte[] getUID() {
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

  /**
   * @param display_name an optional descriptive name for the UID
   */
  public void setDisplayName(final String display_name) {
    if (!Objects.equal(this.display_name, display_name)) {
      changed.add("display_name");
      this.display_name = display_name;
    }
  }

  /** @param description an optional description of the UID */
  public void setDescription(final String description) {
    if (!Objects.equal(this.description, description)) {
      changed.add("description");
      this.description = description;
    }
  }

  /** @param notes optional notes */
  public void setNotes(final String notes) {
    if (!Objects.equal(this.notes, notes)) {
      changed.add("notes");
      this.notes = notes;
    }
  }

  /** @param custom the custom to set */
  public void setCustom(final Map<String, String> custom) {
    // equivalency of maps is a pain, users have to submit the whole map
    // anyway so we'll just mark it as changed every time we have a non-null
    // value
    if (this.custom != null || custom != null) {
      changed.add("custom");
      this.custom = new HashMap<String, String>(custom);
    }
  }

  /** @param created the created timestamp Unix epoch in seconds */
  public final void setCreated(final long created) {
    if (this.created != created) {
      changed.add("created");
      this.created = created;
    }
  }

  public boolean hasChanges() {
    return !changed.isEmpty();
  }
}
