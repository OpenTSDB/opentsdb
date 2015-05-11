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

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import net.opentsdb.uid.IdUtils;
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
  
  /** A short description of what this object represents */
  private String description;
  
  /** A timestamp of when this UID was first recorded by OpenTSDB in seconds */
  private long created;

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
    this(uid, type, name, null, System.currentTimeMillis() / 1000);
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
    }
  }

  public UIDMeta(final byte[] uid,
                 final UniqueIdType type,
                 final String name,
                 final String description,
                 final long created) {
    this.type = checkNotNull(type);

    checkArgument(type.width == uid.length, "UID length must match the UID " +
            "type width");
    this.uid = uid;

    this.name = name;
    this.description = description;
    this.created = created;
  }

  /**
   * @return a string with details about this object
   */
  @Override
  public String toString() {
    return "'" + type + ":" + IdUtils.uidToLong(uid) + "'";
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

  /** @return optional description */
  public String getDescription() {
    return description;
  }

  /** @return when the UID was first assigned, may be 0 if unknown */
  public long getCreated() {
    return created;
  }

  /** @param description an optional description of the UID */
  public void setDescription(final String description) {
    if (!Objects.equal(this.description, description)) {
      this.description = description;
    }
  }

  /** @param created the created timestamp Unix epoch in seconds */
  public final void setCreated(final long created) {
    if (this.created != created) {
      this.created = created;
    }
  }
}
