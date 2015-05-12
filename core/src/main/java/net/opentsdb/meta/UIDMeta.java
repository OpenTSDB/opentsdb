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

import com.google.auto.value.AutoValue;

import net.opentsdb.uid.UniqueIdType;

import static com.google.common.base.Preconditions.checkArgument;

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
@AutoValue
public abstract class UIDMeta {
  public static UIDMeta create(final byte[] uid,
                               final UniqueIdType type,
                               final String name,
                               final String description,
                               final long created) {
    checkArgument(type.width == uid.length, "UID length must match the UID type width");
    checkArgument(!name.isEmpty(), "Name may not be empty");
    checkArgument(!description.isEmpty(), "Description may not be empty");
    return new AutoValue_UIDMeta(uid, type, name, description, created);
  }

  /** The id of this label */
  public abstract byte[] uid();

  /** What type of label this is */
  public abstract UniqueIdType type();

  /** The name of the label */
  public abstract String name();

  /** A free-form description of what this label represents */
  public abstract String description();

  /** The timestamp in milliseconds at which this label was created */
  public abstract long created();
}
