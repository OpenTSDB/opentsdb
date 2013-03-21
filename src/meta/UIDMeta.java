// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

/**
 * UIDMeta objects are associated with the UniqueId of metrics, tag names
 * or tag values. When a new metric, tagk or tagv is generated, a UIDMeta object
 * will also be written to storage with only the uid, type and name filled out. 
 * Users can then modify mutable fields.
 * @since 2.0
 */
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class UIDMeta {

  /** A hexadecimal representation of the UID this metadata is associated with */
  private String uid = "";
  
  /** The type of UID this metadata represents */
  private int type = 0;
  
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
  
  /** @return the uid */
  public final String getUID() {
    return uid;
  }

  /** @return the type */
  public final int getType() {
    return type;
  }

  /** @return the name */
  public final String getName() {
    return name;
  }

  /** @return the display name */
  public final String getDisplayName() {
    return display_name;
  }

  /** @return the description */
  public final String getDescription() {
    return description;
  }

  /** @return the notes */
  public final String getNotes() {
    return notes;
  }

  /** @return the created timestamp */
  public final long getCreated() {
    return created;
  }

  /** @return the custom */
  public final HashMap<String, String> getCustom() {
    return custom;
  }

  /** @param uid the uid to set */
  public final void setUID(final String uid) {
    this.uid = uid;
  }

  /** @param type the type to set */
  public final void setType(final int type) {
    this.type = type;
  }

  /** @param name the name to set */
  public final void setName(final String name) {
    this.name = name;
  }

  /** @param display_name the display name to set */
  public final void setDisplayName(final String display_name) {
    this.display_name = display_name;
  }

  /** @param description the description to set */
  public final void setDescription(final String description) {
    this.description = description;
  }

  /** @param notes the notes to set */
  public final void setNotes(final String notes) {
    this.notes = notes;
  }

  /** @param created the created to set */
  public final void setCreated(final long created) {
    this.created = created;
  }

  /** @param custom the custom to set */
  public final void setCustom(final HashMap<String, String> custom) {
    this.custom = custom;
  }
}
