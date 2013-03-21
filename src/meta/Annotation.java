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
 * Annotations are used to record time-based notes about timeseries events.
 * Every note must have an associated start_time as that determines
 * where the note is stored.
 * <p>
 * Annotations may be associated with a specific timeseries, in which case
 * the tsuid must be configured with a valid TSUID. If no TSUID
 * is provided, the annotation is considered a "global" note that applies
 * to everything stored in OpenTSDB.
 * <p>
 * The description field should store a very brief line of information
 * about the event. GUIs can display the description in their "main" view
 * where multiple annotations may appear. Users of the GUI could then click
 * or hover over the description for more detail including the {@link notes}
 * field.
 * <p>
 * Custom data can be stored in the custom hash map for user
 * specific information. For example, you could add a "reporter" key
 * with the name of the person who recorded the note.
 * @since 2.0
 */
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Annotation {
  /** If the note is associated with a timeseries, represents the ID */
  private String tsuid = "";
  
  /** The start timestamp associated wit this note in seconds or ms */
  private long start_time = 0;
  
  /** Optional end time if the note represents an event that was resolved */
  private long end_time = 0;
  
  /** A short description of the event, displayed in GUIs */
  private String description = "";  
  
  /** A detailed accounting of the event or note */
  private String notes = "";
  
  /** Optional user supplied key/values */
  private HashMap<String, String> custom = null;
  
  /** @return the tsuid, may be empty if this is a global annotation */
  public final String getTSUID() {
    return tsuid;
  }

  /** @return the start_time */
  public final long getStartTime() {
    return start_time;
  }

  /**  @return the end_time, may be 0 */
  public final long getEndTime() {
    return end_time;
  }

  /** @return the description */
  public final String getDescription() {
    return description;
  }

  /** @return the notes, may be empty */
  public final String getNotes() {
    return notes;
  }

  /** @return the custom key/value map, may be null */
  public final HashMap<String, String> getCustom() {
    return custom;
  }

  /** @param tsuid the tsuid to store*/
  public void setTSUID(final String tsuid) {
    this.tsuid = tsuid;
  }

  /** @param start_time the start_time, required for every annotation */
  public void setStartTime(final long start_time) {
    this.start_time = start_time;
  }

  /** @param end_time the end_time, optional*/
  public void setEndTime(final long end_time) {
    this.end_time = end_time;
  }

  /** @param description the description, required for every annotation */
  public void setDescription(final String description) {
    this.description = description;
  }

  /** @param notes the notes to set */
  public void setNotes(final String notes) {
    this.notes = notes;
  }

  /** @param custom the custom key/value map */
  public void setCustom(final HashMap<String, String> custom) {
    this.custom = custom;
  }
}
