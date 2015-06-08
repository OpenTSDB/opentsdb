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

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Annotations are used to record time-based notes about timeseries events.
 * Every note must have an associated start_time as that determines
 * where the note is stored.
 * <p>
 * Annotations may be associated with a specific timeseries, in which case
 * the tsuid must be configured with a valid TSUID. If no TSUID
 * is provided, the annotation is considered a "global" note that applies
 * to everything stored in OpenTSDB. Global annotations are stored in the rows
 * [ 0, 0, 0, &lt;timestamp&gt;] in the same manner as local annotations and
 * timeseries data.
 * <p>
 * The description field should store a very brief line of information
 * about the event. GUIs can display the description in their "main" view
 * where multiple annotations may appear. Users of the GUI could then click
 * or hover over the description for more detail including the {@link #notes}
 * field.
 * <p>
 * Custom data can be stored in the custom hash map for user
 * specific information. For example, you could add a "reporter" key
 * with the name of the person who recorded the note.
 * @since 2.0
 */
public final class Annotation implements Comparable<Annotation> {
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
  private Map<String, String> custom = null;

  /** Tracks fields that have changed by the user to avoid overwrites */
  private final Set<String> changed = Sets.newHashSetWithExpectedSize(6);

  public Annotation() {
  }

  public Annotation(final String tsuid, final long start_time, final String description) {
    this(tsuid, start_time, 0, description, null, null);
  }

  public Annotation(final String tsuid,
                    final long start_time,
                    final long end_time,
                    final String description,
                    final String notes,
                    final Map<String, String> custom) {
    this.tsuid = tsuid;
    this.start_time = start_time;
    this.end_time = end_time;
    this.description = description;
    this.notes = notes;
    this.custom = custom;
  }

  /** @return A string with information about the annotation object */
  @Override
  public String toString() {
    return "TSUID: " + tsuid + " Start: " + start_time + "  Description: " + 
      description;
  }
  
  /**
   * Compares the {@code #start_time} of this annotation to the given note
   * @return 1 if the local start time is greater, -1 if it's less or 0 if
   * equal
   */
  @Override
  public int compareTo(Annotation note) {
    return start_time > note.start_time ? 1 : 
      start_time < note.start_time ? -1 : 0;
  }

  public boolean hasChanges() {
    return !changed.isEmpty();
  }

  /**
   * Syncs the local object with the stored object for atomic writes, 
   * overwriting the stored data if the user issued a PUT request
   * <b>Note:</b> This method also resets the {@code changed} set to false
   * for every field
   * @param note The stored object to sync from
   * @param overwrite Whether or not all user mutable data in storage should be
   * replaced by the local object
   */
  public void syncNote(final Annotation note, final boolean overwrite) {
    if (note.start_time > 0 && (note.start_time < start_time || start_time == 0)) {
      start_time = note.start_time;
    }
    
    // handle user-accessible stuff
    if (!overwrite && !changed.contains("end_time")) {
      end_time = note.end_time;
    }
    if (!overwrite && !changed.contains("description")) {
      description = note.description;
    }
    if (!overwrite && !changed.contains("notes")) {
      notes = note.notes;
    }
    if (!overwrite && !changed.contains("custom")) {
      custom = note.custom;
    }
    
    // reset changed flags
    resetChangedMap();
  }
  
  /**
   * Sets or resets the changed map flags
   */
  private void resetChangedMap() {
    changed.clear();
  }

  // Getters and Setters --------------
  
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
  public final Map<String, String> getCustom() {
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
    if (this.end_time != end_time) {
      this.end_time = end_time;
      changed.add("end_time");
    }
  }

  /** @param description the description, required for every annotation */
  public void setDescription(final String description) {
    if (!this.description.equals(description)) {
      this.description = description;
      changed.add("description");
    }
  }

  /** @param notes the notes to set */
  public void setNotes(final String notes) {
    if (!this.notes.equals(notes)) {
      this.notes = notes;
      changed.add("notes");
    }
  }

  /** @param custom the custom key/value map */
  public void setCustom(final Map<String, String> custom) {
    // equivalency of maps is a pain, users have to submit the whole map
    // anyway so we'll just mark it as changed every time we have a non-null
    // value
    if (this.custom != null || custom != null) {
      changed.add("custom");
      this.custom = new HashMap<>(custom);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final Annotation that = (Annotation) o;

    if (end_time != that.end_time) return false;
    if (start_time != that.start_time) return false;

    return Objects.equal(custom, that.custom) &&
           Objects.equal(description, that.description) &&
           Objects.equal(notes, that.notes) &&
           Objects.equal(tsuid, that.tsuid);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tsuid, start_time, end_time, description, notes, custom);
  }
}
