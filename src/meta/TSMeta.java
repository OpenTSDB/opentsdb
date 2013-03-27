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

import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Timeseries Metadata is associated with a particular series of data points
 * and includes user configurable values and some stats calculated by OpenTSDB.
 * Whenever a new timeseries is recorded, an associated TSMeta object will
 * be recorded with only the tsuid field configured.
 * <p>
 * The metric and tag UIDMeta objects are loaded from their respective locations
 * in the data storage system.
 * @since 2.0
 */
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TSMeta {

  /** Hexadecimal representation of the TSUID this metadata is associated with */
  private String tsuid = "";
  
  /** The metric associated with this timeseries */
  private UIDMeta metric = null;
  
  /** A list of tagk/tagv pairs of UIDMetadata associated with this timeseries */
  private ArrayList<UIDMeta> tags = null;
  
  /** An optional, user supplied descriptive name */
  private String display_name = "";
  
  /** An optional short description of the timeseries */
  private String description = "";
  
  /** Optional detailed notes about the timeseries */
  private String notes = "";
  
  /** A timestamp of when this timeseries was first recorded in seconds */
  private long created = 0;
  
  /** Optional user supplied key/values */
  private HashMap<String, String> custom = null;
  
  /** An optional field recording the units of data in this timeseries */
  private String units = "";
  
  /** An optional field used to record the type of data, e.g. counter, gauge */
  private String data_type = "";
  
  /** How long to keep raw data in this timeseries */
  private int retention = 0;
  
  /** 
   * A user defined maximum value for this timeseries, can be used to 
   * calculate percentages
   */
  private double max = Double.NaN;
  
  /** 
   * A user defined minimum value for this timeseries, can be used to 
   * calculate percentages
   */
  private double min = Double.NaN; 
  
  /** The last time this data was recorded in seconds */
  private long last_received = 0;

  /** @return the tsuid */
  public final String getTSUID() {
    return tsuid;
  }

  /** @return the metric UID meta object */
  public final UIDMeta getMetric() {
    return metric;
  }

  /** @return the tag UID meta objects in an array, tagk first, then tagv, etc */
  public final ArrayList<UIDMeta> getTags() {
    return tags;
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

  /** @return the created */
  public final long getCreated() {
    return created;
  }

  /** @return the custom key/value map, may be null */
  public final HashMap<String, String> getCustom() {
    return custom;
  }

  /** @return the units */
  public final String getUnits() {
    return units;
  }

  /** @return the data type */
  public final String getDataType() {
    return data_type;
  }

  /** @return the retention */
  public final int getRetention() {
    return retention;
  }

  /** @return the max value */
  public final double getMax() {
    return max;
  }

  /** @return the min value*/
  public final double getMin() {
    return min;
  }

  /** @return the last received timestamp */
  public final long getLastReceived() {
    return last_received;
  }

  /** @param tsuid the tsuid to set */
  public final void setTSUID(final String tsuid) {
    this.tsuid = tsuid;
  }

  /** @param metric the metric UID meta object */
  public final void setMetric(final UIDMeta metric) {
    this.metric = metric;
  }

  /** @param tags the tag UID meta objects. Must be an array starting with a 
   * tagk object followed by the associataed tagv. */
  public final void setTags(final ArrayList<UIDMeta> tags) {
    this.tags = tags;
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

  /** @param units the units to set */
  public final void setUnits(final String units) {
    this.units = units;
  }

  /** @param data_type the data type to set */
  public final void setDataType(final String data_type) {
    this.data_type = data_type;
  }

  /** @param retention the retention to set */
  public final void setRetention(final int retention) {
    this.retention = retention;
  }

  /** @param max the max to set */
  public final void setMax(final double max) {
    this.max = max;
  }

  /** @param min the min to set */
  public final void setMin(final double min) {
    this.min = min;
  }

  /** @param last_received the last received timestamp */
  public final void setLastReceived(final long last_received) {
    this.last_received = last_received;
  }
}
