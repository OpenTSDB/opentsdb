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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import net.opentsdb.uid.IdUtils;

import com.google.common.collect.Sets;
import net.opentsdb.uid.LabelId;

/**
 * Timeseries Metadata is associated with a particular series of data points
 * and includes user configurable values and some stats calculated by OpenTSDB.
 * Whenever a new timeseries is recorded, an associated TSMeta object will
 * be stored with only the tsuid field configured. These meta objects may then
 * be used to determine what combinations of metrics and tags exist in the
 * system.
 * <p>
 * When you call {@link #syncToStorage} on this object, it will verify that the
 * associated UID objects this meta data is linked with still exist. Then it 
 * will fetch the existing data and copy changes, overwriting the user fields if
 * specific (e.g. via a PUT command). If overwriting is not called for (e.g. a 
 * POST was issued), then only the fields provided by the user will be saved, 
 * preserving all of the other fields in storage. Hence the need for the 
 * {@code changed} hash map and the {@link #syncMeta} method. 
 * <p>
 * The metric and tag UIDMeta objects may be loaded from their respective 
 * locations in the data storage system if requested. Note that this will cause
 * at least 3 extra storage calls when loading.
 * @since 2.0
 */
public final class TSMeta {
  /** Hexadecimal representation of the TSUID this metadata is associated with */
  private String tsuid = "";

  /** The metric associated with this timeseries */
  private LabelMeta metric = null;
  
  /** A list of tagk/tagv pairs of UIDMetadata associated with this timeseries */
  private ArrayList<LabelMeta> tags = null;
  
  /** An optional, user supplied descriptive name */
  private String display_name = "";
  
  /** An optional short description of the timeseries */
  private String description = "";
  
  /** Optional detailed notes about the timeseries */
  private String notes = "";
  
  /** A timestamp of when this timeseries was first recorded in seconds */
  private long created = 0;
  
  /** Optional user supplied key/values */
  private Map<String, String> custom = null;
  
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
  
  /** The total number of data points recorded since meta has been enabled */
  private long total_dps;

  /** Tracks fields that have changed by the user to avoid overwrites */
  private final Set<String> changed = Sets.newHashSetWithExpectedSize(12);

  /**
   * Default constructor necessary for POJO de/serialization
   */
  public TSMeta() {
  }
  
  /**
   * Constructor for RPC timeseries parsing that will not set the timestamps
   * @param tsuid The UID of the timeseries
   */
  public TSMeta(final String tsuid) {
    this.tsuid = tsuid;
  }

  public TSMeta(final byte[] tsuid) {
    this.tsuid = IdUtils.uidToString(tsuid);
  }
  
  /**
   * Constructor for new timeseries that initializes the created and 
   * last_received times to the current system time
   * @param tsuid The UID of the timeseries
   */
  public TSMeta(final byte[] tsuid, final long created) {
    this.tsuid = IdUtils.uidToString(tsuid);
    // downgrade to seconds
    this.created = created > 9999999999L ? created / 1000 : created;
    changed.add("created");
  }

  TSMeta(final String tsuid,
         final String displayName,
         final String description,
         final String notes,
         final long created,
         final Map<String, String> custom,
         final String units,
         final String dataType,
         final int retention,
         final double max,
         final double min) {
    this.tsuid = tsuid;
    this.display_name = displayName;
    this.description = description;
    this.notes = notes;
    this.created = created;
    this.custom = custom;
    this.units = units;
    this.data_type = dataType;
    this.retention = retention;
    this.max = max;
    this.min = min;
  }
  
  /** @return a string with details about this object */
  @Override
  public String toString() {
    return tsuid;
  }

  public boolean hasChanges() {
    return !changed.isEmpty();
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
  public void syncMeta(final TSMeta meta, final boolean overwrite) {
    // storage *could* have a missing TSUID if something went pear shaped so
    // only use the one that's configured. If the local is missing, we're foobar
    if (meta.tsuid != null && !meta.tsuid.isEmpty()) {
      tsuid = meta.tsuid;
    }
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("TSUID is empty");
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
    if (!overwrite && !changed.contains("units")) {
      units = meta.units;
    }
    if (!overwrite && !changed.contains("data_type")) {
      data_type = meta.data_type;
    }
    if (!overwrite && !changed.contains("retention")) {
      retention = meta.retention;
    }
    if (!overwrite && !changed.contains("max")) {
      max = meta.max;
    }
    if (!overwrite && !changed.contains("min")) {
      min = meta.min;
    }
    
    last_received = meta.last_received;
    total_dps = meta.total_dps;
    
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
  
  /** @return the TSUID as a hex encoded string */
  public final String getTSUID() {
    return tsuid;
  }

  // TODO
  public LabelId metric() {
    return null;
  }

  /** @return the metric UID meta object */
  public final LabelMeta getMetric() {
    return metric;
  }

  // TODO
  public Map<LabelId,LabelId> tags() {
    return null;
  }

  /** @return the tag UID meta objects in an array, tagk first, then tagv, etc */
  public final List<LabelMeta> getTags() {
    return tags;
  }

  /** @return optional display name */
  public final String getDisplayName() {
    return display_name;
  }

  /** @return optional description */
  public final String getDescription() {
    return description;
  }

  /** @return optional notes */
  public final String getNotes() {
    return notes;
  }

  /** @return when the TSUID was first recorded, Unix epoch */
  public final long getCreated() {
    return created;
  }

  /** @return optional custom key/value map, may be null */
  public final Map<String, String> getCustom() {
    return custom;
  }

  public String getUnits() {
    return units;
  }

  public String getDataType() {
    return data_type;
  }

  /** @return optional retention, default of 0 means retain indefinitely */
  public final int getRetention() {
    return retention;
  }

  /** @return optional max value, set by the user */
  public final double getMax() {
    return max;
  }

  /** @return optional min value, set by the user */
  public final double getMin() {
    return min;
  }

  /** @return the total number of data points as tracked by the meta data */
  public final long getTotalDatapoints() {
    return this.total_dps;
  }
  
  /** @param display_name an optional name for the timeseries */
  public final void setDisplayName(final String display_name) {
    if (!this.display_name.equals(display_name)) {
      changed.add("display_name");
      this.display_name = display_name;
    }
  }

  /** @param description an optional description */
  public final void setDescription(final String description) {
    if (!this.description.equals(description)) {
      changed.add("description");
      this.description = description;
    }
  }

  /** @param notes optional notes */
  public final void setNotes(final String notes) {
    if (!this.notes.equals(notes)) {
      changed.add("notes");
      this.notes = notes;
    }
  }

  /** @param created the created timestamp Unix epoch in seconds */
  public final void setCreated(final long created) {
    if (this.created != created) {
      changed.add("created");
      this.created = created;
    }
  }
  
  /** @param tsuid The TSUID of the timeseries. */
  public final void setTSUID(final String tsuid) {
    this.tsuid = tsuid;
  }
  
  /** @param custom optional key/value map */
  public final void setCustom(final Map<String, String> custom) {
    // equivalency of maps is a pain, users have to submit the whole map
    // anyway so we'll just mark it as changed every time we have a non-null
    // value
    if (this.custom != null || custom != null) {
      changed.add("custom");
      this.custom = new HashMap<>(custom);
    }
  }

  /** @param units optional units designation */
  public final void setUnits(final String units) {
    if (!this.units.equals(units)) {
      changed.add("units");
      this.units = units;
    }
  }

  /** @param data_type optional type of data, e.g. "counter", "gauge" */
  public final void setDataType(final String data_type) {
    if (!this.data_type.equals(data_type)) {
      changed.add("data_type");
      this.data_type = data_type;
    }
  }

  /** @param retention optional rentention in days, 0 = indefinite */
  public final void setRetention(final int retention) {
    if (this.retention != retention) {
      changed.add("retention");
      this.retention = retention;
    }
  }

  /** @param max optional max value for the timeseries, NaN is the default */
  public final void setMax(final double max) {
    if (this.max != max) {
      changed.add("max");
      this.max = max;
    }
  }

  /** @param min optional min value for the timeseries, NaN is the default */
  public final void setMin(final double min) {
    if (this.min != min) {
      changed.add("min");
      this.min = min;
    }
  }

  /**
   * Used when parsing this object from storage.
   *
   * @param last_received
   */
  public void setLastReceived(final long last_received) {
    if (this.last_received != last_received) {
      changed.add("last_received");
      this.last_received = last_received;
    }
  }

  /**
   * Used when parsing this object from storage.
   *
   * @param total_dps set the total_dps value of this TSMeta.
   */
  public void setTotalDatapoints(final long total_dps) {
    if (this.total_dps != total_dps) {
      changed.add("total_dps");
      this.total_dps = total_dps;
    }
  }

  /**
   * Used by the {@link net.opentsdb.core.TSDB.LoadUIDs} class in TSDB store.
   *
   * @param tags Sets the tags of this object to parameter.
   */
  public void setTags(ArrayList<LabelMeta> tags) {
    this.tags = tags;
  }

  /**
   * Used by the {@link net.opentsdb.core.TSDB.LoadUIDs} class in TSDB store.
   *
   * @param metric Sets the metircs of this object to parameter.
   */
  public void setMetric(LabelMeta metric) {
    this.metric = metric;
  }

  /**
   * Often we check if this TSMeta is ready for storage by checking if the
   * tsuid was not null or empty. This function does just that.
   * @throws IllegalArgumentException
   */
  public void checkTSUI() throws IllegalArgumentException{
    if (Strings.isNullOrEmpty(tsuid)) {
      throw new IllegalArgumentException("Missing TSUID");
    }
  }
}
