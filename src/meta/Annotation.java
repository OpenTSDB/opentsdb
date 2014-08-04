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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Const;
import net.opentsdb.core.Internal;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

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
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Annotation implements Comparable<Annotation> {
  private static final Logger LOG = LoggerFactory.getLogger(Annotation.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  
  /** Byte used for the qualifier prefix to indicate this is an annotation */
  public static final byte ANNOTATION_QUAL_PREFIX = 0x01;
    
  /** The single column family used by this class. */
  private static final byte[] FAMILY = "t".getBytes(CHARSET);
  
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

  /** Tracks fields that have changed by the user to avoid overwrites */
  private final Set<String> changed = Sets.newHashSetWithExpectedSize(6);

  /**
   * Default constructor, initializes the change map
   */
  public Annotation() {
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
   * Scans through the global annotation storage rows and returns a list of 
   * parsed annotation objects. If no annotations were found for the given
   * timespan, the resulting list will be empty.
   * @param tsdb The TSDB to use for storage access
   * @param start_time Start time to scan from. May be 0
   * @param end_time End time to scan to. Must be greater than 0
   * @return A list with detected annotations. May be empty.
   * @throws IllegalArgumentException if the end timestamp has not been set or 
   * the end time is less than the start time
   */
  public static Deferred<List<Annotation>> getGlobalAnnotations(final TSDB tsdb, 
      final long start_time, final long end_time) {
    if (end_time < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (end_time < start_time) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }
    
    /**
     * Scanner that loops through the [0, 0, 0, timestamp] rows looking for
     * global annotations. Returns a list of parsed annotation objects.
     * The list may be empty.
     */
    final class ScannerCB implements Callback<Deferred<List<Annotation>>, 
      ArrayList<ArrayList<KeyValue>>> {
      final Scanner scanner;
      final List<Annotation> annotations = new ArrayList<Annotation>();
      
      /**
       * Initializes the scanner
       */
      public ScannerCB() {
        final byte[] start = new byte[Const.METRICS_WIDTH +
                                      Const.TIMESTAMP_BYTES];
        final byte[] end = new byte[Const.METRICS_WIDTH +
                                    Const.TIMESTAMP_BYTES];
        
        final long normalized_start = (start_time - 
            (start_time % Const.MAX_TIMESPAN));
        final long normalized_end = (end_time - 
            (end_time % Const.MAX_TIMESPAN));

        Bytes.setInt(start, (int) normalized_start, Const.METRICS_WIDTH);
        Bytes.setInt(end, (int) normalized_end, Const.METRICS_WIDTH);

        scanner = tsdb.getTsdbStore().newScanner(tsdb.dataTable());
        scanner.setStartKey(start);
        scanner.setStopKey(end);
        scanner.setFamily(FAMILY);
      }
      
      public Deferred<List<Annotation>> scan() {
        return scanner.nextRows().addCallbackDeferring(this);
      }
      
      @Override
      public Deferred<List<Annotation>> call (
          final ArrayList<ArrayList<KeyValue>> rows) throws Exception {
        if (rows == null || rows.isEmpty()) {
          return Deferred.fromResult(annotations);
        }
        
        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue column : row) {
            if ((column.qualifier().length == 3 || column.qualifier().length == 5)
                && column.qualifier()[0] == ANNOTATION_QUAL_PREFIX) {
              Annotation note = JSON.parseToObject(row.get(0).value(), 
                  Annotation.class);
              if (note.start_time < start_time || note.end_time > end_time) {
                continue;
              }
              annotations.add(note);
            }
          }
        }
        
        return scan();
      }
      
    }

    return new ScannerCB().scan();
  }
  
  /**
   * Deletes global or TSUID associated annotiations for the given time range.
   * @param tsdb The TSDB object to use for storage access
   * @param tsuid An optional TSUID. If set to null, then global annotations for
   * the given range will be deleted
   * @param start_time A start timestamp in milliseconds
   * @param end_time An end timestamp in millseconds
   * @return The number of annotations deleted
   * @throws IllegalArgumentException if the timestamps are invalid
   * @since 2.1
   */
  public static Deferred<Integer> deleteRange(final TSDB tsdb, 
      final byte[] tsuid, final long start_time, final long end_time) {
    if (end_time < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (end_time < start_time) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }
    
    final List<Deferred<Object>> delete_requests = new ArrayList<Deferred<Object>>();
    int width = tsuid != null ? tsuid.length + Const.TIMESTAMP_BYTES :
      Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES;
    final byte[] start_row = new byte[width];
    final byte[] end_row = new byte[width];
    
    // downsample to seconds for the row keys
    final long start = start_time / 1000;
    final long end = end_time / 1000;
    final long normalized_start = (start - (start % Const.MAX_TIMESPAN));
    final long normalized_end = (end - (end % Const.MAX_TIMESPAN));
    Bytes.setInt(start_row, (int) normalized_start, Const.METRICS_WIDTH);
    Bytes.setInt(end_row, (int) normalized_end, Const.METRICS_WIDTH);
    
    if (tsuid != null) {
      // first copy the metric UID then the tags
      System.arraycopy(tsuid, 0, start_row, 0, Const.METRICS_WIDTH);
      System.arraycopy(tsuid, 0, end_row, 0, Const.METRICS_WIDTH);
      width = Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES;
      final int remainder = tsuid.length - Const.METRICS_WIDTH;
      System.arraycopy(tsuid, Const.METRICS_WIDTH, start_row, width, remainder);
      System.arraycopy(tsuid, Const.METRICS_WIDTH, end_row, width, remainder);
    }

    /**
     * Iterates through the scanner results in an asynchronous manner, returning
     * once the scanner returns a null result set.
     */
    final class ScannerCB implements Callback<Deferred<List<Deferred<Object>>>, 
        ArrayList<ArrayList<KeyValue>>> {
      final Scanner scanner;

      public ScannerCB() {
        scanner = tsdb.getTsdbStore().newScanner(tsdb.dataTable());
        scanner.setStartKey(start_row);
        scanner.setStopKey(end_row);
        scanner.setFamily(FAMILY);
        if (tsuid != null) {
          final List<String> tsuids = new ArrayList<String>(1);
          tsuids.add(UniqueId.uidToString(tsuid));
          Internal.createAndSetTSUIDFilter(scanner, tsuids);
        }
      }
      
      public Deferred<List<Deferred<Object>>> scan() {
        return scanner.nextRows().addCallbackDeferring(this);
      }
      
      @Override
      public Deferred<List<Deferred<Object>>> call (
          final ArrayList<ArrayList<KeyValue>> rows) throws Exception {
        if (rows == null || rows.isEmpty()) {
          return Deferred.fromResult(delete_requests);
        }
        
        for (final ArrayList<KeyValue> row : rows) {
          final long base_time = Internal.baseTime(tsdb, row.get(0).key());
          for (KeyValue column : row) {
            if ((column.qualifier().length == 3 || column.qualifier().length == 5)
                && column.qualifier()[0] == ANNOTATION_QUAL_PREFIX) {
              final long timestamp = timeFromQualifier(column.qualifier(), 
                  base_time);
              if (timestamp < start_time || timestamp > end_time) {
                continue;
              }
              final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), 
                  column.key(), FAMILY, column.qualifier());
              delete_requests.add(tsdb.getTsdbStore().delete(delete));
            }
          }
        }
        return scan();
      }
    }

    /** Called when the scanner is done. Delete requests may still be pending */
    final class ScannerDoneCB implements Callback<Deferred<ArrayList<Object>>, 
      List<Deferred<Object>>> {
      @Override
      public Deferred<ArrayList<Object>> call(final List<Deferred<Object>> deletes)
          throws Exception {
        return Deferred.group(delete_requests);
      }
    }
    
    /** Waits on the group of deferreds to complete before returning the count */
    final class GroupCB implements Callback<Deferred<Integer>, ArrayList<Object>> {
      @Override
      public Deferred<Integer> call(final ArrayList<Object> deletes)
          throws Exception {
        return Deferred.fromResult(deletes.size());
      }
    }
    
    Deferred<ArrayList<Object>> scanner_done = new ScannerCB().scan()
        .addCallbackDeferring(new ScannerDoneCB());
    return scanner_done.addCallbackDeferring(new GroupCB());
  }

  /**
   * Serializes the object in a uniform matter for storage. Needed for 
   * successful CAS calls
   * @return The serialized object as a byte array
   */
  public byte[] getStorageJSON() {
    // TODO - precalculate size
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output); 
      json.writeStartObject();
      if (tsuid != null && !tsuid.isEmpty()) {
        json.writeStringField("tsuid", tsuid);  
      }
      json.writeNumberField("startTime", start_time);
      json.writeNumberField("endTime", end_time);
      json.writeStringField("description", description);
      json.writeStringField("notes", notes);
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
      throw new RuntimeException("Unable to serialize Annotation", e);
    }
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

  /**
   * Returns a timestamp after parsing an annotation qualifier.
   * @param qualifier The full qualifier (including prefix) on either 3 or 5 bytes
   * @param base_time The base time from the row in seconds
   * @return A timestamp in milliseconds
   * @since 2.1
   */
  private static long timeFromQualifier(final byte[] qualifier, final long base_time) {
    final long offset;
    if (qualifier.length == 3) {
      offset = Bytes.getUnsignedShort(qualifier, 1);
      return (base_time + offset) * 1000;
    } else {
      offset = Bytes.getUnsignedInt(qualifier, 1);
      return (base_time * 1000) + offset;
    }
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
      this.custom = new HashMap<String, String>(custom);
    }
  }
}
