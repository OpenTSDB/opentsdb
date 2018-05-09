// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data.types.numeric;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

/**
 * A mutable summary value with timestamp. It contains a map of summary
 * IDs to {@link MutableNumericType} values.
 * 
 * @since 3.0
 */
public class MutableNumericSummaryValue implements NumericSummaryType, 
                                          TimeSeriesValue<NumericSummaryType> {
  // NOTE: Fields are not final to make an instance available to store a new
  // pair of a timestamp and a value to reduce memory burden.
  
  /** The timestamp for this data point. */
  private TimeStamp timestamp;
  
  /** The map of summary IDs to values. */
  private Map<Integer, MutableNumericType> values;
  
  /** Whether or not the current value is null. */
  private boolean nulled; 
  
  /**
   * Default ctor. Sets the timestamp to 0 and initializes the map setting
   * the data point to "null".
   */
  public MutableNumericSummaryValue() {
    timestamp = new MillisecondTimeStamp(0L);
    values = Maps.newHashMapWithExpectedSize(1);
    nulled = true;
  }
  
  /**
   * Ctor that clones another summary.
   * @param value A non-null value to clone. The value of the value can
   * be null and we'll null out this dp.
   */
  public MutableNumericSummaryValue(
      final TimeSeriesValue<NumericSummaryType> value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    if (value.timestamp() == null) {
      throw new IllegalArgumentException("Value timestamp cannot be null.");
    }
    timestamp = value.timestamp().getCopy();
    values = Maps.newHashMapWithExpectedSize(value.value() != null ? 
        value.value().summariesAvailable().size() : 1);
    if (value.value() != null) {
      for (final int summary : value.value().summariesAvailable()) {
        final MutableNumericType clone = new MutableNumericType();
        clone.set(value.value().value(summary));
        values.put(summary, clone);
      }
      nulled = false;
    } else {
      nulled = true;
    }
  }
    
  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public Collection<Integer> summariesAvailable() {
    return values.keySet();
  }
  
  @Override
  public NumericType value(final int summary) {
    return values.get(summary);
  }
  
  /**
   * Empties the hash map of all values and sets the null flag to true.
   */
  public void clear() {
    values.clear();
    nulled = true;
  }
  
  /**
   * Resets the local value by copying the timestamp and value from the arguments.
   * @param A non-null timestamp.
   * @param value A numeric summary value.
   * @throws IllegalArgumentException if the timestamp or value was null.
   */
  public void reset(final TimeSeriesValue<NumericSummaryType> value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    if (value.value() == null) {
      resetNull(value.timestamp());
    } else {
      reset(value.timestamp(), value.value());
    }
  }
  
  /**
   * Resets with the non-null timestamp and optional value. If the value
   * is null, the local value is nulled. If not, then summaries are 
   * copied over and summaries that don't exist in the given value are
   * cleared.
   * @param timestamp A non-null timestamp.
   * @param value An optional value to clone from.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void reset(final TimeStamp timestamp, 
                    final NumericSummaryType value) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
    if (value == null) {
      clear();
      return;
    }
    
    final Collection<Integer> summaries = value.summariesAvailable();
    for (final int summary : summaries) {
      final NumericType dp = value.value(summary);
      if (dp == null) {
        values.remove(summary);
      } else {
        MutableNumericType extant = values.get(summary);
        if (extant == null) {
          extant = new MutableNumericType();
          values.put(summary, new MutableNumericType(dp));
        } else {
          extant.set(dp);
        }
      }
    }
    final Iterator<Entry<Integer, MutableNumericType>> iterator = 
        values.entrySet().iterator();
    while (iterator.hasNext()) {
      final Entry<Integer, MutableNumericType> entry = iterator.next();
      if (!summaries.contains(entry.getKey())) {
        iterator.remove();
      }
    }
    nulled = false;
  }
  
  /**
   * Resets the value to null with the given timestamp.
   * @param timestamp A non-null timestamp to update from.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void resetNull(final TimeStamp timestamp) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
    clear();
    nulled = true;
  }
  
  /**
   * Resets just the value for this summary.
   * @param summary A summary ID.
   * @param value A value to set.
   */
  public void resetValue(final int summary, final long value) {
    final MutableNumericType v = values.get(summary);
    if (v == null) {
      values.put(summary, new MutableNumericType(value));
    } else {
      v.set(value);
    }
    nulled = false;
  }
  
  /**
   * Resets just the value for this summary.
   * @param summary A summary ID.
   * @param value A value to set.
   */
  public void resetValue(final int summary, final double value) {
    final MutableNumericType v = values.get(summary);
    if (v == null) {
      values.put(summary, new MutableNumericType(value));
    } else {
      v.set(value);
    }
    nulled = false;
  }
  
  /**
   * Resets the value for this summary. If the value was null and the
   * map has been cleared, we set the whole data point to null.
   * @param summary A summary ID.
   * @param value An optional value to reset.
   */
  public void resetValue(final int summary, final NumericType value) {
    if (value == null) {
      values.remove(summary);
      if (values.isEmpty()) {
        nulled = true;
      }
      return;
    }
    
    MutableNumericType v = values.get(summary);
    if (v == null) {
      values.put(summary, new MutableNumericType(value));
    } else {
      v.set(value);
    }
    nulled = false;
  }
  
  /**
   * Resets the value for this summary. If the value was null and the
   * map has been cleared, we set the whole data point to null.
   * @param summary A summary ID.
   * @param value An optional value to reset.
   */
  public void resetValue(final int summary, 
                         final TimeSeriesValue<NumericType> value) {
    if (value == null) {
      resetValue(summary, (NumericType) null);  
    } else {
      resetValue(summary, value.value());
    }
  }
  
  /**
   * Removes the value and nulls the whole data point if the map is empty.
   * @param summary The summary to remove.
   */
  public void nullSummary(final int summary) {
    values.remove(summary);
    if (values.isEmpty()) {
      nulled = true;
    }
  }
  
  /**
   * Resets just the timestamp of the data point. Good for use with 
   * aggregators but be CAREFUL not to return without updating the value.
   * @param timestamp The timestamp to set.
   */
  public void resetTimestamp(final TimeStamp timestamp) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
  }
  
  @Override
  public TypeToken<NumericSummaryType> type() {
    return NumericSummaryType.TYPE;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("timestamp=")
        .append(timestamp)
        .append(", nulled=")
        .append(nulled)
        .append(", values=")
        .append(values)
        .toString();
  }

  @Override
  public NumericSummaryType value() {
    return nulled ? null : this;
  }
}