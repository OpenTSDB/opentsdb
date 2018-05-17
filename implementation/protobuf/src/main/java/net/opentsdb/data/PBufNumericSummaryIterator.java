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
package net.opentsdb.data;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

import net.opentsdb.data.pbuf.TimeSeriesDataPB;
import net.opentsdb.data.pbuf.NumericSummarySegmentPB.NumericSummarySegment;
import net.opentsdb.data.pbuf.NumericSummarySegmentPB.NumericSummarySegment.NumericSummary;
import net.opentsdb.data.pbuf.TimeSeriesDataPB.TimeSeriesData;
import net.opentsdb.data.pbuf.TimeSeriesDataSequencePB.TimeSeriesDataSegment;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.utils.Bytes;

/**
 * An implementation of the {@link NumericSummaryType} from OpenTSDB. The 
 * protobuf version may have multiple segments so this class will iterate
 * over all of them in order.
 * 
 * TODO - reverse
 * 
 * @since 3.0
 */
public class PBufNumericSummaryIterator implements 
  Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> {

  /** The encoded source. */
  private final TimeSeriesDataPB.TimeSeriesData source;
  
  /** The data point updated and returned during iteration. */
  private final MutableNumericSummaryValue dp;
  
  /** The current base timestamp of the current segment. */
  private TimeStamp base;
  
  /** The current timestamp, updated during iteration. */
  private TimeStamp current;
  
  /** Current index into the segment list. */
  private int segment_idx = 0;
  
  /** How many bytes the offset is encoded on for the current segment. */
  private byte encode_on;
  
  /** The current segment resolution. */
  private ChronoUnit resolution;
  
  /** A map of summery type IDs to summary iterators. */
  private Map<Integer, Summary> summaries;
  
  /** Whether or not the iterator has more data. */
  private boolean has_next;
  
  /** The next time offset to return. */
  private long next_offset;
  
  /**
   * Default ctor.
   * @param source A non-null source.
   * @throws IllegalArgumentException if the source was null.
   */
  public PBufNumericSummaryIterator(final TimeSeriesData source) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    this.source = source;
    if (source.getSegmentsCount() < 1) {
      base = null;
      current = null;
    } else {
      summaries = Maps.newHashMap();
      try {
        setSegment();
      } catch (InvalidProtocolBufferException e) {
        throw new SerdesException("Failed to load initial segment", e);
      }
    }
    dp = new MutableNumericSummaryValue();
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TimeSeriesValue<NumericSummaryType> next() {
    has_next = false;
    long next_next_offset = Long.MAX_VALUE;
    dp.clear();
    boolean wrote_data = false;
    for (final Entry<Integer, Summary> entry : summaries.entrySet()) {
      final Summary summary = entry.getValue();
      if (summary.hasNext() && summary.current_offset == next_offset) {
        if (!summary.is_null) {
          dp.resetValue(entry.getKey(), summary);
          wrote_data = true;
        }
        summary.advance();
      }
      
      if (summary.hasNext() && summary.current_offset < next_next_offset) {
        has_next = true;
        next_next_offset = summary.current_offset;
      }
    }
    
    switch (resolution) {
    case NANOS:
    case MICROS:
      long seconds = next_offset / (1000L * 1000L * 1000L);
      current.update(base.epoch() + seconds, 
          next_offset - (seconds * 1000L * 1000L * 1000L));
      break;
    case MILLIS:
      current.updateMsEpoch(base.msEpoch() + next_offset);
      break;
    default:
      current.updateEpoch(base.epoch() + next_offset);
    }
    if (wrote_data) {
      dp.resetTimestamp(current);
    } else {
      dp.resetNull(current);
    }
    next_offset = next_next_offset;
    
    if (next_offset == Long.MAX_VALUE && segment_idx < source.getSegmentsCount()) {
      // move to next segment
      try {
        setSegment();
      } catch (InvalidProtocolBufferException e) {
        throw new SerdesException("Failed to load initial segment", e);
      }
    }
    return dp;
  }
  
  /**
   * Advances to the next segment in the protobuf.
   * @throws InvalidProtocolBufferException If decoding failed.
   */
  void setSegment() throws InvalidProtocolBufferException {
    has_next = false;
    next_offset = Long.MAX_VALUE;
    while (segment_idx < source.getSegmentsCount()) {
      if (source.getSegments(segment_idx).getData().is(NumericSummarySegment.class)) {
        final TimeSeriesDataSegment segment = source.getSegments(segment_idx);
        if (base == null) {
          base = new ZonedNanoTimeStamp(segment.getStart().getEpoch(),
              segment.getStart().getNanos(),
              Strings.isNullOrEmpty(segment.getStart().getZoneId()) ? ZoneId.of("UTC") : 
                ZoneId.of(segment.getStart().getZoneId()));
        } else {
          base.update(segment.getStart().getEpoch(), 
              segment.getStart().getNanos());
          // TODO - validate same zone. Should be the same
        }
        
        if (segment.getData() == null) {
          segment_idx++;
          continue;
        }
        
        final NumericSummarySegment parsed = 
            segment.getData().unpack(NumericSummarySegment.class);
        if (parsed.getDataCount() < 1) {
          segment_idx++;
          continue;
        }
        
        // set this first as summary classes depend on it.
        encode_on = (byte) parsed.getEncodedOn();
        resolution = ChronoUnit.values()[parsed.getResolution()];
        for (final NumericSummary summary : parsed.getDataList()) {
          Summary iterator = summaries.get(summary.getSummaryId());
          if (iterator == null) {
            iterator = new Summary();
            summaries.put(summary.getSummaryId(), iterator);
          }
          // TODO - see if we can avoid the copy here.
          iterator.reset(summary.getData().toByteArray());
            if (iterator.hasNext() && 
                iterator.current_offset < next_offset) {
              next_offset = iterator.current_offset;
              has_next = true;
          }
        }
        
        if (current == null) {
          current = base.getCopy(); // just needs to be initialized
        }
        segment_idx++;
        break;
      } else {
        throw new SerdesException("Segment was not a NumericSummarySegment: " 
            + source.getSegments(segment_idx).getData().getTypeUrl());
      }
    }
  }
  
  /**
   * A single summary array within a sequence. This is used to iterate
   * over the individual values and make sure they're sync'd over time.
   */
  class Summary implements NumericType {
    /** Used to track the offset from the sequence base time. */
    byte[] offset = new byte[8];
    
    /** The encoded numeric data. */
    byte[] data;
    
    /** An index into the data array. */
    int idx = 0;
    
    /** The current offset from the base timestamp in the proper 
     * resolution or -1 if no more data is present. */
    long current_offset;
    
    /** The current value as either a long or a double. */
    long current_value;
    
    /** Whether or not the current value is a long. */
    boolean is_integer;
    
    /** Whether or not the current summary value is null. */
    boolean is_null;
    
    /**
     * Resets the offset and data, advancing to the first value if 
     * applicable.
     * @param data The encoded numeric data.
     */
    void reset(final byte[] data) {
      for (int i = 0; i < offset.length; i++) {
        offset[i] = 0;
      }
      this.data = data;
      idx = 0;
      
      // This is the same as next()'s as we need to set the initial
      // offset for comparison.
      if (data != null && data.length > 0) {
        advance();
      } else {
        current_offset = -1;
      }
    }

    /** @return True if there is more data. */
    boolean hasNext() {
      return current_offset >= 0;
    }
    
    /** Moves to the next value in the array or sets the current offset 
     * to -1 if there isn't any more data. */
    void advance() {
      if (idx >= data.length) {
        current_offset = -1;
        return;
      }
      System.arraycopy(data, idx, offset, 8 - encode_on, encode_on);
      long off = Bytes.getLong(offset);
      current_offset = off >> NumericCodec.FLAG_BITS;
      final byte flags = (byte) (off & NumericCodec.FLAGS_MASK);
      final byte vlen = (byte) ((flags & NumericCodec.LENGTH_MASK) + 1);
      
      idx += encode_on;
      if ((flags & NumericCodec.FLAG_FLOAT) == NumericCodec.FLAG_FLOAT) {
        if (vlen < 2) {
          is_null = true;
          is_integer = true;
        } else {
          is_null = false;
          is_integer = false;
          current_value = Double.doubleToLongBits(
              NumericCodec.extractFloatingPointValue(data, idx, flags));
          idx += vlen;
        }
      } else {
        is_null = false;
        is_integer = true;
        current_value = NumericCodec.extractIntegerValue(data, idx, flags);
        idx += vlen;
      }
    }
    
    @Override
    public boolean isInteger() {
      return is_integer;
    }

    @Override
    public long longValue() {
      return current_value;
    }

    @Override
    public double doubleValue() {
      return Double.longBitsToDouble(current_value);
    }

    @Override
    public double toDouble() {
      return is_integer ? (double) longValue() : doubleValue();
    }
    
  }
}
