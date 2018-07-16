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

import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;

import net.opentsdb.data.pbuf.NumericSegmentPB.NumericSegment;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.data.pbuf.TimeSeriesDataPB.TimeSeriesData;
import net.opentsdb.data.pbuf.TimeSeriesDataSequencePB.TimeSeriesDataSegment;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.utils.Bytes;

/**
 * An implementation of the {@link NumericType} from OpenTSDB. The 
 * protobuf version may have multiple segments so this class will iterate
 * over all of them in order.
 * 
 * TODO - reverse
 * 
 * @since 3.0
 */
public class PBufNumericIterator implements 
  Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> {

  /** The encoded source. */
  private final TimeSeriesData source;
  
  /** The data point updated and returned during iteration. */
  private final MutableNumericValue dp;
  
  /** The current base timestamp of the current segment. */
  private TimeStamp base;
  
  /** The current timestamp, updated during iteration. */
  private TimeStamp current;
  
  /** Current index into the segment list. */
  private int segment_idx = 0;
  
  /** Current index into the data array from a segment. */
  private int idx = 0;
  
  /** Pointer to the current data byte array. */
  private byte[] data;
  
  /** How many bytes the offset is encoded on for the current segment. */
  private byte encode_on;
  
  /** The current segment resolution. */
  private ChronoUnit resolution;
  
  /** The current offset read from the data array. */
  private byte[] offset = new byte[8];
  
  /**
   * Default ctor.
   * @param source A non-null source.
   * @throws IllegalArgumentException if the source was null.
   */
  public PBufNumericIterator(final TimeSeriesData source) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    this.source = source;
    if (source.getSegmentsCount() < 1) {
      base = null;
      current = null;
      data = new byte[0];
    } else {
      try {
        setSegment();
      } catch (InvalidProtocolBufferException e) {
        throw new SerdesException("Failed to load initial segment", e);
      }
    }
    dp = new MutableNumericValue();
  }
  
  @Override
  public boolean hasNext() {
    return idx < data.length && segment_idx <= source.getSegmentsCount();
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    for (int i = 0; i < offset.length; i++) {
      offset[i] = 0;
    }
    System.arraycopy(data, idx, offset, 8 - encode_on, encode_on);
    long off = Bytes.getLong(offset);
    final byte flags = (byte) (off & NumericCodec.FLAGS_MASK);
    off = off >> NumericCodec.FLAG_BITS;
    final byte vlen = (byte) ((flags & NumericCodec.LENGTH_MASK) + 1);
    switch (resolution) {
    case NANOS:
    case MICROS:
      long seconds = off / (1000L * 1000L * 1000L);
      current.update(base.epoch() + seconds, 
          off - (seconds * 1000L * 1000L * 1000L));
      break;
    case MILLIS:
      current.updateMsEpoch(base.msEpoch() + off);
      break;
    default:
      current.updateEpoch(base.epoch() + off);
    }
    
    idx += encode_on;
    if ((flags & NumericCodec.FLAG_FLOAT) == NumericCodec.FLAG_FLOAT) {
      if (vlen < 2) {
        dp.resetNull(current);
      } else {
        dp.reset(current, NumericCodec.extractFloatingPointValue(data, idx, flags));
        idx += vlen;
      }
    } else {
      dp.reset(current, NumericCodec.extractIntegerValue(data, idx, flags));
      idx += vlen;
    }
    
    if (idx >= data.length && segment_idx < source.getSegmentsCount()) {
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
    while (segment_idx < source.getSegmentsCount()) {
      if (source.getSegments(segment_idx).getData().is(NumericSegment.class)) {
        final TimeSeriesDataSegment segment = source.getSegments(segment_idx);
        final NumericSegment parsed = source.getSegments(segment_idx)
            .getData().unpack(NumericSegment.class);
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
        
        // TODO - see if we can avoid the copy here.
        data = parsed.getData().toByteArray();
        if (data == null || data.length < 1) {
          segment_idx++;
          continue;
        }
        encode_on = (byte) parsed.getEncodedOn();
        resolution = ChronoUnit.values()[parsed.getResolution()];
        if (current == null) {
          current = base.getCopy(); // just needs to be initialized
        }
        segment_idx++;
        idx = 0;
        break;
      } else {
        throw new SerdesException("Segment was not a NumericSegment: " 
            + source.getSegments(segment_idx).getData().getTypeUrl());
      }
    }
  }
}
