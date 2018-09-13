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
import java.time.temporal.TemporalAmount;

import com.google.common.base.Strings;

import net.opentsdb.utils.DateTime;
import net.opentsdb.data.pbuf.TimeSpecificationPB;

/**
 * Handles a time specification tied to a downsampler.
 * 
 * @since 3.0
 */
public class PBufTimeSpecification implements TimeSpecification {

  private final TimeSpecificationPB.TimeSpecification spec;
  private TimeStamp start;
  private TimeStamp end;
  private TemporalAmount interval;
  private ChronoUnit units;
  private ZoneId zone;
  
  /**
   * Default ctor.
   * @param spec The non-null time spec protobuf.
   */
  public PBufTimeSpecification(final TimeSpecificationPB.TimeSpecification spec) {
    this.spec = spec;
  }
  
  @Override
  public TimeStamp start() {
    if (start == null) {
      start = new ZonedNanoTimeStamp(
          spec.getStart().getEpoch(),
          spec.getStart().getNanos(),
          Strings.isNullOrEmpty(spec.getTimeZone()) ? ZoneId.of("UTC") : 
            ZoneId.of(spec.getTimeZone()));
    }
    return start;
  }

  @Override
  public TimeStamp end() {
    if (end == null) {
      end = new ZonedNanoTimeStamp(
          spec.getEnd().getEpoch(),
          spec.getEnd().getNanos(),
          Strings.isNullOrEmpty(spec.getTimeZone()) ? ZoneId.of("UTC") : 
            ZoneId.of(spec.getTimeZone()));
    }
    return end;
  }

  @Override
  public TemporalAmount interval() {
    if (interval == null) {
      interval = DateTime.parseDuration2(spec.getInterval());
    }
    return interval;
  }

  @Override
  public String stringInterval() {
    return spec.getInterval();
  }

  @Override
  public ChronoUnit units() {
    if (units == null) {
      units = DateTime.unitsToChronoUnit(
          DateTime.getDurationUnits(spec.getInterval()));
    }
    return units;
  }

  @Override
  public ZoneId timezone() {
    if (zone == null) {
      zone = ZoneId.of(spec.getTimeZone());
    }
    return zone;
  }

  @Override
  public void updateTimestamp(final int offset, 
                              final TimeStamp timestamp) {
    TimeStamp clone = start().getCopy();
    for (int i = 0; i < offset; i++) {
      clone.add(interval());
    }
    timestamp.update(clone);
  }

  @Override
  public void nextTimestamp(final TimeStamp timestamp) {
    timestamp.add(interval());
  }

}
