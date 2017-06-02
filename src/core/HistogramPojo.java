// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;

public class HistogramPojo extends IncomingDataPoint {
  private static final Logger LOG = LoggerFactory.getLogger(HistogramPojo.class);
  
  private int id;
  
  private Map<String, Long> buckets;
  
  private long underflow;
  
  private long overflow;
  
  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }
  
  @Override
  public boolean validate(final List<Map<String, Object>> details) {
    if (this.getMetric() == null || this.getMetric().isEmpty()) {
      if (details != null) {
        details.add(getHttpDetails("Metric name was empty"));
      }
      LOG.warn("Metric name was empty: " + this);
      return false;
    }
    
    if (this.getTimestamp() <= 0) {
      if (details != null) {
        details.add(getHttpDetails("Invalid timestamp"));
      }
      LOG.warn("Invalid timestamp: " + this);
      return false;
    }
    
    if (this.getTags() == null || this.getTags().size() < 1) {
      if (details != null) {
        details.add(getHttpDetails("Missing tags"));
      }
      LOG.warn("Missing tags: " + this);
      return false;
    }
    
    if (Strings.isNullOrEmpty(value)) {
      if (buckets == null || buckets.isEmpty()) {
        if (details != null) {
          details.add(getHttpDetails("Histogram buckets cannot be null or empty "
              + "if 'value' is empty."));
        }
        LOG.warn("Histogram buckets cannot be null or empty if 'value' is empty.");
        return false;
      }
    } else {
      // ID is required for binary histos.
      if (id < 0 || id > 255) {
        if (details != null) {
          details.add(getHttpDetails("Invalid type. Must be from 0 to 255."));
        }
        LOG.warn("Invalid type. Must be from 0 to 255.");
        return false;
      }
    }
    return true;
  }
  
  public SimpleHistogram toSimpleHistogram(final TSDB tsdb) {
    if (buckets == null || buckets.isEmpty()) {
      throw new IllegalArgumentException("Buckets cannot be empty when "
          + "creating a simple histogram.");
    }
    
    final SimpleHistogram shdp = new SimpleHistogram(
        tsdb.histogramManager().getCodec(SimpleHistogramDecoder.class));
    shdp.setOverflow(overflow);
    shdp.setUnderflow(underflow);
    
    for (final Entry<String, Long> bucket : buckets.entrySet()) {
      final String[] bounds = Tags.splitString(bucket.getKey(), ',');
      if (bounds.length != 2) {
        throw new IllegalArgumentException("Unable to parse bucket bounds: " 
            + bucket.getKey());
      }
      shdp.addBucket(Float.parseFloat(bounds[0]), Float.parseFloat(bounds[1]), 
          bucket.getValue());
    }
    return shdp;
  }
  
  public Map<String, Long> getBuckets() {
    return buckets;
  }
  
  public long getUnderflow() {
    return underflow;
  }
  
  public long getOverflow() {
    return overflow;
  }
  
  @JsonIgnore
  public byte[] getBytes() {
    return base64StringToBytes(value);
  }
  
  public void setBuckets(final Map<String, Long> buckets) {
    this.buckets = buckets;
  }
  
  public void setUnderflow(final long underflow) {
    this.underflow = underflow;
  }
  
  public void setOverflow(final long overflow) {
    this.overflow = overflow;
  }
  
  public static String bytesToBase64String(final byte[] raw) {
    return DatatypeConverter.printBase64Binary(raw);
  }
  
  public static byte[] base64StringToBytes(final String encoded) {
    return DatatypeConverter.parseBase64Binary(encoded);
  }
}
