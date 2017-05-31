// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

/**
 * Responsible for encoding or decoding {@code HistogramDataPoint}s to and from
 * byte arrays.
 *
 * NOTE: Implementation of this plugin should be thread safe.
 * @see HistogramCodecManager
 * 
 * @since 2.4
 */
public abstract class HistogramDataPointCodec {

  /** The ID of this codec in the Histogram Manager. */
  protected int id;
  
  /**
   * Default empty ctor, required for plugin and class instantiation.
   * <b>WARNING</b> Any overrides with arguments will be ignored.
   */
  public HistogramDataPointCodec() {
    
  }
  
  public int getId() {
    return id;
  }
  
  public void setId(final int id) {
    this.id = id;
  }
  
  /**
   * Creates {@code HistogramDataPoint} from raw data and timestamp. Note that 
   * the data point identifier is separate.
   * @param raw_data The encoded byte array of the histogram data
   * @param timestamp The timestamp of this data point
   * @param includes_id Whether or not to include the id prefix.
   * @return The decoded histogram data point instance
   */
  public abstract Histogram decode(final byte[] raw_data, 
                                   final boolean includes_id);
  
  
  public abstract byte[] encode(final Histogram data_point,
                                final boolean include_id);
}
