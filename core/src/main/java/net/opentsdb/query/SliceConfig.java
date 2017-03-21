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
package net.opentsdb.query;

import net.opentsdb.utils.DateTime;

/**
 * A small helper config that determines how queries are sliced at runtime.
 * A query can be sliced into smaller portions for caching chunks.
 * 
 * @since 3.0
 */
public class SliceConfig {

  /** Type of config, either a percent or duration */
  public enum SliceType {
    PERCENT,
    DURATION
  }
  
  /** The raw config from the user */
  protected final String config;
  
  /** The type of slicing to perform */
  protected final SliceType type;
  
  /** The slice quantity portion */
  protected final long quantity;
  
  /** The units if a duration */
  protected String units;
  
  /**
   * Default ctor
   * @param config A non-null and non-empty slice config, e.g. "100%" or "1h"
   * @throws IllegalArgumentException if the slice couldn't be parsed
   * @throws NumberFormatException if the slice couldn't be parsed
   */
  public SliceConfig(final String config) {
    if (config == null || config.isEmpty()) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    this.config = config;
    
    if (config.endsWith("%")) {
      type = SliceType.PERCENT;
      quantity = Integer.parseInt(config.substring(0, config.length() - 1));
      if (quantity > 100 || quantity < 1) {
        throw new IllegalArgumentException("Slice percentage must be between "
            + "1 and 100");
      }
    } else {
      type = SliceType.DURATION;
      // validation
      DateTime.parseDuration(config);
      quantity = DateTime.getDurationInterval(config);
      units = DateTime.getDurationUnits(config);
    }
  }
  
  /** @return The type of slice. */
  public SliceType getSliceType() {
    return type;
  }
  
  /** @return The width of each slice, depends on {@link #getSliceType()}. */
  public long getQuantity() {
    return quantity;
  }
  
  /** @return The raw config string. */
  public String getStringConfig() {
    return config;
  }
  
  /** @return The units of time if the quantity is not a {@link SliceType#PERCENT}. */
  public String getUnits() {
    return units;
  }
}
