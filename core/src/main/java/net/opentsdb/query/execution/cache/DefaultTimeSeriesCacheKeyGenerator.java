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
package net.opentsdb.query.execution.cache;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.DateTime;

/**
 * Simple implementation of the key generator that prepends keys with
 * "TSDBQ".
 * 
 * TODO - a stepdown function.
 * 
 * @since 3.0
 */
public class DefaultTimeSeriesCacheKeyGenerator 
  extends TimeSeriesCacheKeyGenerator {
  
  /** The prefix to prepend */
  public static final byte[] CACHE_PREFIX = 
      new byte[] { 'T', 'S', 'D', 'B', 'Q' };

  /** The default expiration in milliseconds if no expiration was given. */
  private final long default_expiration;
  
  /** The default max expiration for old data. */
  private final long default_max_expiration;
  
  /**
   * Default ctor.
   * @param default_expiration The default expiration in milliseconds.
   * @param default_max_expiration The default max expiration for old data.
   */
  public DefaultTimeSeriesCacheKeyGenerator(final long default_expiration,
                                            final long default_max_expiration) {
    this.default_expiration = default_expiration;
    this.default_max_expiration = default_max_expiration;
  }
  
  @Override
  public byte[] generate(final TimeSeriesQuery query, 
                         final boolean with_timestamps) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    final byte[] hash = with_timestamps ? 
        query.buildHashCode().asBytes() : 
        query.buildTimelessHashCode().asBytes();
    final byte[] key = new byte[hash.length + CACHE_PREFIX.length];
    System.arraycopy(CACHE_PREFIX, 0, key, 0, CACHE_PREFIX.length);
    System.arraycopy(hash, 0, key, CACHE_PREFIX.length, hash.length);
    return key;
  }

  @Override
  public long expiration(final TimeSeriesQuery query, final long expiration) {
    if (expiration == 0) {
      return 0;
    }
    if (expiration > 0) {
      return expiration;
    }
    
    if (query == null) {
      return default_expiration;
    }
    
    // calculate it
    final TimeStamp end = query.getTime().endTime();
    final long timestamp = DateTime.currentTimeMillis();
    
    // TODO - proper step-down function.
    if (timestamp - end.msEpoch() > (3600000 * 2)) {
      return default_max_expiration;
    }
    
    if (query.getTime().getDownsampler() == null) {
      return default_expiration;
    }
    
    final long interval = DateTime.parseDuration(
        query.getTime().getDownsampler().getInterval());
    final long result = (timestamp - (timestamp - (timestamp % interval)));
    return result > default_max_expiration ? default_max_expiration : result;
  }

}
