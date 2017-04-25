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

import net.opentsdb.query.pojo.TimeSeriesQuery;

/**
 * Simple implementation of the key generator that prepends keys with
 * "TSDBQ".
 * 
 * @since 3.0
 */
public class DefaultTimeSeriesCacheKeyGenerator 
  extends TimeSeriesCacheKeyGenerator {
  
  public static final byte[] CACHE_PREFIX = 
      new byte[] { 'T', 'S', 'D', 'B', 'Q' };

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

}
