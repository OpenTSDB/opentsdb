// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.data;

import net.opentsdb.data.pbuf.DataShardPB.DataShards;

/**
 * A class that takes shards from multiple clusters or multiple runs
 * and merges them into a single response using the implemented strategy.
 * 
 * @since 3.0
 */
public interface DataShardMergeStrategy {

  /**
   * Merge the given shards into one result.
   * <p>
   * Invariate: All shards in the array must be null or have the same base
   * time and time spans. Nulled shards are skipped.
   * <p>
   * This operation must be thread safe as the same merge strategy object
   * will be shared across multiple executors so this method may be called 
   * at the same time from multiple threads.
   * 
   * @param shards The list of shards to merge.
   * @return A shards object with the merged results. If all shards in the
   * list were null, the resulting shard will be null.
   */
  public DataShards merge(final DataShards[] shards);
  
}
