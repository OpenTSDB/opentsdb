// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;
import net.opentsdb.stats.StatsCollector;
import org.hbase.async.*;
import org.hbase.async.HBaseClient;


import java.util.ArrayList;
import java.util.Map;

/**
 * A interface defining the functions any database used with TSDB must implement.
 * Another requirement is tha the database connection has to be asynchronous.
 */
public interface TsdbStore {
  public Deferred<Object> addPoint(final byte[] row,
                                   final long timestamp,
                                   final byte[] value,
                                   final short flags);

  public Deferred<Long> atomicIncrement(AtomicIncrementRequest air);

  public Deferred<Long> bufferAtomicIncrement(final AtomicIncrementRequest request);

  public Deferred<Boolean> compareAndSet(final PutRequest edit, final byte[] expected);

  public Deferred<Object> delete(final DeleteRequest request);

  /**
   * Ensures that a given table really exists.
   *
   * @param table The name of the table you intend to use.
   * @return A deferred object that indicates the completion of the request.
   * You probably want to attach at least an errback to this Deferred to
   * handle failures.
   */
  public Deferred<ArrayList<Object>> checkNecessaryTablesExist();

  public Deferred<Object> flush();

  public Deferred<ArrayList<KeyValue>> get(final GetRequest request);

  long getFlushInterval();

  public Scanner newScanner(final byte[] table);

  public Deferred<Object> put(final PutRequest request);

  void setFlushInterval(short aShort);

  public Deferred<Object> shutdown();

  void collectStats(StatsCollector collector);
}
