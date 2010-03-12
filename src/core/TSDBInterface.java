// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import net.opentsdb.HBaseException;

/**
 * Interface to a TSDB (Time Series DataBase).
 */
public interface TSDBInterface {

  /**
   * Returns a new {@link Query} instance suitable for this TSDB.
   */
  Query newQuery();

  /**
   * Returns a new {@link WritableDataPoints} instance suitable for this TSDB.
   */
  WritableDataPoints newDataPoints();

  /**
   * Forces a flush of any un-committed in memory data.
   * <p>
   * For instance, any data point not persisted will be sent to HBase.
   * @throws HBaseException if there was a problem sending un-committed data
   * to HBase.
   */
  void flush() throws HBaseException;

}
