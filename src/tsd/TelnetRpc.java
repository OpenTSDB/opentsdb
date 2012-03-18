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
package net.opentsdb.tsd;

import com.stumbleupon.async.Deferred;

import org.jboss.netty.channel.Channel;

import net.opentsdb.core.TSDB;

/** Base interface for all telnet-style RPC handlers. */
interface TelnetRpc {

  /**
   * Executes this RPC.
   * @param tsdb The TSDB to use.
   * @param chan The channel on which the RPC was received.
   * @param command The command received, split.
   * @return A deferred result.
   */
  Deferred<Object> execute(TSDB tsdb, Channel chan, String[] command);

}
