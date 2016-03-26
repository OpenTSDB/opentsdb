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
package net.opentsdb.tools;

import net.opentsdb.utils.Config;

/**
 * Static reference to the TSD's listening port and stats port configuration
 */

public class TSDPort {
	/** The RPC listening port  */
	private static int rpcPort = -1;
	/** Indicates if RPC stats include the listening port. Set by config <b><code>tsd.core.stats_with_port</code></b>
	 or CLI option <b><code>--statswport</code></b>.  */
	private static boolean statsWithPort = false;
	
	/**
	 * Sets the rpc port and stats config on TSD startup
	 * @param config The final config
	 */
	static void set(Config config) {
		rpcPort = config.getInt("tsd.network.port");
	    statsWithPort = config.getBoolean("tsd.core.stats_with_port");
	}

	/**
	 * Returns the TSD's listening port
	 * @return the port
	 */
	public static int getTSDPort()  {
		return rpcPort;
	}

	/**
	 * Indicates if stats should be reported with the port as a tag
	 * @return true if stats should be reported with the port as a tag, false otherwise
	 */
	public static boolean isStatsWithPort() {
		return statsWithPort;
	}


	private TSDPort() {}

}
