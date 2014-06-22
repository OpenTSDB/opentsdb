// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
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
package net.opentsdb.plugins.ganglia;

import javax.annotation.Nullable;

import net.opentsdb.utils.Config;

/** Configuration for the Ganglia plugin. */
public class GangliaConfig {

  /** The default Ganglia udp port. */
  private static final int UDP_PORT = 8649;

  /** OpenTSDB Configuration. */
  private final Config config;

  public GangliaConfig(Config config) {
    this.config = config;
  }

  /** @return UDP port to receive Ganglia v30x and v31x messages. */
  public int ganglia_udp_port() {
    final String property = "tsd.ganglia.udp.port";
    if (config.getString(property) != null) {
      return config.getInt(property);
    }
    return UDP_PORT;  // Default value.
  }

  /** @return The prefix to the Ganglia metric names. */
  @Nullable
  public String ganglia_name_prefix() {
    return config.getString("tsd.ganglia.name.prefix");
  }

  /** @return Cluster name tag to add for the Ganglia metrics. */
  @Nullable
  public String ganglia_tags_cluster() {
    return config.getString("tsd.ganglia.tags.cluster");
  }
}
