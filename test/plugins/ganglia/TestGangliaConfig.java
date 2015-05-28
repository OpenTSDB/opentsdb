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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.utils.Config;


public class TestGangliaConfig {

  private Config config;

  @Before
  public void before() throws Exception {
    config = new Config(false);
  }

  @Test
  public void ganglia_udp_port() {
    GangliaConfig ganlia_config = new GangliaConfig(config);
    assertEquals(8649, ganlia_config.ganglia_udp_port());
  }

  @Test
  public void ganglia_udp_port_new_port() {
    config.overrideConfig("tsd.ganglia.udp.port", "1234");
    GangliaConfig ganlia_config = new GangliaConfig(config);
    assertEquals(1234, ganlia_config.ganglia_udp_port());
  }
}
