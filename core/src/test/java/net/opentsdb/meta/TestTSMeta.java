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
package net.opentsdb.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public final class TestTSMeta {
  @Test
  public void constructor() { 
    assertNotNull(new TSMeta());
  }
 
  @Test
  public void createConstructor() {
    final TSMeta meta = new TSMeta(new byte[]{0, 0, 1, 0, 0, 2, 0, 0, 3}, 1357300800000L);
    assertEquals(1357300800000L / 1000, meta.getCreated());
  }
}
