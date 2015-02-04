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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static net.opentsdb.uid.UniqueIdType.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(UIDMeta.class)
public final class TestUIDMeta {
  @Test
  public void constructor2() {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 5});
    assertNotNull(meta);
    assertEquals(METRIC, meta.getType());
    assertArrayEquals(new byte[]{0, 0, 5}, meta.getUID());
  }
  
  @Test
  public void constructor3() {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 5}, "sys.cpu.5");
    assertNotNull(meta);
    assertEquals(METRIC, meta.getType());
    assertArrayEquals(new byte[]{0, 0, 5}, meta.getUID());
    assertEquals("sys.cpu.5", meta.getName());
    assertEquals(System.currentTimeMillis() / 1000, meta.getCreated());
  }

  @Test
  public void createConstructor() {
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L);
    final UIDMeta meta = new UIDMeta(TAGK, new byte[]{1, 0, 0}, "host");
    assertEquals(1357300800000L / 1000, meta.getCreated());
    assertArrayEquals(new byte[]{1, 0, 0}, meta.getUID());
    assertEquals("host", meta.getName());
  }

  @Test (expected = NullPointerException.class)
  public void ctorNullType() throws Exception {
    new UIDMeta(null, new byte[]{0}, null, false);
  }

  @Test (expected = NullPointerException.class)
  public void ctorNullUID() throws Exception {
    new UIDMeta(METRIC, null, null, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyUID() throws Exception {
    new UIDMeta(METRIC, new byte[] {}, null, false);
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctorWrongUIDLength() throws Exception {
    new UIDMeta(METRIC, new byte[] {0}, null, false);
    new UIDMeta(METRIC, new byte[] {0, 1, 2, 3}, null, false);
  }
}
