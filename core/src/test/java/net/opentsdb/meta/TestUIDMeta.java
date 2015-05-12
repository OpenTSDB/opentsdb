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

import net.opentsdb.core.IllegalDataException;
import net.opentsdb.uid.UniqueIdType;
import org.junit.Test;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static net.opentsdb.uid.UniqueIdType.TAGK;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public final class TestUIDMeta {
  private final byte[] VALID_UID = new byte[] {0, 0, 1};
  private final UniqueIdType VALID_TYPE = METRIC;
  private final String VALID_NAME = "valid_name";
  private final String VALID_DESCRIPTION = "valid_description";
  private final long VALID_CREATED = 100;

  @Test(expected = NullPointerException.class)
  public void testCtorNoUID() {
    UIDMeta.create(null, VALID_TYPE, VALID_NAME, VALID_DESCRIPTION, VALID_CREATED);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCtorWrongUIDLength() {
    final byte[] wrong_length_uid = new byte[] {0, 1};
    UIDMeta.create(wrong_length_uid, VALID_TYPE, VALID_NAME, VALID_DESCRIPTION, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoType() {
    UIDMeta.create(VALID_UID, null, VALID_NAME, VALID_DESCRIPTION, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoName() {
    UIDMeta.create(VALID_UID, VALID_TYPE, null, VALID_DESCRIPTION, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorEmptyName() {
    UIDMeta.create(VALID_UID, VALID_TYPE, "", VALID_DESCRIPTION, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoDescription() {
    UIDMeta.create(VALID_UID, VALID_TYPE, VALID_NAME, null, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorEmptyDescription() {
    UIDMeta.create(VALID_UID, VALID_TYPE, VALID_NAME, "", VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorArgumentOrder() {
    final UIDMeta meta = UIDMeta.create(VALID_UID, VALID_TYPE, VALID_NAME, VALID_DESCRIPTION, VALID_CREATED);
    assertArrayEquals(VALID_UID, meta.uid());
    assertEquals(VALID_TYPE, meta.type());
    assertEquals(VALID_NAME, meta.name());
    assertEquals(VALID_DESCRIPTION, meta.description());
    assertEquals(VALID_CREATED, meta.created());
  }
}
