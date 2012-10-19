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
package net.opentsdb.uid;

import junit.framework.TestCase;

public final class TestNoSuchUniqueId extends TestCase {

  public void testMessage() {
    final byte[] id = { 0, 'A', 'a' };
    final NoSuchUniqueId e = new NoSuchUniqueId("Foo", id);
    assertEquals("No such unique ID for 'Foo': [0, 65, 97]", e.getMessage());
  }

  public void testFields() {
    final String kind = "bar";
    final byte[] id = { 42, '!' };
    final NoSuchUniqueId e = new NoSuchUniqueId(kind, id);
    assertEquals(kind, e.kind());
    assertEquals(id, e.id());
  }

}
