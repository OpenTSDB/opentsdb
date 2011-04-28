// This file is part of OpenTSDB.
// Copyright (C) 2011  StumbleUpon, Inc.
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

import java.util.HashMap;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public final class TestTags {

  @Test
  public void parseSuccessful() {
    final HashMap<String, String> tags = new HashMap<String, String>(2);
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
    Tags.parse(tags, "qux=baz");
    assertEquals(2, tags.size());
    assertEquals("bar", tags.get("foo"));
    assertEquals("baz", tags.get("qux"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void parseNoEqualSign() {
    Tags.parse(new HashMap<String, String>(1), "foo");
  }

  @Test(expected=IllegalArgumentException.class)
  public void parseTooManyEqualSigns() {
    Tags.parse(new HashMap<String, String>(1), "foo=bar=qux");
  }

  @Test(expected=IllegalArgumentException.class)
  public void parseEmptyTagName() {
    Tags.parse(new HashMap<String, String>(1), "=bar");
  }

  @Test(expected=IllegalArgumentException.class)
  public void parseEmptyTagValue() {
    Tags.parse(new HashMap<String, String>(1), "foo=");
  }

  @Test(expected=IllegalArgumentException.class)
  public void parseDifferentValues() {
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
    Tags.parse(tags, "foo=qux");
  }

  @Test
  public void parseSameValues() {
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
    Tags.parse(tags, "foo=bar");
    assertEquals(1, tags.size());
    assertEquals("bar", tags.get("foo"));
  }

  @Test
  public void validateGoodString() {
    Tags.validateString("test", "omg-TSDB/42._foo_");
  }

  @Test(expected=IllegalArgumentException.class)
  public void validateNullString() {
    Tags.validateString("test", null);
  }

  @Test(expected=IllegalArgumentException.class)
  public void validateBadString() {
    Tags.validateString("test", "this is a test!");
  }

}
