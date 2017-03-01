// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.query.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class TestTagVNotKeyFilter {
  private static final String TAGK = "host";
  private static final String TAGK2 = "owner";
  private Map<String, String> tags;
  
  @Before
  public void before() throws Exception {
    tags = new HashMap<String, String>(1);
    tags.put(TAGK, "ogg-01.ops.ankh.morpork.com");
    tags.put(TAGK2, "Hrun");
  }
  
  @Test
  public void matchHasKey() throws Exception {
    TagVFilter filter = new TagVNotKeyFilter(TAGK, "");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchDoesNotHaveKey() throws Exception {
    TagVFilter filter = new TagVNotKeyFilter("colo", "");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void ctorNullFilter() throws Exception {
    TagVFilter filter = new TagVNotKeyFilter(TAGK, null);
    assertTrue(filter.postScan());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorFilterHasValue() throws Exception {
    assertNotNull(new TagVNotKeyFilter(TAGK, "Evadne"));
  }
}
