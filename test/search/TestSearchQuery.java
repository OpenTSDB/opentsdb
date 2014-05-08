// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.search;

import static org.junit.Assert.assertEquals;
import net.opentsdb.search.SearchQuery.SearchType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public final class TestSearchQuery {

  @Test
  public void parseSearchTypeTSMeta() throws Exception {
    assertEquals(SearchType.TSMETA, SearchQuery.parseSearchType("tsmeta"));
  }
  
  @Test
  public void parseSearchTypeTSMetaSummary() throws Exception {
    assertEquals(SearchType.TSMETA_SUMMARY, 
        SearchQuery.parseSearchType("TSMeta_Summary"));
  }
  
  @Test
  public void parseSearchTypeTSUIDs() throws Exception {
    assertEquals(SearchType.TSUIDS, SearchQuery.parseSearchType("tsuids"));
  }
  
  @Test
  public void parseSearchTypeUIDMeta() throws Exception {
    assertEquals(SearchType.UIDMETA, SearchQuery.parseSearchType("UIDMeta"));
  }
  
  @Test
  public void parseSearchTypeAnnotation() throws Exception {
    assertEquals(SearchType.ANNOTATION, 
        SearchQuery.parseSearchType("Annotation"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseSearchTypeNull() throws Exception {
    SearchQuery.parseSearchType(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseSearchTypeEmtpy() throws Exception {
    SearchQuery.parseSearchType("");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void parseSearchTypeInvalid() throws Exception {
    SearchQuery.parseSearchType("NotAType");
  }
  
}
