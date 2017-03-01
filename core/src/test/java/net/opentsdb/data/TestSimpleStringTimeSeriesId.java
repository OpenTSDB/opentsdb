// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestSimpleStringTimeSeriesId {

  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    tags.put("colo", "lax");
    
    final SimpleStringTimeSeriesId id1 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    
    SimpleStringTimeSeriesId id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID2") // <-- Diff
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB", "EGADS"))  // <-- Diff
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        //.setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))  // <-- Diff
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("OpenTSDB", "Yahoo"))  // <-- Diff order OK!
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user")) 
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        //.setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle")) // <-- Diff
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.idle", "sys.cpu.user")) // <-- Diff order OK!
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    Map<String, String> tags2 = Maps.newHashMap();
    tags.put("host", "web02"); // <-- Diff
    tags.put("colo", "lax");
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags2) // <-- Diff
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    tags2 = Maps.newHashMap();
    tags.put("host", "web01");
    //tags.put("colo", "lax"); // <-- Diff
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags2) // <-- Diff
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        //.setTags(tags) // <-- Diff
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner")) // <-- Diff
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        //.setAggregatedTags(Lists.newArrayList("owner", "role")) // <-- Diff
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("role", "owner")) // <-- Diff order OK!
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery"))  // <-- Diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        //.setDisjointTags(Lists.newArrayList("propery", "type"))  // <-- Diff
        .build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("FakeID")
        .setNamespaces(Lists.newArrayList("Yahoo", "OpenTSDB"))
        .setMetrics(Lists.newArrayList("sys.cpu.user", "sys.cpu.idle"))
        .setTags(tags) 
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("type", "propery")) // <-- Diff order OK!
        .build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
  }
}
