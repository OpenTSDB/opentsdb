// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

public class TestTsdbTracer {

  @Test
  public void successfulTags() throws Exception {
    Map<String, String> tags = TsdbTrace.successfulTags(
        "family", "Baratheon", "slogan", "Ours is the Fury");
    assertEquals(4, tags.size());
    assertEquals("OK", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    assertEquals("Baratheon", tags.get("family"));
    assertEquals("Ours is the Fury", tags.get("slogan"));
    
    tags = TsdbTrace.successfulTags(
        "family", "Baratheon", "slogan"/*, "Ours is the Fury"*/);
    assertEquals(3, tags.size());
    assertEquals("OK", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    assertEquals("Baratheon", tags.get("family"));
    assertNull(tags.get("slogan"));
    
    tags = TsdbTrace.successfulTags();
    assertEquals(2, tags.size());
    assertEquals("OK", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    
    tags = TsdbTrace.successfulTags(
        "family", "Baratheon", null, "Ours is the Fury");
    assertEquals(3, tags.size());
    assertEquals("OK", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    assertEquals("Baratheon", tags.get("family"));
    assertNull(tags.get("slogan"));
    
    tags = TsdbTrace.successfulTags(
        "family", "Baratheon", "slogan", null);
    assertEquals(3, tags.size());
    assertEquals("OK", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    assertEquals("Baratheon", tags.get("family"));
    assertNull(tags.get("slogan"));
    
    tags = TsdbTrace.successfulTags(
        "family", "Baratheon", null, null, "slogan", "Ours is the Fury");
    assertEquals(4, tags.size());
    assertEquals("OK", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    assertEquals("Baratheon", tags.get("family"));
    assertEquals("Ours is the Fury", tags.get("slogan"));
  }
  
  @Test
  public void canceledTags() throws Exception {
    final Exception e = new IllegalArgumentException("Boo!");
    Map<String, String> tags = TsdbTrace.canceledTags(e,
        "family", "Baratheon", "slogan", "Ours is the Fury");
    assertEquals(5, tags.size());
    assertEquals("Canceled", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    assertEquals("Boo!", tags.get("error"));
    assertEquals("Baratheon", tags.get("family"));
    assertEquals("Ours is the Fury", tags.get("slogan"));
    
    tags = TsdbTrace.canceledTags(null,
        "family", "Baratheon", "slogan", "Ours is the Fury");
    assertEquals(5, tags.size());
    assertEquals("Canceled", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    assertEquals("Canceled", tags.get("error"));
    assertEquals("Baratheon", tags.get("family"));
    assertEquals("Ours is the Fury", tags.get("slogan"));
  }
  
  @Test
  public void exceptionTags() throws Exception {
    final Exception e = new IllegalArgumentException("Boo!");
    Map<String, String> tags = TsdbTrace.exceptionTags(e,
        "family", "Baratheon", "slogan", "Ours is the Fury");
    assertEquals(5, tags.size());
    assertEquals("Error", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    assertEquals("Boo!", tags.get("error"));
    assertEquals("Baratheon", tags.get("family"));
    assertEquals("Ours is the Fury", tags.get("slogan"));
    
    tags = TsdbTrace.exceptionTags(null,
        "family", "Baratheon", "slogan", "Ours is the Fury");
    assertEquals(5, tags.size());
    assertEquals("Error", tags.get("status"));
    assertTrue(tags.containsKey("finalThread"));
    assertEquals("Unknown", tags.get("error"));
    assertEquals("Baratheon", tags.get("family"));
    assertEquals("Ours is the Fury", tags.get("slogan"));
  }
  
  @Test
  public void exceptionAnnotation() throws Exception {
    final Exception e = new IllegalArgumentException("Boo!");
    Map<String, Object> notes = TsdbTrace.exceptionAnnotation(e,
        "long", 42, "double", 24.5);
    assertEquals(3, notes.size());
    assertSame(e, notes.get("exception"));
    assertEquals(42, notes.get("long"));
    assertEquals(24.5, (Double) notes.get("double"), 0.01);
    
    notes = TsdbTrace.exceptionAnnotation(null,"long", 42, "double", 24.5);
    assertEquals(3, notes.size());
    assertEquals("null", notes.get("exception"));
    assertEquals(42, notes.get("long"));
    assertEquals(24.5, (Double) notes.get("double"), 0.01);
  }
  
  @Test
  public void annotations() throws Exception {
    Map<String, Object> notes = TsdbTrace.annotations(
        "long", 42, "double", 24.5);
    assertEquals(2, notes.size());
    assertEquals(42, notes.get("long"));
    assertEquals(24.5, (Double) notes.get("double"), 0.01);
    
    notes = TsdbTrace.annotations("long", 42, null, 24.5);
    assertEquals(1, notes.size());
    assertEquals(42, notes.get("long"));
    assertNull(notes.get("double"));
    
    notes = TsdbTrace.annotations("long", 42, "double", null);
    assertEquals(1, notes.size());
    assertEquals(42, notes.get("long"));
    assertNull(notes.get("double"));
    
    notes = TsdbTrace.annotations("long", 42, null, null);
    assertEquals(1, notes.size());
    assertEquals(42, notes.get("long"));
    assertNull(notes.get("double"));
    
    notes = TsdbTrace.annotations("long", 42, null, null, "double", 24.5);
    assertEquals(2, notes.size());
    assertEquals(42, notes.get("long"));
    assertEquals(24.5, (Double) notes.get("double"), 0.01);
    
    notes = TsdbTrace.annotations("long", 42, 24.5, "double");
    assertEquals(1, notes.size());
    assertEquals(42, notes.get("long"));
    assertNull(notes.get("double"));
    
    notes = TsdbTrace.annotations("long", 42, "double");
    assertEquals(1, notes.size());
    assertEquals(42, notes.get("long"));
    assertNull(notes.get("double"));
  }
}
