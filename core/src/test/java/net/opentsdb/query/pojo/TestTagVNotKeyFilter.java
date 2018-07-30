// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.query.pojo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.query.pojo.TagVFilter;
import net.opentsdb.query.pojo.TagVNotKeyFilter;

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
