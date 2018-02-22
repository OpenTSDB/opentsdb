// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestConfigSetting {

  @Test
  public void builder() throws Exception {
    ConfigSetting.Builder builder = ConfigSetting.newBuilder();
    builder.setSource("net.opentsdb.tsd");
    assertEquals("net.opentsdb.tsd", builder.source);
    try {
      builder.setSource(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      builder.setSource("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    builder.setValue(42L);
    assertEquals(42L, builder.value);
    builder.setValue(null);
    assertNull(builder.value);
    
    try {
      ConfigSetting.newBuilder()
          .setValue("Boo!")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void obfuscate() throws Exception {
    final ConfigSetting setting = ConfigSetting.newBuilder()
        .setSource("Here")
        .setValue("A nice string to hide")
        .build();
    assertTrue(setting.toString().contains("A nice string to hide"));
    assertFalse(setting.toString().contains("********"));
    
    setting.obfuscate = true;
    assertFalse(setting.toString().contains("A nice string to hide"));
    assertTrue(setting.toString().contains("********"));
  }
}
