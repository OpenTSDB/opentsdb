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
package net.opentsdb.configuration.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.util.Map.Entry;

import org.junit.Test;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;

public class TestSystemPropertiesProvider {
  private Configuration config = mock(Configuration.class);
  private ProviderFactory factory = mock(ProviderFactory.class);
  private HashedWheelTimer timer = mock(HashedWheelTimer.class);
  
  @Test
  public void ctor() throws Exception {
    try (final SystemPropertiesProvider props = 
        new SystemPropertiesProvider(factory, config, timer)) {
      assertEquals(SystemPropertiesProvider.SOURCE, props.source());
    }
  }
  
  @Test
  public void getSetting() throws Exception {
    try (final SystemPropertiesProvider props = 
        new SystemPropertiesProvider(factory, config, timer)) {
      
      if (System.getProperties().isEmpty()) {
        // could happen, maybe sorta kinda
        return;
      }
      final Entry<Object, Object> entry = 
          System.getProperties().entrySet().iterator().next();
      ConfigurationOverride override = props.getSetting((String) entry.getKey());
      assertEquals(entry.getValue(), override.getValue());
      assertEquals(SystemPropertiesProvider.SOURCE, override.getSource());
      
      assertNull(props.getSetting("some,key.thatshouldn'texist!!!!"));
    }
  }
  
}
