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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.google.common.collect.Sets;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;

public class TestEnvironmentProvider {
  private Configuration config = mock(Configuration.class);
  private ProviderFactory factory = mock(ProviderFactory.class);
  private HashedWheelTimer timer = mock(HashedWheelTimer.class);
  
  @Test
  public void ctor() throws Exception {
    try (final EnvironmentProvider environment = 
        new EnvironmentProvider(factory, config, timer)) {
      assertEquals(EnvironmentProvider.SOURCE, environment.source());
    }
  }
  
  @Test
  public void getSetting() throws Exception {
    try (final EnvironmentProvider environment = 
        new EnvironmentProvider(factory, config, timer)) {
      
      final Map<String, String> env = System.getenv();
      if (env == null || env.isEmpty()) {
        // could happen, maybe possibly. would be really odd though.
        return;
      }
      final Entry<String, String> entry = env.entrySet().iterator().next();
      ConfigurationOverride override = environment.getSetting(entry.getKey());
      assertEquals(entry.getValue(), override.getValue());
      assertEquals(EnvironmentProvider.SOURCE, override.getSource());
      
      assertNull(environment.getSetting("some,key.thatshouldn'texist!!!!"));
    }
  }
  
  @Test
  public void reload() throws Exception {
    try (final EnvironmentProvider environment = 
        new EnvironmentProvider(factory, config, timer)) {
      
      final Map<String, String> env = System.getenv();
      if (env == null || env.isEmpty()) {
        // could happen, maybe possibly. would be really odd though.
        return;
      }
      final Entry<String, String> entry = env.entrySet().iterator().next();
      when(config.reloadableKeys()).thenReturn(Sets.newHashSet(entry.getKey()));
      environment.reload();
      
      verify(config, times(1)).addOverride(eq(entry.getKey()), 
          any(ConfigurationOverride.class));
    }
  }
}
