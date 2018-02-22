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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;

public class TestProvider {

  private ProviderFactory factory = mock(ProviderFactory.class);
  private Configuration config = mock(Configuration.class);
  private HashedWheelTimer timer = mock(HashedWheelTimer.class);
  private Set<String> reload_keys = Collections.emptySet();
  
  @Test
  public void ctor() throws Exception {
    try {
      new MockProvider(null, config, timer, reload_keys).close();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MockProvider(factory, null, timer, reload_keys).close();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MockProvider(factory, config, null, reload_keys).close();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MockProvider(factory, config, timer, null).close();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try (final MockProvider provider = 
        new MockProvider(factory, config, timer, reload_keys)) {
      assertSame(config, provider.config);
      assertSame(timer, provider.timer);
      assertSame(reload_keys, provider.reload_keys);
    }
  }
  
  @Test
  public void timerTask() throws Exception {
    when(config.getTyped(anyString(), any(Class.class))).thenReturn(42L);
    
    try (final MockProvider provider = 
        new MockProvider(factory, config, timer, reload_keys)) {
      assertFalse(provider.reloaded);
      
      provider.run(null);
      
      assertTrue(provider.reloaded);
      verify(timer, times(1)).newTimeout(eq(provider), 
          anyLong(), eq(TimeUnit.SECONDS));
    }
    
    try (final MockProvider provider = 
        new MockProvider(factory, config, timer, reload_keys)) {
      provider.throw_exception_on_reload = true;
      assertFalse(provider.reloaded);
      
      provider.run(null);
      
      assertTrue(provider.reloaded);
      verify(timer, times(1)).newTimeout(eq(provider), 
          anyLong(), eq(TimeUnit.SECONDS));
    }
  }
  
  static class MockProvider extends Provider {
    public MockProvider(final ProviderFactory factory, 
                        final Configuration config,
                        final HashedWheelTimer timer, 
                        final Set<String> reload_keys) {
      super(factory, config, timer, reload_keys);
    }

    boolean reloaded = false;
    boolean throw_exception_on_reload = false;
    @Override
    public void close() throws IOException {  }

    @Override
    public ConfigurationOverride getSetting(final String key) { return null; }

    @Override
    public String source() { return null; }

    @Override
    public void reload() { 
      reloaded = true;
      if (throw_exception_on_reload) {
        throw new RuntimeException("Boo!");
      }
    }
    
  }
}
