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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationOverride;

public class TestCommandLineProvider {
  private Configuration config = mock(Configuration.class);
  private ProviderFactory factory = mock(ProviderFactory.class);
  private HashedWheelTimer timer = mock(HashedWheelTimer.class);
  private Set<String> reload_keys = Collections.emptySet();
  
  @Test
  public void ctor() throws Exception {
    try {
      new CommandLineProvider(factory, config, timer, reload_keys, null).close();;
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    new CommandLineProvider(factory, config, timer, reload_keys, 
        new String[0]).close();
    new CommandLineProvider(factory, config, timer, reload_keys, 
        new String[] { "--test.conf=foo" }).close();
  }
  
  @Test
  public void getSettings() throws Exception {
    final String[] args = new String[] {
        "--test.conf=foo",
        "--test.bar=42"
    };
    try (final CommandLineProvider provider = 
        new CommandLineProvider(factory, config, timer, reload_keys, args)) {
      try {
        provider.getSetting(null);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      try {
        provider.getSetting("");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
      
      assertNull(provider.getSetting("no.such.key"));
      ConfigurationOverride override = provider.getSetting("test.conf");
      assertEquals("foo", override.getValue());
      
      override = provider.getSetting("test.bar");
      assertEquals("42", override.getValue());
    }
  }
}
