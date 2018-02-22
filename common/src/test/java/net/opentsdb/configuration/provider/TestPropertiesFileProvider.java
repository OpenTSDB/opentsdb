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
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.configuration.ConfigurationOverride;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileInputStream.class, PropertiesFileProvider.class, File.class })
public class TestPropertiesFileProvider {
  private ProviderFactory factory = mock(ProviderFactory.class);
  private Configuration config = mock(Configuration.class);
  private HashedWheelTimer timer = mock(HashedWheelTimer.class);
  private Set<String> reload_keys = Collections.emptySet();
  
  @Test
  public void ctorDefault() throws Exception {
    PowerMockito.whenNew(File.class)
      .withAnyArguments()
      .thenReturn(mock(File.class));
    try {
      new PropertiesFileProvider(factory, config, timer, reload_keys).close();;
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
    
    final File local = mock(File.class);
    when(local.exists()).thenReturn(true);
    
    PowerMockito.whenNew(File.class)
      .withAnyArguments()
      .thenReturn(local);
    PowerMockito.whenNew(FileInputStream.class)
      .withAnyArguments()
      .thenReturn(mock(FileInputStream.class));
    new PropertiesFileProvider(factory, config, timer, reload_keys).close();
  }
  
  @Test
  public void ctorWithFile() throws Exception {
    new PropertiesFileProvider(factory, config, timer, reload_keys, 
        "opentsdb.conf").close();
  }
  
  @Test
  public void reload() throws Exception {
    final Properties properties = new Properties();
    properties.put("tsd.conf", "foo");
    properties.put("key.2", "42");
    
    File file = mock(File.class);
    when(file.exists()).thenReturn(true);
    PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(file);
    
    PowerMockito.mockStatic(Properties.class);
    PowerMockito.whenNew(Properties.class).withAnyArguments()
      .thenReturn(properties);
    
    PowerMockito.whenNew(FileInputStream.class)
      .withAnyArguments()
      .thenReturn(mock(FileInputStream.class));
    final PropertiesFileProvider provider = new PropertiesFileProvider(factory, 
        config, timer, reload_keys, "opentsdb.conf");
    
    assertEquals(2, provider.cache().size());
    assertEquals("foo", provider.cache().get("tsd.conf"));
    assertEquals("42", provider.cache().get("key.2"));
    
    // key change
    properties.put("key.2", "24");
    provider.reload();
    
    assertEquals(2, provider.cache().size());
    assertEquals("foo", provider.cache().get("tsd.conf"));
    assertEquals("24", provider.cache().get("key.2"));
    
    // drop and add
    properties.remove("key.2");
    properties.put("key.3", "boo!");
    provider.reload();
    
    assertEquals(2, provider.cache().size());
    assertEquals("foo", provider.cache().get("tsd.conf"));
    assertEquals("boo!", provider.cache().get("key.3"));
    
    provider.close();
  }
  
  @Test
  public void getSetting() throws Exception {
    final Properties properties = new Properties();
    properties.put("tsd.conf", "foo");
    properties.put("key.2", "42");
    
    PowerMockito.mockStatic(Properties.class);
    PowerMockito.whenNew(Properties.class).withAnyArguments()
      .thenReturn(properties);
    
    PowerMockito.whenNew(FileInputStream.class)
      .withAnyArguments()
      .thenReturn(mock(FileInputStream.class));
    final PropertiesFileProvider provider = new PropertiesFileProvider(factory, config, 
        timer, reload_keys, "opentsdb.conf");
    
    assertNull(provider.getSetting("no.such.key"));
    ConfigurationOverride override = provider.getSetting("tsd.conf");
    assertEquals("opentsdb.conf", override.getSource());
    assertEquals("foo", override.getValue());
    provider.close();
  }
}
