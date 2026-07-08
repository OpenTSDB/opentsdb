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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.configuration.ConfigurationOverride;

public class TestPropertiesFileProvider {
  private MockedStatic<Files> mockedFiles;
  private ProviderFactory factory;
  private Configuration config;
  private HashedWheelTimer timer;
  private ByteSource source;
  private File file;
  private HashCode hash;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mockedFiles = Mockito.mockStatic(Files.class);
    factory = mock(ProviderFactory.class);
    config = mock(Configuration.class);
    timer = mock(HashedWheelTimer.class);
    source = mock(ByteSource.class);
    file = mock(File.class);

    when(file.exists()).thenReturn(true);
    mockedFiles.when(() -> Files.asByteSource(any(File.class))).thenReturn(source);

    hash = Const.HASH_FUNCTION().hashInt(1);
    when(source.hash(any(HashFunction.class))).thenReturn(hash);
  }

  @After
  public void tearDownStaticMocks() {
    mockedFiles.closeOnDemand();
  }

  @Test(expected = ConfigurationException.class)
  public void ctorDefault() throws Exception {
    new PropertiesFileProvider(factory, config, timer).close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void ctorNoProtocol() throws Exception {
    new PropertiesFileProvider(factory, config, timer, 
      "opentsdb.conf").close();
  }
  
  @Test
  public void testRealFile() throws Exception {
    new PropertiesFileProvider(factory, config, timer, 
      "file://src/test/resources/opentsdb.conf").close();
    
    try (final PropertiesFileProvider provider = new PropertiesFileProvider(
        factory, config, timer, "file://src/test/resources/opentsdb.conf")) {
      assertNull(provider.getSetting("no.such.key"));

      ConfigurationOverride override = provider.getSetting("tsd.network.port");
      assertEquals("1234", override.getValue());
      assertEquals("src/test/resources/opentsdb.conf", override.getSource());
    }
  }

  @Test
  public void reload() throws Exception {
    final File confFile = folder.newFile("opentsdb.conf");

    FileWriter writer = new FileWriter(confFile, false);
    writer.write("tsd.conf = foo\nkey.2 = 42\n");
    writer.close();

    try (final PropertiesFileProvider provider = new PropertiesFileProvider(
        factory, config, timer, "file://" + confFile)) {

      assertEquals(2, provider.cache().size());
      assertEquals("foo", provider.cache().get("tsd.conf"));
      assertEquals("42", provider.cache().get("key.2"));

      // change value of key.2
      writer = new FileWriter(confFile, false);
      writer.write("tsd.conf = foo\nkey.2 = 24\n");
      writer.close();
      hash = Const.HASH_FUNCTION().hashInt(2);
      when(source.hash(any(HashFunction.class))).thenReturn(hash);
      provider.reload();

      assertEquals(2, provider.cache().size());
      assertEquals("foo", provider.cache().get("tsd.conf"));
      assertEquals("24", provider.cache().get("key.2"));

      // drop key.2 and add key.3
      writer = new FileWriter(confFile, false);
      writer.write("tsd.conf = foo\nkey.3 = boo!\n");
      writer.close();
      hash = Const.HASH_FUNCTION().hashInt(3);
      when(source.hash(any(HashFunction.class))).thenReturn(hash);
      provider.reload();

      assertEquals(2, provider.cache().size());
      assertEquals("foo", provider.cache().get("tsd.conf"));
      assertEquals("boo!", provider.cache().get("key.3"));
    }
  }
}
