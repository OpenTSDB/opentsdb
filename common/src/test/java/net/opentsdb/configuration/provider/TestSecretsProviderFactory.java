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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;

public class TestSecretsProviderFactory {

  @Test
  public void ctor() throws Exception {
    try (SecretProviderFactory factory = new SecretProviderFactory()) {
      assertEquals("Secrets", factory.simpleName());
      assertTrue(factory.isReloadable());
      assertEquals(SecretProviderFactory.PROTOCOL, factory.protocol());
    }
  }
  
  @Test
  public void newInstance() throws Exception {
    try (SecretProviderFactory factory = new SecretProviderFactory()) {
      Provider provider = factory.newInstance(mock(Configuration.class), 
                                              mock(HashedWheelTimer.class), 
                                              null,
          "secrets://net.opentsdb.configuration.provider.PlainTextSecretProvider");
      assertTrue(provider instanceof PlainTextSecretProvider);
      
      try {
        factory.newInstance(mock(Configuration.class), 
            mock(HashedWheelTimer.class), 
            null,
            "secrets://net.opentsdb.configuration.provider.noSuchClass");
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) { }
    }
  }
}
