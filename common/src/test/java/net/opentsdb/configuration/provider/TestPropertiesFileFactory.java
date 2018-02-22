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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;

import org.junit.Test;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.configuration.Configuration;

public class TestPropertiesFileFactory {

  @Test
  public void handlesProtocol() throws Exception {
    try (final PropertiesFileFactory factory = new PropertiesFileFactory()) {
      assertFalse(factory.handlesProtocol("HttpS://mysite.com/file.conf"));
      assertTrue(factory.handlesProtocol("File:///mysite.com/file.conf"));
      assertTrue(factory.handlesProtocol("file:///mysite.com/file.txt"));
      assertTrue(factory.handlesProtocol("File://C:\\myfolder\\foo.properties"));
      assertFalse(factory.handlesProtocol("file:///mysite.com/file.txt.tmp"));
    }
  }
  
  @Test
  public void newInstance() throws Exception {
    try (final PropertiesFileFactory factory = new PropertiesFileFactory()) {
      assertEquals("file://.conf", factory.newInstance(
          mock(Configuration.class), 
          mock(HashedWheelTimer.class),
          Collections.emptySet(),
          "file://.conf").source());
    }
  }
  
}
