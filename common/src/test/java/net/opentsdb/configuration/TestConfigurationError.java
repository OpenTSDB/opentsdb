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
package net.opentsdb.configuration;

import org.junit.Test;

public class TestConfigurationError {

  @Test
  public void ctors() throws Exception {
    new ConfigurationException("Oops");
    new ConfigurationException(null);
    new ConfigurationException("");
    
    final Exception ex = new Exception("Boo!");
    new ConfigurationException("Oops", ex);
    new ConfigurationException(null, ex);
    new ConfigurationException("", ex);
    new ConfigurationException(null, ex);
  }
}
