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
package net.opentsdb.core;

/**
 * Helper class to init a singleton default TSDB for the unit test suite
 * that loads default plugins.
 */
public class MockTSDBDefault {
  private static Object MUTEX = new Object();
  private static MockTSDB TSDB;
  
  private MockTSDBDefault() {
    // don't init me!
  }
  
  public static MockTSDB getMockTSDB() {
    if (TSDB == null) {
      // we need a lock as tests can be run in multiple threads.
      synchronized(MUTEX) {
        if (TSDB == null) {
          TSDB = new MockTSDB();
          TSDB.registry = new DefaultRegistry(TSDB);
          try {
            ((DefaultRegistry) TSDB.registry).initialize(true).join(60_000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
    
    return TSDB;
  }
}
