// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.stats;

import net.opentsdb.core.BaseTSDBPlugin;

/**
 * A plugin abstraction definition used for implementations of the OpenTracing
 * API including reporters, etc.
 * 
 * @since 3.0
 */
public abstract class TsdbTracer extends BaseTSDBPlugin {

  /**
   * Returns a new tracer for an operation. Uses the service name from the 
   * 'tsdb.tracer.service_name' config.
   * @param report Whether or not the tracer should report to external systems
   * or remain in-memory.
   * @return A trace object with methods to serialize the tracer results.
   */
  public abstract TsdbTrace getTracer(final boolean report);
  
  /**
   * Returns a new tracer for an operation with an override for the service 
   * name.
   * @param report Whether or not the tracer should report to external systems
   * or remain in-memory.
   * @param service_name An override for the service name. If null or empty,
   * the default is used.
   * @return A trace object with methods to serialize the tracer results.
   */
  public abstract TsdbTrace getTracer(final boolean report, 
                                      final String service_name);
  
}
