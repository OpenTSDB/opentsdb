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

import net.opentsdb.core.TSDBPlugin;

/**
 * A tracing implementation for OpenTSDB that abstracts the actual framework.
 * This is meant to be implemented via plugins.
 * 
 * TODO - need APIs to pull from existing traces.
 * 
 * @since 3.0
 */
public interface Tracer extends TSDBPlugin {

  /**
   * Returns a new trace using the default service name.
   * @param report Whether or not the trace should report externally.
   * @param debug Whether or not to capture debug information.
   * @return A non-null trace ready to record spans.
   */
  public Trace newTrace(final boolean report, final boolean debug);
  
  /**
   * Returns a new trace.
   * @param report Whether or not the trace should report externally.
   * @param debug Whether or not to capture debug information.
   * @param service_name A non-null and non-empty service name.
   * @return A non-null trace ready to record spans.
   */
  public Trace newTrace(final boolean report, 
                        final boolean debug, 
                        final String service_name);
  
}
