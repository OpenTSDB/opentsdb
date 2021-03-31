// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.servlet.sinks;

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.QueryContext;

import java.io.ByteArrayOutputStream;

/**
 * Probably temporary interface that will take the serialized output of the
 * query and TEE the result to another destination. For now this is used to
 * validate query responses on a secondary system. Yes the serializers can be
 * chained but that's expensive to work through the full query graph twice so
 * we're doing this hacky thing for now.
 *
 * @since 3.0
 */
public interface ServletSinkTee extends TSDBPlugin {

  /**
   * Forwards the stream to another destination.
   * @param context The non-null context.
   * @param stream The non-null stream.
   */
  public void send(final QueryContext context, final ByteArrayOutputStream stream);

}
