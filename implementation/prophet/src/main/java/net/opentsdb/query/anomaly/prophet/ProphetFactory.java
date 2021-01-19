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
package net.opentsdb.query.anomaly.prophet;

import org.apache.commons.exec.CommandLine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.anomaly.BaseAnomalyFactory;

/**
 * The factory for Prophet training and predictions.
 *  
 * @since 3.0
 */
public class ProphetFactory extends BaseAnomalyFactory<ProphetConfig, Prophet> {
  
  public static final String TYPE = "Prophet";
  
  public static final String PROPHET_PYTHON_SCRIPT = "prophet.script.path";
  public static final String PROPHET_PYTHON_EXEC = "prophet.python3.executable";

  private String python_script;
  private String python_executable;
  private CommandLine cmd;
  
  public Prophet newNode(final QueryPipelineContext context, 
                         final ProphetConfig config) {
    return new Prophet(this, context, config);
  }

  @Override
  public ProphetConfig parseConfig(final ObjectMapper mapper, 
                                   final TSDB tsdb, 
                                   final JsonNode node) {
    return ProphetConfig.parse(mapper, tsdb, node);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.type = TYPE;
    return super.initialize(tsdb, id)
    .addCallback(new Callback<Object, Object>() {

      @Override
      public Object call(Object arg) throws Exception {
        if (!tsdb.getConfig().hasProperty(PROPHET_PYTHON_SCRIPT)) {
          tsdb.getConfig().register(
              PROPHET_PYTHON_SCRIPT,
              null,
              false,
              "Location of the python script.");
        }
        
        if (!tsdb.getConfig().hasProperty(PROPHET_PYTHON_EXEC)) {
          tsdb.getConfig().register(
              PROPHET_PYTHON_EXEC,
              "/usr/local/bin/python3",
              false,
              "Location of the python executable.");
        }
        
        python_script = tsdb.getConfig().getString(PROPHET_PYTHON_SCRIPT);
        python_executable = tsdb.getConfig().getString(PROPHET_PYTHON_EXEC);
        cmd = CommandLine.parse(python_executable + " " + python_script);
        return null;
      }
      
    });
  }

  @Override
  public String type() {
    return TYPE;
  }
  
  protected CommandLine commandLine() {
    return cmd;
  }
 
}
