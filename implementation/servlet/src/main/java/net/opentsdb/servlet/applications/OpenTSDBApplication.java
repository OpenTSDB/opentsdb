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
package net.opentsdb.servlet.applications;

import javax.servlet.ServletConfig;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Context;

import org.glassfish.jersey.server.ResourceConfig;

import com.google.common.collect.ImmutableMap;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.servlet.exceptions.QueryExecutionExceptionMapper;
import net.opentsdb.servlet.resources.JMXResource;
import net.opentsdb.servlet.resources.QueryRpc;
import net.opentsdb.servlet.resources.RawQueryRpc;

@ApplicationPath("/")
public class OpenTSDBApplication extends ResourceConfig {

  /** The TSDB attribute for storing in the context. */
  public static final String TSD_ATTRIBUTE = "TSDB";
  
  /** The ASYNC timeout attribute for storing in the context. */
  public static final String ASYNC_TIMEOUT_ATTRIBUTE = "ASYNCTIMEOUT";
  public static final int DEFAULT_ASYNC_TIMEOUT = 60000;
  
  /** Query response keys for async calls. */
  public static final String QUERY_EXCEPTION_ATTRIBUTE = "QUERYEX";
  public static final String QUERY_RESULT_ATTRIBUTE = "QUERYRESULT";
  
  public OpenTSDBApplication(@Context ServletConfig servletConfig) {
    try {
      final Object pre_instantiated_tsd = servletConfig.getServletContext()
          .getAttribute(TSD_ATTRIBUTE);
      final DefaultTSDB tsdb;
      if (pre_instantiated_tsd != null && pre_instantiated_tsd instanceof DefaultTSDB) {
        tsdb = (DefaultTSDB) pre_instantiated_tsd;
      } else {
        tsdb = new DefaultTSDB(new Configuration()); 
        servletConfig.getServletContext().setAttribute(TSD_ATTRIBUTE, tsdb);
        tsdb.initializeRegistry(true).join();
      }
      
      final int asyncTimeout;
      if (tsdb.getConfig().hasProperty("tsd.http.async.timeout")) {
        asyncTimeout = tsdb.getConfig().getInt("tsd.http.async.timeout");
      } else {
        asyncTimeout = DEFAULT_ASYNC_TIMEOUT;
      }
      servletConfig.getServletContext().setAttribute(ASYNC_TIMEOUT_ATTRIBUTE, 
          asyncTimeout);

      register(QueryRpc.class);
      register(RawQueryRpc.class);
      register(JMXResource.class);
      register(GenericExceptionMapper.class);
      register(new QueryExecutionExceptionMapper(false, 1024));
      
      addProperties(ImmutableMap.of(
          "jersey.config.server.monitoring.statistics.mbeans.enabled", "true"));
      
    } catch (Exception e) {
      throw new RuntimeException("Unable to initialize OpenTSDB app!", e);
    }
  }
  
}
