// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.servlet.applications;

import javax.servlet.ServletConfig;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Context;

import org.glassfish.jersey.server.ResourceConfig;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.context.HttpContextFactory;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.servlet.exceptions.QueryExecutionExceptionMapper;
import net.opentsdb.stats.BraveTracer;
import net.opentsdb.utils.Config;

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
  
  public static final String HTTP_CONTEXT_FACTORY = "HTTPCTXFACTORY";
  
  public OpenTSDBApplication(@Context ServletConfig servletConfig) {
    try {
      final TSDB tsdb = new TSDB(new Config(true));
      servletConfig.getServletContext().setAttribute(TSD_ATTRIBUTE, tsdb);
      
      HttpContextFactory f = new HttpContextFactory();
      f.initialize(tsdb, tsdb.getTimer()).join();
      
      servletConfig.getServletContext().setAttribute(HTTP_CONTEXT_FACTORY, f);
      
      final int asyncTimeout;
      if (tsdb.getConfig().hasProperty("mt.async.timeout")) {
        asyncTimeout = tsdb.getConfig().getInt("mt.async.timeout");
      } else {
        asyncTimeout = DEFAULT_ASYNC_TIMEOUT;
      }
      servletConfig.getServletContext().setAttribute(ASYNC_TIMEOUT_ATTRIBUTE, 
          asyncTimeout);
      
      // TEMP
      tsdb.getConfig().overrideConfig("tsdb.tracer.service_name", "OpenTSDBServlet");
      tsdb.getConfig().overrideConfig("tracer.brave.zipkin.endpoint", 
          "http://127.0.0.1:9411/api/v1/spans");
      
      tsdb.getRegistry().registerTracer(new BraveTracer());
      
      tsdb.initializeRegistry().join();
      
      register(GenericExceptionMapper.class);
      register(new QueryExecutionExceptionMapper(false, 1024));
      
    } catch (Exception e) {
      throw new RuntimeException("Unable to initialize OpenTSDB app!", e);
    }
  }
}
