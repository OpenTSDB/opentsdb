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

import java.io.File;
import java.lang.reflect.Constructor;

import javax.servlet.ServletConfig;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Context;

import org.glassfish.jersey.server.ResourceConfig;

import com.google.common.base.Strings;
import com.google.common.io.Files;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.query.execution.CachingQueryExecutor;
import net.opentsdb.query.execution.DefaultQueryExecutorFactory;
import net.opentsdb.query.execution.HttpQueryV2Executor;
import net.opentsdb.query.execution.MetricShardingExecutor;
import net.opentsdb.query.execution.MultiClusterQueryExecutor;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorFactory;
import net.opentsdb.query.execution.TimeSlicedCachingExecutor;
import net.opentsdb.query.execution.cluster.ClusterConfig;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.servlet.exceptions.QueryExecutionExceptionMapper;
import net.opentsdb.servlet.resources.V2QueryResource;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

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
      final TSDB tsdb;
      if (pre_instantiated_tsd != null && pre_instantiated_tsd instanceof TSDB) {
        tsdb = (TSDB) pre_instantiated_tsd;
      } else {
        tsdb = new TSDB(new Config(true)); 
        servletConfig.getServletContext().setAttribute(TSD_ATTRIBUTE, tsdb);
      }
      
      final int asyncTimeout;
      if (tsdb.getConfig().hasProperty("tsd.http.async.timeout")) {
        asyncTimeout = tsdb.getConfig().getInt("tsd.http.async.timeout");
      } else {
        asyncTimeout = DEFAULT_ASYNC_TIMEOUT;
      }
      servletConfig.getServletContext().setAttribute(ASYNC_TIMEOUT_ATTRIBUTE, 
          asyncTimeout);
      
      setupDefaultExecutors(tsdb);

      register(V2QueryResource.class);
      register(GenericExceptionMapper.class);
      register(new QueryExecutionExceptionMapper(false, 1024));
      
    } catch (Exception e) {
      throw new RuntimeException("Unable to initialize OpenTSDB app!", e);
    }
  }
  
  @SuppressWarnings("unchecked")
  public void setupDefaultExecutors(final TSDB tsdb) {
    try {      
      Constructor<?> ctor/* = MetricShardingExecutor.class.getConstructor(
          ExecutionGraphNode.class);
      QueryExecutorFactory<DataShardsGroups> sink = 
          new DefaultQueryExecutorFactory<DataShardsGroups>(
              (Constructor<QueryExecutor<?>>) ctor,
                MetricShardingExecutor.Config.<DataShardsGroups>newBuilder()
                .setParallelExecutors(20)
                .setType(DataShardsGroups.class)
                //.setDataMerger((DataMerger<DataShardsGroups>) mergers.get(DataShardsGroups.TYPE))
                .build());
      
      final List<ClusterDescriptor> clusters = 
          Lists.newArrayListWithExpectedSize(endpoints.getEndpoints(null).size());
      final List<String> snapshot = endpoints.getEndpoints(null);
      for (int i = 0; i < snapshot.size(); i++) {
        clusters.add(new HttpCluster(i, snapshot));
      }
      ctor*/  = HttpQueryV2Executor.class.getConstructor(ExecutionGraphNode.class);
      QueryExecutorFactory<IteratorGroups> factory = 
              new DefaultQueryExecutorFactory<IteratorGroups>(
                  (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class, "HttpQueryV2Executor");
      tsdb.getRegistry().registerFactory(factory);

      ctor = CachingQueryExecutor.class.getConstructor(ExecutionGraphNode.class);
      factory = 
          new DefaultQueryExecutorFactory<IteratorGroups>(
              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class, "CachingQueryExecutor");
      tsdb.getRegistry().registerFactory(factory);
      
      ctor = TimeSlicedCachingExecutor.class.getConstructor(ExecutionGraphNode.class);
      factory = 
          new DefaultQueryExecutorFactory<IteratorGroups>(
              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class, "TimeSlicedCachingExecutor");
      tsdb.getRegistry().registerFactory(factory);
      
      ctor = MetricShardingExecutor.class.getConstructor(ExecutionGraphNode.class);
      factory = 
          new DefaultQueryExecutorFactory<IteratorGroups>(
              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class, "MetricShardingExecutor");
      tsdb.getRegistry().registerFactory(factory);

      ctor = MultiClusterQueryExecutor.class.getConstructor(
              ExecutionGraphNode.class);
      QueryExecutorFactory<IteratorGroups> downstream = 
          new DefaultQueryExecutorFactory<IteratorGroups>(
              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class,
                "MultiClusterQueryExecutor");
      tsdb.getRegistry().registerFactory(downstream);
      
      String cluster_conf = tsdb.getConfig().getString("tsd.servlet.config.default_cluster");
      if (!Strings.isNullOrEmpty(cluster_conf)) {
        ClusterConfig graph = JSON.parseToObject(cluster_conf, ClusterConfig.class);
        graph.initialize(tsdb).join(1);
        tsdb.getRegistry().registerClusterConfig(graph);
      }
      
      String exec_graph = tsdb.getConfig().getString("tsd.servlet.config.default_execution_graph");
      if (!Strings.isNullOrEmpty(exec_graph)) {
        if (exec_graph.endsWith(".json")) {
          exec_graph = Files.toString(new File(exec_graph), Const.UTF8_CHARSET);
        }
        ExecutionGraph eg = JSON.parseToObject(exec_graph, ExecutionGraph.class);
        eg.initialize(tsdb, null).join(1);
        tsdb.getRegistry().registerExecutionGraph(eg, true);
      }
      
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize default query "
          + "executor context", e);
    } 
  }
}
