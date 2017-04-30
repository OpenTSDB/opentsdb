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

import java.lang.reflect.Constructor;

import javax.servlet.ServletConfig;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Context;

import org.glassfish.jersey.server.ResourceConfig;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.query.execution.CachingQueryExecutor;
import net.opentsdb.query.execution.DefaultQueryExecutorFactory;
import net.opentsdb.query.execution.HttpQueryV2Executor;
import net.opentsdb.query.execution.MetricShardingExecutor;
import net.opentsdb.query.execution.MultiClusterQueryExecutor;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorFactory;
import net.opentsdb.query.execution.cache.GuavaLRUCache;
import net.opentsdb.query.execution.cluster.ClusterConfig;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.servlet.exceptions.QueryExecutionExceptionMapper;
import net.opentsdb.stats.BraveTracer;
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
  
  public static final String HTTP_CONTEXT_FACTORY = "HTTPCTXFACTORY";
  
  public OpenTSDBApplication(@Context ServletConfig servletConfig) {
    try {
      final TSDB tsdb = new TSDB(new Config(true));
      servletConfig.getServletContext().setAttribute(TSD_ATTRIBUTE, tsdb);
      
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
      setupDefaultExecutors(tsdb);

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
      QueryExecutorFactory<IteratorGroups> http_factory = 
              new DefaultQueryExecutorFactory<IteratorGroups>(
                  (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class, "HttpQueryV2Executor");
      tsdb.getRegistry().registerFactory(http_factory);

      ctor = CachingQueryExecutor.class.getConstructor(ExecutionGraphNode.class);
      QueryExecutorFactory<IteratorGroups> cache_factory = 
          new DefaultQueryExecutorFactory<IteratorGroups>(
              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class, "CachingQueryExecutor");
      tsdb.getRegistry().registerFactory(cache_factory);

      ctor = MultiClusterQueryExecutor.class.getConstructor(
              ExecutionGraphNode.class);
      QueryExecutorFactory<IteratorGroups> downstream = 
          new DefaultQueryExecutorFactory<IteratorGroups>(
              (Constructor<QueryExecutor<?>>) ctor, IteratorGroups.class,
                "MultiClusterQueryExecutor");
      tsdb.getRegistry().registerFactory(downstream);
      
      //String cluster_conf = "{\"id\":\"http_cluster\",\"executionGraph\":{\"id\":\"http_cluster\",\"nodes\":[{\"executorId\":\"http\",\"dataType\":\"timeseries\",\"upstream\":null,\"executorType\":\"HttpQueryV2Executor\",\"defaultConfig\":{\"executorType\":\"HttpQueryV2Executor\",\"executorId\":\"http\",\"timeout\":6000}}]},\"config\":{\"id\":\"StaticConfig\",\"implementation\":\"StaticClusterConfig\",\"clusters\":[{\"cluster\":\"Primary\",\"description\":\"somethinguseful\",\"executorConfigs\":[{\"executorType\":\"HttpQueryV2Executor\",\"executorId\":\"Primary_http\",\"endpoint\":\"http://data.yamas.ops.yahoo.com:9999\"}]},{\"cluster\":\"Secondary\",\"description\":\"somethinguseful\",\"executorConfigs\":[{\"executorType\":\"HttpQueryV2Executor\",\"executorId\":\"Secondary_http\",\"endpoint\":\"http://bcp-data.yamas.ops.yahoo.com:9999\"}]}],\"overrides\":[{\"id\":\"prod-pri\",\"clusters\":[{\"cluster\":\"Primary\"}]},{\"id\":\"prod-bcp\",\"clusters\":[{\"cluster\":\"Secondary\"}]},{\"id\":\"prod-gq1\",\"clusters\":[{\"cluster\":\"Primary\",\"executorConfigs\":[{\"executorType\":\"HttpQueryV2Executor\",\"executorId\":\"Primary_http\",\"endpoint\":\"http://api-tsdbr-gq1.yamas.ops.yahoo.com:9999/\"}]}]},{\"id\":\"prod-bf1\",\"clusters\":[{\"cluster\":\"Primary\",\"executorConfigs\":[{\"executorType\":\"HttpQueryV2Executor\",\"executorId\":\"Primary_http\",\"endpoint\":\"http://api-tsdbr-bf1.yamas.ops.yahoo.com:9999/\"}]}]},{\"id\":\"hosts\",\"clusters\":[{\"cluster\":\"Primary\",\"executorConfigs\":[{\"executorType\":\"HttpQueryV2Executor\",\"executorId\":\"Primary_http\",\"endpoint\":\"http://ymstsdb-11.ops.gq1.yahoo.com:9999\"}]}]}]}}";
      String cluster_conf = tsdb.getConfig().getString("tsd.servlet.config.default_cluster");
      ClusterConfig graph = JSON.parseToObject(cluster_conf, ClusterConfig.class);
      graph.initialize(tsdb).join(1);
      tsdb.getRegistry().registerClusterConfig(graph);
      
      //String exec_graph = "{\"id\":\"default_executor_context\",\"nodes\":[{\"executorId\":\"HttpMultiCluster\",\"dataType\":\"timeseries\",\"upstream\":null,\"executorType\":\"MultiClusterQueryExecutor\",\"defaultConfig\":{\"executorId\":\"DefaultCluster\",\"executorType\":\"MultiClusterQueryExecutor\",\"clusterConfig\":\"http_cluster\"}}]}";
      String exec_graph = tsdb.getConfig().getString("tsd.servlet.config.default_execution_graph");
      ExecutionGraph eg = JSON.parseToObject(exec_graph, ExecutionGraph.class);
      eg.initialize(tsdb, null).join(1);
      tsdb.getRegistry().registerExecutionGraph(eg, true);
      
      
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize default query "
          + "executor context", e);
    } 
  }
}
