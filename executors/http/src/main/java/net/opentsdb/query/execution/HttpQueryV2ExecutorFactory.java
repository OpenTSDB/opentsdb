package net.opentsdb.query.execution;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;

public class HttpQueryV2ExecutorFactory extends QueryExecutorFactory<IteratorGroups> {
  public static final TypeToken<?> token = TypeToken.of(IteratorGroups.class);
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    try {
      tsdb.getRegistry().registerFactory(this);
    } catch (Exception e) {
      Deferred.fromResult(e);
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public String id() {
    return "HttpQueryV2Executor";
  }

  @Override
  public TypeToken<?> type() {
    return token;
  }

  @Override
  public QueryExecutor<IteratorGroups> newExecutor(final ExecutionGraphNode node) {
    return new HttpQueryV2Executor(node);
  }

}
