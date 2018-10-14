package net.opentsdb.query.plan;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.stats.Span;

public interface QueryPlanner {

  public Deferred<Void> plan(final Span span);
  
  public void replace(final QueryNodeConfig old_config,
      final QueryNodeConfig new_config);
  
  public DirectedAcyclicGraph<QueryNode, DefaultEdge> graph();
  
  public DirectedAcyclicGraph<QueryNodeConfig, DefaultEdge> configGraph();
  
}
