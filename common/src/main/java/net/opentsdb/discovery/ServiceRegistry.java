package net.opentsdb.discovery;

import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;

import java.util.Set;

public interface ServiceRegistry {
    Set<String> getEndpoints(QueryPipelineContext pipelineContext,
                             TimeSeriesDataSourceConfig timeSeriesDataSourceConfig);
}
