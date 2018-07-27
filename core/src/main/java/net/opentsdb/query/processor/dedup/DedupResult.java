package net.opentsdb.query.processor.dedup;

import com.google.common.reflect.TypeToken;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.rollup.RollupConfig;

public class DedupResult implements QueryResult {

    private QueryResult next;
    private List<TimeSeries> results;

    public DedupResult(QueryResult next) {
        this.next = next;
    }

    @Override
    public TimeSpecification timeSpecification() {
        return next.timeSpecification();
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
        return results;
    }

    @Override
    public long sequenceId() {
        return 0;
    }

    @Override
    public QueryNode source() {
        return null;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
        return null;
    }

    @Override
    public ChronoUnit resolution() {
        return null;
    }

    @Override
    public RollupConfig rollupConfig() {
        return null;
    }

    @Override
    public void close() {

    }
}
