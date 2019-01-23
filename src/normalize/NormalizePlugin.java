package net.opentsdb.normalize;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

public abstract class NormalizePlugin {


    public abstract void initialize(final TSDB tsdb);

    public abstract Deferred<Object> shutdown();

    public abstract String version();

    public abstract void collectStats(final StatsCollector collector);


}
