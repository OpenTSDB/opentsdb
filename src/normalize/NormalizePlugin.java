package net.opentsdb.normalize;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.PluginConfig;

public abstract class NormalizePlugin {

    private PluginConfig pluginConfig;

    public  NormalizePlugin(PluginConfig pluginConfig){
        this.pluginConfig = pluginConfig;
    };

    private NormalizePlugin() {}
    public abstract void initialize(final TSDB tsdb);
    public abstract Deferred<Object> shutdown();
    public abstract String version();
    public abstract void collectStats(final StatsCollector collector);


}
