package net.opentsdb.storage;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

public abstract class TimeSeriesDataStoreFactory extends BaseTSDBPlugin {

  public abstract TimeSeriesDataStore newInstance(final TSDB tsdb, final String id);
}
