package net.opentsdb.storage;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;

public interface TimeSeriesDataStoreFactory extends TSDBPlugin {

  TimeSeriesDataStore newInstance(final TSDB tsdb, final String id);
  
}
