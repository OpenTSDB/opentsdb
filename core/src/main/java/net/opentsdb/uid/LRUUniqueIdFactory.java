package net.opentsdb.uid;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

public class LRUUniqueIdFactory extends BaseTSDBPlugin implements UniqueIdFactory {

  @Override
  public UniqueId newInstance(TSDB tsdb, String id, UniqueIdType type,
      UniqueIdStore store) {
    // TODO Auto-generated method stub
    return new LRUUniqueId(tsdb, id, type, store);
  }

  @Override
  public String id() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String version() {
    // TODO Auto-generated method stub
    return null;
  }

}
