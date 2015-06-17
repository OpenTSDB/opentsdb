package net.opentsdb.storage.cassandra.functions;

import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;

import javax.annotation.Nullable;

public class ResultSetToVoid implements Function<ResultSet, Void> {
  @Nullable
  @Override
  public Void apply(final ResultSet rows) {
    return null;
  }
}
