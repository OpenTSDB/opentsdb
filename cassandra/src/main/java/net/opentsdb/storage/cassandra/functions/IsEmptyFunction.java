package net.opentsdb.storage.cassandra.functions;

import com.google.common.base.Function;

import java.util.Collection;

public class IsEmptyFunction implements Function<Collection, Boolean> {
  @Override
  public Boolean apply(final Collection input) {
    return input.isEmpty();
  }
}
