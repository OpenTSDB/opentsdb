package net.opentsdb.storage.cassandra.functions;

import com.google.common.base.Function;

import java.util.Collection;
import javax.annotation.Nullable;

public class IsEmptyFunction implements Function<Collection, Boolean> {
  @Override
  public Boolean apply(@Nullable final Collection input) {
    return input.isEmpty();
  }
}
