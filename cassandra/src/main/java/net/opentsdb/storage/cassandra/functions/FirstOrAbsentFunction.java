package net.opentsdb.storage.cassandra.functions;


import com.google.common.base.Function;
import com.google.common.base.Optional;

import java.util.List;
import javax.annotation.Nullable;

public class FirstOrAbsentFunction<V> implements Function<List<V>, Optional<V>> {
  @Override
  public Optional<V> apply(@Nullable final List<V> input) {
    // No point in checking for null since the method call will throw an NPE anyway
    if (input.isEmpty()) {
      return Optional.absent();
    } else {
      return Optional.of(input.get(0));
    }
  }
}
