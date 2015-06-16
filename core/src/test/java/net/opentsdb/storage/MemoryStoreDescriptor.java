package net.opentsdb.storage;

import net.opentsdb.uid.LabelId;

import com.codahale.metrics.MetricRegistry;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;

@AutoService(StoreDescriptor.class)
public class MemoryStoreDescriptor extends StoreDescriptor {
  @Override
  public TsdbStore createStore(final Config config, final MetricRegistry metrics) {
    return new MemoryStore();
  }

  @Nonnull
  @Override
  public LabelId.LabelIdSerializer<MemoryLabelId> labelIdSerializer() {
    return new MemoryLabelId.MemoryLabelIdSerializer();
  }

  @Nonnull
  @Override
  public LabelId.LabelIdDeserializer labelIdDeserializer() {
    return new MemoryLabelId.MemoryLabelIdDeserializer();
  }
}
