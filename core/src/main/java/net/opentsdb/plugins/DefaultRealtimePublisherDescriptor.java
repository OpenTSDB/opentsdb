package net.opentsdb.plugins;

import com.google.auto.service.AutoService;
import com.typesafe.config.Config;

@AutoService(RealTimePublisherDescriptor.class)
public class DefaultRealtimePublisherDescriptor extends RealTimePublisherDescriptor {

  @Override
  public RealTimePublisher create(final Config config) throws Exception {
    return new DefaultRealtimePublisher();
  }
}
