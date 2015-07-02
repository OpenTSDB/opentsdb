package net.opentsdb.search;

import com.google.auto.service.AutoService;
import com.typesafe.config.Config;

@AutoService(SearchPluginDescriptor.class)
public class DefaultSearchPluginDescriptor extends SearchPluginDescriptor {
  @Override
  public SearchPlugin create(final Config config) throws Exception {
    return new DefaultSearchPlugin();
  }
}
