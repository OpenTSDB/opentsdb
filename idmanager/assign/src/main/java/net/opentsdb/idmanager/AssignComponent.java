package net.opentsdb.idmanager;

import net.opentsdb.core.CoreModule;
import net.opentsdb.plugins.PluginsModule;
import net.opentsdb.storage.StoreModule;
import net.opentsdb.storage.TsdbStore;

import dagger.Component;

import javax.inject.Singleton;

@Component(
    modules = {
        CoreModule.class,
        PluginsModule.class,
        StoreModule.class
    })
@Singleton
public interface AssignComponent {
  Assign assign();

  TsdbStore store();
}
