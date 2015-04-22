package net.opentsdb.storage.json;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;

import com.fasterxml.jackson.databind.module.SimpleModule;

public class StorageModule extends SimpleModule {
  public StorageModule() {
    super("StorageModule");
  }

  @Override
  public void setupModule(SetupContext context) {
    context.setMixInAnnotations(UIDMeta.class, UIDMetaMixIn.class);
    context.setMixInAnnotations(Annotation.class, AnnotationMixIn.class);
    context.setMixInAnnotations(TSMeta.class, TSMetaMixIn.class);
  }
}
