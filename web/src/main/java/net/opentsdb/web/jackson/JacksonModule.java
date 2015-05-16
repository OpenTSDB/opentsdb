package net.opentsdb.web.jackson;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.meta.TSMeta;

import com.fasterxml.jackson.databind.module.SimpleModule;

public class JacksonModule extends SimpleModule {
  public JacksonModule() {
    super("StorageModule");
  }

  @Override
  public void setupModule(SetupContext context) {
    context.setMixInAnnotations(LabelMeta.class, LabelMetaMixIn.class);
    context.setMixInAnnotations(Annotation.class, AnnotationMixIn.class);
    context.setMixInAnnotations(TSMeta.class, TSMetaMixIn.class);
  }
}
