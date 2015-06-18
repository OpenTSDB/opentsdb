package net.opentsdb.web.jackson;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.uid.LabelId;

import com.fasterxml.jackson.databind.module.SimpleModule;


public class JacksonModule extends SimpleModule {


  public JacksonModule(final LabelId.LabelIdSerializer idSerializer,
                       final LabelId.LabelIdDeserializer idDeserializer) {
    super("JsonModule");
      setMixInAnnotation(LabelMeta.class, LabelMetaMixIn.class);
      setMixInAnnotation(Annotation.class, AnnotationMixIn.class);

      addSerializer(new LabelIdJsonSerializer(idSerializer));
      addDeserializer(LabelId.class, new LabelIdJsonDeserializer(idDeserializer));
  }
}
