package net.opentsdb.web.jackson;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.uid.LabelId;

import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * A reusable Jackson module that registers all custom serializers and mix-ins.
 */
public class JacksonModule extends SimpleModule {
  /**
   * Create a new instance that uses the provided {@link LabelId} specific serializers.
   */
  public JacksonModule(final LabelId.LabelIdSerializer idSerializer,
                       final LabelId.LabelIdDeserializer idDeserializer) {
    super("JsonModule");
    setMixInAnnotation(LabelMeta.class, LabelMetaMixIn.class);
    setMixInAnnotation(Annotation.class, AnnotationMixIn.class);

    addSerializer(new LabelIdJsonSerializer(idSerializer));
    addKeySerializer(LabelId.class, new LabelIdJsonKeySerializer(idSerializer));

    addDeserializer(LabelId.class, new LabelIdJsonDeserializer(idDeserializer));
    addKeyDeserializer(LabelId.class, new LabelIdJsonKeyDeserializer(idDeserializer));
  }
}
