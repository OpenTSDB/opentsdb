package net.opentsdb.web.jackson;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.uid.LabelId;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class JacksonModule extends SimpleModule {
  private final LabelId.LabelIdSerializer idSerializer;
  private final LabelId.LabelIdDeserializer idDeserializer;

  public JacksonModule(final LabelId.LabelIdSerializer idSerializer,
                       final LabelId.LabelIdDeserializer idDeserializer) {
    super("JsonModule");
    this.idSerializer = idSerializer;
    this.idDeserializer = idDeserializer;
  }

  @Override
  public void setupModule(SetupContext context) {
    context.setMixInAnnotations(LabelMeta.class, LabelMetaMixIn.class);
    context.setMixInAnnotations(Annotation.class, AnnotationMixIn.class);

    context.addSerializers(new SimpleSerializers(
        ImmutableList.<JsonSerializer<?>>of(new LabelIdJsonSerializer(idSerializer))));

    context.addDeserializers(new SimpleDeserializers(
        ImmutableMap.<Class<?>, JsonDeserializer<?>>of(
            LabelId.class, new LabelIdJsonDeserializer(idDeserializer))));
  }
}
