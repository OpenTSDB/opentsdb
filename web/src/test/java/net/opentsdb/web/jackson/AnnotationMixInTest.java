package net.opentsdb.web.jackson;

import static org.junit.Assert.assertEquals;

import net.opentsdb.meta.Annotation;
import net.opentsdb.web.HttpModule;
import net.opentsdb.web.TestHttpModule;

import com.fasterxml.jackson.databind.ObjectMapper;
import dagger.ObjectGraph;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

public class AnnotationMixInTest {
  @Inject ObjectMapper jsonMapper;

  private Annotation annotation;
  private String annotationJson;

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new TestHttpModule(), new HttpModule()).inject(this);

    annotation = Annotation.create("000001000001000001", 1328140800, 1328140801, "Description");

    annotationJson = "{\"timeSeriesId\":\"000001000001000001\",\"startTime\":1328140800,"
                     + "\"endTime\":1328140801,\"message\":\"Description\",\"properties\":{}}";
  }

  @Test
  public void serializeMatchesExactly() throws Exception {
    final String json = new String(jsonMapper.writeValueAsBytes(annotation));
    assertEquals(annotationJson, json);
  }

  @Test
  public void deserialize() throws Exception {
    Annotation parsedAnnotation = jsonMapper.reader(Annotation.class)
        .readValue(annotationJson);
    assertEquals(annotation, parsedAnnotation);
  }

  @Test
  public void deserializeIgnoresUnknown() throws Exception {
    final String jsonWithUnknown = "{\"timeSeriesId\":\"000001000001000001\"," +
                                   "\"startTime\":1328140800,\"endTime\":1328140801,\"unknown\":1328140801," +
                                   "\"message\":\"Description\",\"notes\":\"Notes\",\"properties\":{}}";
    final Annotation parsedAnnotation = jsonMapper.reader(Annotation.class)
        .readValue(jsonWithUnknown);
    assertEquals(annotation, parsedAnnotation);
  }
}
