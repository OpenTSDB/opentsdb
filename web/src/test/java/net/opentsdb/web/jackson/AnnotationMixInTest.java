package net.opentsdb.web.jackson;

import static org.junit.Assert.assertEquals;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.AnnotationFixtures;
import net.opentsdb.web.DaggerTestHttpComponent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

public class AnnotationMixInTest {
  @Inject ObjectMapper jsonMapper;

  private Annotation annotation;
  private String annotationJson;

  @Before
  public void setUp() throws Exception {
    DaggerTestHttpComponent.create().inject(this);

    annotation = AnnotationFixtures.provideAnnotation();

    annotationJson = "{\"metric\":\"2d11dffc-a8c6-4830-8f4e-aa3417c4f9dc\","
                     + "\"tags\":{\"edf3c0cc-9ebb-404e-b148-8cf140e13a1e\":"
                     + "\"c4aa3396-4f0e-4def-988a-513191279cb3\"},\"startTime\":10,\"endTime\":12,"
                     + "\"message\":\"My message\",\"properties\":{}}";
  }

  @Test
  public void deserialize() throws Exception {
    Annotation parsedAnnotation = jsonMapper.reader(Annotation.class)
        .readValue(annotationJson);
    assertEquals(annotation, parsedAnnotation);
  }

  @Test(expected = UnrecognizedPropertyException.class)
  public void deserializeFailsOnUnknown() throws Exception {
    final String jsonWithUnknown = "{\"metric\":\"2d11dffc-a8c6-4830-8f4e-aa3417c4f9dc\","
                                   + "\"tags\":{\"edf3c0cc-9ebb-404e-b148-8cf140e13a1e\":"
                                   + "\"c4aa3396-4f0e-4def-988a-513191279cb3\"},\"startTime\":10,"
                                   + "\"endTime\":12,\"message\":\"My message\",\"properties\":{},"
                                   + "\"unknown\":null}";
    jsonMapper.reader(Annotation.class)
        .readValue(jsonWithUnknown);
  }

  @Test
  public void serializeMatchesExactly() throws Exception {
    final String json = new String(jsonMapper.writeValueAsBytes(annotation));
    assertEquals(annotationJson, json);
  }
}
