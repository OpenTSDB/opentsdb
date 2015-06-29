
package net.opentsdb.plugins;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.AnnotationFixtures;
import net.opentsdb.uid.TimeseriesId;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

public abstract class RealTimePublisherTest {
  protected RealTimePublisher publisher;

  @Test
  public void sinkLongDataPoint() {
    assertNotNull(publisher.publishDataPoint("sys.cpu.user", 123123123, 123,
        ImmutableMap.of("host", "east"), mock(TimeseriesId.class)));
  }

  @Test
  public void sinkDoubleDataPoint() {
    assertNotNull(publisher.publishDataPoint("sys.cpu.user", 123123123, 12.5,
        ImmutableMap.of("host", "east"), mock(TimeseriesId.class)));
  }

  @Test
  public void publishAnnotation() {
    final Annotation ann = AnnotationFixtures.provideAnnotation();
    assertNotNull(publisher.publishAnnotation(ann));
  }
}
