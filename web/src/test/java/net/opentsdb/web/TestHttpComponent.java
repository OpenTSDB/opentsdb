package net.opentsdb.web;

import net.opentsdb.web.jackson.AnnotationMixInTest;
import net.opentsdb.web.jackson.LabelMetaMixInTest;

import dagger.Component;

import javax.inject.Singleton;

@Component(modules = HttpModule.class)
@Singleton
public interface TestHttpComponent {
  void inject(AnnotationMixInTest annotationMixInTest);

  void inject(LabelMetaMixInTest labelMetaMixInTest);
}
