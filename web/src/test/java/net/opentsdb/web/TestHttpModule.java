package net.opentsdb.web;

import net.opentsdb.web.jackson.AnnotationMixInTest;
import net.opentsdb.web.jackson.LabelMetaMixInTest;

import dagger.Module;

@Module(
    addsTo = HttpModule.class,
    injects = {
        AnnotationMixInTest.class,
        LabelMetaMixInTest.class
    })
public class TestHttpModule {
}
