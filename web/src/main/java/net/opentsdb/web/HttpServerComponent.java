package net.opentsdb.web;

import com.typesafe.config.Config;
import dagger.Component;

import javax.inject.Singleton;

@Component(modules = HttpModule.class)
@Singleton
interface HttpServerComponent {
  Config config();

  HttpServerInitializer httpServerInitializer();
}
