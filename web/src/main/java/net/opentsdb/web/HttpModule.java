package net.opentsdb.web;

import net.opentsdb.core.CoreModule;
import net.opentsdb.core.DataPointsClient;
import net.opentsdb.plugins.PluginsModule;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.storage.StoreModule;
import net.opentsdb.web.jackson.JacksonModule;
import net.opentsdb.web.resources.DatapointsResource;
import net.opentsdb.web.resources.MetricsResource;
import net.opentsdb.web.resources.NotFoundResource;
import net.opentsdb.web.resources.Resource;

import autovalue.shaded.com.google.common.common.collect.ImmutableMap;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import dagger.Module;
import dagger.Provides;
import io.netty.handler.codec.http.cors.CorsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;

/**
 * A Dagger module that is capable of creating objects relevant to the HTTP server.
 */
@Module(
    includes = {
        CoreModule.class,
        PluginsModule.class,
        StoreModule.class
    },
    injects = {
        HttpServerInitializer.class
    })
public class HttpModule {
  private static final Logger LOG = LoggerFactory.getLogger(HttpModule.class);

  private final Config config;

  public HttpModule(final File configFile) {
    this(ConfigFactory.parseFileAnySyntax(configFile,
        ConfigParseOptions.defaults().setAllowMissing(false)));
  }

  public HttpModule() {
    this(ConfigFactory.load(ConfigParseOptions.defaults().setAllowMissing(false)));
  }

  private HttpModule(final Config config) {
    this.config = config.withFallback(
        ConfigFactory.parseResourcesAnySyntax("reference"));
    LOG.info("Loaded config from {}", config.origin());
  }

  @Provides
  @Singleton
  Config provideConfig() {
    return config;
  }

  @Provides
  @Singleton
  ObjectMapper provideObjectMapper(final StoreDescriptor storeDescriptor) {
    return new ObjectMapper()
        .registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false))
        .registerModule(new JacksonModule(storeDescriptor.labelIdSerializer(),
            storeDescriptor.labelIdDeserializer()))
        .registerModule(new GuavaModule())
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
        .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
        .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
  }

  @Provides
  @Singleton
  HttpServerInitializer provideHttpServerInitializer(final Config config,
                                                     final DataPointsClient dataPointsClient,
                                                     final ObjectMapper objectMapper,
                                                     final MetricRegistry metricRegistry) {
    final Resource datapointsResource = new DatapointsResource(dataPointsClient, objectMapper);
    final Resource metricResource = new MetricsResource(objectMapper, metricRegistry);

    final ImmutableMap<String, Resource> resources = ImmutableMap.of(
        "datapoints", datapointsResource,
        "admin/metrics", metricResource);
    final Resource defaultResource = new NotFoundResource();

    // http://lmgtfy.com/?q=facepalm
    final List<String> corsHeaders = config.getStringList("tsdb.web.cors.request.headers");
    final String[] corsHeadersArray = corsHeaders.toArray(new String[corsHeaders.size()]);

    final List<String> corsDomains = config.getStringList("tsdb.web.cors_domains");
    final String[] corsDomainsArray = corsDomains.toArray(new String[corsDomains.size()]);

    final CorsConfig corsConfig = CorsConfig
        .withOrigins(corsDomainsArray)
        .allowedRequestHeaders(corsHeadersArray)
        .build();

    final int maxContentLength = config.getInt("tsdb.web.max_content_length");

    return new HttpServerInitializer(resources, defaultResource, corsConfig, maxContentLength);
  }
}
