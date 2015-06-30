package net.opentsdb.web;

import net.opentsdb.core.ConfigModule;
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
import dagger.Module;
import dagger.Provides;
import io.netty.handler.codec.http.cors.CorsConfig;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;

/**
 * A Dagger module that is capable of creating objects relevant to the HTTP server.
 */
@Module(
    includes = {
        ConfigModule.class,
        CoreModule.class,
        PluginsModule.class,
        StoreModule.class
    })
public class HttpModule {
  @Provides
  @Singleton
  ObjectMapper provideObjectMapper(final StoreDescriptor storeDescriptor) {
    return new ObjectMapper()
        .registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false))
        .registerModule(new JacksonModule(storeDescriptor.labelIdSerializer(),
            storeDescriptor.labelIdDeserializer()))
        .registerModule(new GuavaModule())
        .enable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES)
        .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
        .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
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
