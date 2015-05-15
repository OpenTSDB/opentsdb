package net.opentsdb.web;

import autovalue.shaded.com.google.common.common.collect.ImmutableMap;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import io.netty.handler.codec.http.cors.CorsConfig;
import net.opentsdb.core.DataPointsClient;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.web.resources.DatapointsResource;
import net.opentsdb.web.resources.MetricsResource;
import net.opentsdb.web.resources.NotFoundResource;
import net.opentsdb.web.resources.Resource;

import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A Dagger module that is capable of creating objects relevant to the HTTP
 * server.
 */
@Module(
    addsTo = TsdbModule.class,
    injects = {
        HttpServerInitializer.class
    })
public class HttpModule {
  @Provides @Singleton
  ObjectMapper provideObjectMapper() {
    return new ObjectMapper()
        .registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false))
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
        .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
        .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
  }

  @Provides @Singleton
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
    final List<String> corsHeaders = config.getStringList("tsd.http.request.cors_headers");
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
