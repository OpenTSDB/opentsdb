package net.opentsdb.kubernetes.discovery;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.discovery.ServiceRegistry;
import net.opentsdb.kubernetes.discovery.impl.ShardedTSDBContext;
import net.opentsdb.kubernetes.discovery.impl.ShardedTSDBMatcher;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * A simple Kubernetes Service API, which lists endpoints that can collectively satifsy a query.
 * Can be used for both sharded and non-sharded discovery.
 * Assumes that there a metric k8s_namespace.
 *
 * Assume the config follows the same format as kubeconfig
 *
 * TODO: Class is doing too much.
 * TODO: Class is very closely coupled with stateful set
 */
public class BaseKubernetesRegistry extends BaseTSDBPlugin implements ServiceRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(BaseKubernetesRegistry.class);
    private static final String TYPE = "KubernetesRegistry";
    private static final String KEY_PREFIX = "kubernetes.registry";
    private static final String KUBE_CONFIG = "kubeconfig";
    private static final String KUBE_NAMESPACE = "kube-namespace";
    private static final String HTTP_TIMEOUT = "http-timeout";
    private static final String CACHE_TIMEOUT = "cache-timeout";
    private static final String DOMAIN = "k8s-domain";
    private static final String PORT_NAME = "port-name";
    private static final String PROTOCOL = "service-protocol";
    //This is the label on the stateful set that gives the epoch.
    private static final String TIME_LABEL = "time-label";
    private static final String NAMESPACE_LABEL = "namespace-label";
    /**
     * How frequently the end point list is refreshed.
     */
    private static final String REFRESH_PERIOD = "refresh.period";

    private String configured_kube_namespace = null;
    private int httpTimeoutInSeconds;
    private int cacheTimeoutInSeconds;
    private ApiClient client;
    private Map<String, Set<String>> cache = new HashMap<>();
    private long lastFetchInSeconds = 0;
    //TODO: Pluginize this.
    private ServiceMatcher<V1StatefulSet> serviceMatcher = new ShardedTSDBMatcher();
    private String portName;
    private String domain;
    private String protocol;
    private String format = "%s://%s.%s:%";
    private String timeLabel;
    private String namespaceLabel;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Deferred<Object> initialize(TSDB tsdb, final String id) {

        this.tsdb = tsdb;
        this.id = id;

        final String final_config_key
                = getConfigKey(KUBE_CONFIG);

        if (!tsdb.getConfig().hasProperty(final_config_key)) {
            tsdb.getConfig().register(final_config_key, null, true,
                    "The truststore for Athenz.");
        }

        final String kubeConfigStr = tsdb.getConfig().getString(final_config_key);

        try {
            this.client = Config.fromConfig(
                    new ByteArrayInputStream(
                            kubeConfigStr.getBytes(StandardCharsets.UTF_8)));
        } catch (IOException e) {
            return Deferred.fromError(e);
        }

        this.configured_kube_namespace = tsdb.getConfig().getString(getConfigKey(KUBE_NAMESPACE));
        this.httpTimeoutInSeconds = getInt(tsdb.getConfig().getString(getConfigKey(HTTP_TIMEOUT)), 30);
        this.cacheTimeoutInSeconds = getInt(tsdb.getConfig().getString(getConfigKey(CACHE_TIMEOUT)), 120);
        this.portName = get(tsdb.getConfig().getString(getConfigKey(PORT_NAME)), "https");
        this.domain = get(tsdb.getConfig().getString(getConfigKey(DOMAIN)), "svc.cluster.local");
        this.protocol = get(tsdb.getConfig().getString(getConfigKey(PROTOCOL)), "https");

        //TODO: Move these to ShardedTSDBContext
        this.timeLabel = get(tsdb.getConfig().getString(getConfigKey(TIME_LABEL)), "epoch");
        this.namespaceLabel = get(tsdb.getConfig().getString(getConfigKey(NAMESPACE_LABEL)), "namespace");
        //There should be a better way to do this in the context of TSDB.

        return Deferred.fromResult(null);
    }

    private String get(String config, String https) {
        return config != null ? config : https;
    }

    private int getInt(String string, int i) {
        if (!Strings.isNullOrEmpty(string)) {
            return Integer.parseInt(string);
        } else {
            return i;
        }
    }

    /**
     * A few things need to happen here:
     *
     * 1. Convert the metric to a k8s_namespace.
     * 2. Convert the time and other context variables into a service name.
     *
     * TODO: This method is too cumbersome.
     * TODO: Rely on endpoints and label selectors instead of getting into deeper k8s APIs.
     * @param pipelineContext
     * @param timeSeriesDataSourceConfig
     * @return
     */
    @Override
    public Set<String> getEndpoints(QueryPipelineContext pipelineContext,
                                  TimeSeriesDataSourceConfig timeSeriesDataSourceConfig) {

        final String data_namespace = timeSeriesDataSourceConfig.getNamespace();
        AppsV1Api appsV1Api = new AppsV1Api(client);
        CoreV1Api coreV1Api = new CoreV1Api(client);
        long currTimeInSeconds = System.currentTimeMillis() / 1000;

        //To avoid aggressive calls on negative results.
        if (currTimeInSeconds - lastFetchInSeconds < cacheTimeoutInSeconds) {
            if (cache.containsKey(data_namespace)) {
                return cache.get(data_namespace);
            }
        }

        final String k8s_namespace;
        if ( configured_kube_namespace == null) {
            k8s_namespace = data_namespace;
        } else {
            k8s_namespace = configured_kube_namespace;
        }

        List<String> endpoints = new ArrayList<>();

        String _continue = null;
        boolean finish = false;
        try {
            ServiceContext<V1StatefulSet> context =
                    new ShardedTSDBContext(timeLabel, namespaceLabel, pipelineContext, timeSeriesDataSourceConfig);
            while (!finish) {
                V1StatefulSetList v1StatefulSetList = appsV1Api.listNamespacedStatefulSet(
                        k8s_namespace,
                        null,
                        false,
                        null,
                        null,
                        null,
                        100,
                        null,
                        null,
                        httpTimeoutInSeconds,
                        false);

                if(v1StatefulSetList == null) {
                    //Should never happen
                    LOG.error("K8s stateful set fetch failed with null for" +
                            " ns: {} ", k8s_namespace);
                    break;
                }
                List<V1StatefulSet> items = v1StatefulSetList.getItems();

                if(items == null || items.size() ==0) {
                    LOG.error("0 K8s stateful sets fetched for" +
                            " ns: {} ", k8s_namespace);
                    this.lastFetchInSeconds = System.currentTimeMillis() / 1000;
                    break;
                }

                V1ListMeta metadata = v1StatefulSetList.getMetadata();
                _continue = metadata != null ? metadata.getContinue() : null;

                if (_continue == null) {
                    finish = true;
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Succefully fetched {} K8s stateful sets for" +
                                " ns: {}", items.size(), k8s_namespace);
                    }
                } else {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Succefully fetched {} K8s stateful sets for" +
                                " ns: {}" +
                                " remaining: {}",
                                items.size(),
                                k8s_namespace,
                                metadata.getRemainingItemCount());
                    }
                }

                for (V1StatefulSet statefulSet : items) {
                    if(statefulSet.getSpec() == null || statefulSet.getMetadata() == null) {
                        continue;
                    }
                    if(serviceMatcher.match(statefulSet, context)) {
                        V1LabelSelector selector = statefulSet
                                .getSpec()
                                .getSelector();
                        String selectorStr = selector.toString();
                        V1ServiceList v1ServiceList = coreV1Api.listNamespacedService(
                                k8s_namespace,
                                "false",
                                false,
                                null,
                                null,
                                selectorStr,
                                100,
                                null,
                                null,
                                httpTimeoutInSeconds,
                                false);
                        List<V1Service> services = v1ServiceList.getItems();
                        if(services == null || services.size() == 0) {
                            LOG.error("K8s service with label {} is not present or is empty" +
                                    " k8s ns: {} data ns: {}", selectorStr,
                                    k8s_namespace, data_namespace);
                        } else {
                            if(LOG.isDebugEnabled()) {
                                LOG.debug("Succefully fetched {} K8s services for" +
                                                " selector: {}" +
                                                " ns: {}" +
                                                " remaining: {}",
                                        services.size(),
                                        selectorStr,
                                        k8s_namespace,
                                        metadata.getRemainingItemCount());

                                String name = statefulSet.getMetadata().getName();
                                Integer replicas = statefulSet
                                        .getSpec()
                                        .getReplicas();
                                // Should have only one result
                                V1Service v1Service = services.get(0);
                                Optional<V1ServicePort> any = v1Service.getSpec().getPorts()
                                        .stream()
                                        .filter(v1ServicePort ->
                                                v1ServicePort.getName().equalsIgnoreCase(portName))
                                        .findAny();
                                if(any.isPresent()) {
                                    // This follows
                                    // https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#stable-network-id
                                    for (int i = 0; i < replicas ; i++) {
                                        // TODO: Try to us a standard format
                                        String endPoint =
                                                String.format(
                                                        format,
                                                        protocol,
                                                        name + "-" + i,
                                                        domain,
                                                        any);
                                        endpoints.add(endPoint);
                                    }
                                }
                            }

                        }

                    }
                }
            }
        } catch (ApiException e) {
            //Weird that response codes are only given on failures.
            LOG.error("K8s stateful set fetch failed for" +
                    " ns: {} " +
                    "code: {} " +
                    "error: {}" +
                    "headers {}",
                    k8s_namespace,
                    e.getCode(),
                    e.getResponseBody(),
                    e.getResponseHeaders());
            
        } catch (Exception e) {
            LOG.error("Error when fetching API info from kubernetes", e);
        }

        return null;
    }
    protected String getConfigKey(final String key) {
        return KEY_PREFIX + (Strings.isNullOrEmpty(id) ? "" : id + ".") + key;
    }
}
