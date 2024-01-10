package net.opentsdb.kubernetes.discovery.impl;

import io.kubernetes.client.openapi.models.V1StatefulSet;
import net.opentsdb.kubernetes.discovery.ServiceContext;
import net.opentsdb.kubernetes.discovery.ServiceMatcher;

public class ShardedTSDBMatcher implements ServiceMatcher<V1StatefulSet> {
    @Override
    public boolean match(V1StatefulSet statefulSet, ServiceContext<V1StatefulSet> context) {
        return context.match(statefulSet);
    }
}
