package net.opentsdb.kubernetes.discovery;

import io.kubernetes.client.openapi.models.V1StatefulSet;

public interface ServiceMatcher<A> {
    boolean match(V1StatefulSet statefulSet, ServiceContext<A> context);
}
