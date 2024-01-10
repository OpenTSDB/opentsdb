package net.opentsdb.kubernetes.discovery;

public interface ServiceContext<A> {

    boolean match(A a);
}
