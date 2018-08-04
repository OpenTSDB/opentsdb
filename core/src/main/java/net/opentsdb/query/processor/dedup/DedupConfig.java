package net.opentsdb.query.processor.dedup;

import com.google.common.hash.HashCode;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;

public class DedupConfig extends BaseQueryNodeConfig {

    protected DedupConfig(Builder builder) {
        super(builder);
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public HashCode buildHashCode() {
        return null;
    }

    @Override
    public int compareTo(QueryNodeConfig o) {
        return 0;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends BaseQueryNodeConfig.Builder {

        @Override
        public DedupConfig build() {
            return new DedupConfig(this);
        }
    }
}
