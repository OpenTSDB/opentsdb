package net.opentsdb.core;


import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.hbase.HBaseStore;
import net.opentsdb.utils.Config;
import org.hbase.async.HBaseClient;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Use this class so create a TsdbStore instance. Given a config file this
 * class will set up an instance of the configured type of the database store.
 */
public class StoreSupplier implements Supplier<TsdbStore> {
    /**
     *  The static string that should be used in the config for an HBase client.
     */
    private static final String HBASE_NAME = "HBase".toLowerCase();

    /**
     *  The static string that should be used in the config for a Cassandra
     *  client.
     */
    private static final String CASSANDRA_NAME = "Cassandra".toLowerCase();

    /**
     * The config object to be used when we want to create a
     * {@link TsdbStore} object.
     */
    private final Config config;

    /**
     * Instantiates a supplier for further use.
     *
     * @param config The configuration object used when generating a
     *               TsdbStore object
     */
    public StoreSupplier(final Config config) {
        this.config  = checkNotNull(config);
    }

    /**
     * Use this method when you need to get a {@link TsdbStore} object that is
     * not a {@link net.opentsdb.storage.MemoryStore} object used for testing.
     *
     * @return This method will return a ready to use {@link TsdbStore} object.
     * No guarantee is made that it will connect properly to the database but
     * it will be configured according to the config class sent in when this
     * object was created.
     */
    @Override
    public TsdbStore get() {
        String adapter_type = config.getString("tsd.storage.adapter");
        if (Strings.isNullOrEmpty(adapter_type)) {
            throw new IllegalArgumentException("The config could not find the" +
                    " field 'tsd.storage.adapter', please make sure it was " +
                    "configured correctly.");
        }

        if (HBASE_NAME.equals(adapter_type.toLowerCase()))
            return getHBaseStore();

        if (CASSANDRA_NAME.equals(adapter_type.toLowerCase()))
            return getCassandraStore();

        throw new IllegalArgumentException("The config could not find a valid" +
                " value for the field 'tsd.storage.adapter', please make sure" +
                " it was configured correctly. It was '"+  adapter_type + "'.");
    }

    /**
     * Use this method when you want to create a new HBaseStore object.
     *
     * @return Returns a HBaseStore object ready for use.
     */
    private HBaseStore getHBaseStore() {
        return new HBaseStore(
                new HBaseClient(
                        config.getString("tsd.storage.hbase.zk_quorum"),
                        config.getString("tsd.storage.hbase.zk_basedir")),
                config);
    }

    /**
     * Return null for now.
     *
     * @return null.
     */
    private TsdbStore getCassandraStore() {
        return null;
    }

    /**
     * Only for debug at the moment.
     *
     * @return Returns a string showing what kind of TsdbStore this supplier
     * will return when one call {@link StoreSupplier#get()}.
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("config.adapter", config.getString("tsd.storage.adapter"))
                .toString();
    }
}
