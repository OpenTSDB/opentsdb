package net.opentsdb.storage.cassandra;

/**
 * Constants used by Cassandra
 */
public class CassandraConst {
    /**
     * The default Cassandra Port used by the cassandra by default if the
     * port was not specified in the configuration file.
     */
    public static final int DEFAULT_CASSANDRA_PORT = 9042;
    public static final long CASSANDRA_TIMEOUT = 50;
}
