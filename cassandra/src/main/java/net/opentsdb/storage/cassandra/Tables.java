package net.opentsdb.storage.cassandra;

/**
 * Constants that describe which tables we use
 */
class Tables {
  static final String KEYSPACE = "tsdb";

  static final String DATAPOINTS = "datapoints";
  static final String TS_INVERTED_INDEX = "ts_inverted_index";

  static final String ID_TO_NAME   = "id_to_name";
  static final String NAME_TO_ID   = "name_to_id";

  private Tables() {}
}
