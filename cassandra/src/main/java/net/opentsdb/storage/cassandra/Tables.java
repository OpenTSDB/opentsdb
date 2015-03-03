package net.opentsdb.storage.cassandra;

/**
 * Constants that describe which tables we use
 */
class Tables {
  private Tables() {}

  static final String DATAPOINTS = "datapoints";
  static final String TS_INVERTED_INDEX = "ts_inverted_index";

  static final String ID_TO_NAME   = "id_to_name";
  static final String NAME_TO_ID   = "name_to_id";
  static final String MAX_ID       = "max_id";
  static final String ID_NAME_LOCK = "id_name_lock";
}
