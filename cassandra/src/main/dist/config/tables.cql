
// Pipe me into cqlsh to let me add this for you.

DROP KEYSPACE IF EXISTS "tsdb";

// We use SimpleStrategy and a low replication factor here during development
// but for a production deployment you probably want to change these to fit
// your requirements.
CREATE KEYSPACE tsdb WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

//
// Identifier tables
//
// Maps (identifier, type) into a name. We use a timestamp that indicates when
// the id was created to resolve conflicts. The timestamp should be the same
// as in the table name_to_id. The numeric ids are useful to avoid ID collisions
// for timeseries_ids (timeseries_ids are a hash of the the label_ids in this
// table). The type column is one of metric, tagk or tagv.
CREATE TABLE tsdb.id_to_name (
  label_id bigint,
  type varchar,
  creation_time timestamp,
  name varchar,
  PRIMARY KEY ((label_id, type), creation_time)
) WITH CLUSTERING ORDER BY (creation_time DESC);

// Maps (name, type) into a label_id. We use a timestamp that indicates when
// the id was created to resolve conflicts. The timestamp should be the same
// as in the table name_to_id.
CREATE TABLE tsdb.name_to_id (
  name varchar,
  type varchar,
  creation_time timestamp,
  label_id bigint,
  PRIMARY KEY ((name, type), creation_time)
) WITH CLUSTERING ORDER BY (creation_time DESC);
//
// End identifier tables
//

// An index for resolving which timeseries that uses a label_id.
CREATE TABLE tsdb.ts_inverted_index (
  label_id bigint,
  type varchar,
  timeseries_id uuid,
  PRIMARY KEY ((label_id, type), timeseries_id)
);

// An index for resolving which label_ids a timeseries is composed of. The
// metric and tags are label_ids.
CREATE TABLE tsdb.ts_index (
  timeseries_id uuid,
  metric bigint,
  tags map<bigint, bigint>,
  PRIMARY KEY (timeseries_id)
);

// The table that contains all data points.
CREATE TABLE tsdb.datapoints (
  timeseries_id uuid,
  basetime bigint,
  timestamp bigint,
  float_value float,
  double_value double,
  long_value bigint,
  PRIMARY KEY ((timeseries_id, basetime), timestamp)
);
