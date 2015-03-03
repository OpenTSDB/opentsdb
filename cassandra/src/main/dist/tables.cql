
// Pipe me into cqlsh to let me add this for you.

DROP KEYSPACE IF EXISTS "tsdb";
DROP KEYSPACE IF EXISTS "tsdbunique";

// We use SimpleStrategy and a low replication factor here during development
// but for any production deployment you probably want to change these to fit
// your requirements.
CREATE KEYSPACE tsdb WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE KEYSPACE tsdbunique WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

//
// Identifier tables
//
// Maps (identifier, type) into a name
CREATE TABLE tsdb.id_to_name (
  id bigint,
  type varchar,
  name varchar,
  PRIMARY KEY (id, type)
);

// Maps (name, type) into an identifier
CREATE TABLE tsdb.name_to_id (
  name varchar,
  type varchar,
  id bigint,
  PRIMARY KEY (name, type)
);

// A table that keeps track on the max identifier used for the different
// identifier types so that we easily can figure out what id to try next.
CREATE TABLE tsdb.max_id (
	type varchar,
	max counter,
	PRIMARY KEY (type)
);

// A lock for (id, type) and (name, type) so we can use lightweight transactions
// and avoid certain race conditions when we create IDs.
CREATE TABLE tsdbunique.id_name_lock (
  key varchar,
  type varchar,
  PRIMARY KEY (type, key)
);
//
// End identifier tables
//

// The table that contains all data points.
CREATE TABLE tsdb.datapoints (
  tsid varchar,
  basetime bigint,
  timestamp bigint,
  flags int,
  val varchar,
  PRIMARY KEY ((tsid, basetime), timestamp)
);

// An index for resolving which timeseries that uses an identifier.
CREATE TABLE tsdb.ts_inverted_index (
	id bigint,
	type varchar,
	tsid varchar,
	PRIMARY KEY ((id, type), tsid)
);
