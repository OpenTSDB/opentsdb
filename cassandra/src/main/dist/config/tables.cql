
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
// as in the table name_to_id.
CREATE TABLE tsdb.id_to_name (
  id bigint,
  type varchar,
  ctim timestamp,
  name varchar,
  PRIMARY KEY ((id, type), ctim)
);

// Maps (name, type) into an identifier. We use a timestamp that indicates when
// the id was created to resolve conflicts. The timestamp should be the same
// as in the table name_to_id.
CREATE TABLE tsdb.name_to_id (
  name varchar,
  type varchar,
  ctim timestamp,
  id bigint,
  PRIMARY KEY ((name, type), ctim)
);
//
// End identifier tables
//

// The table that contains all data points.
CREATE TABLE tsdb.datapoints (
  tsid uuid,
  basetime bigint,
  timestamp bigint,
  fval float,
  dval double,
  lval bigint,
  PRIMARY KEY ((tsid, basetime), timestamp)
);

// An index for resolving which timeseries that uses an identifier.
CREATE TABLE tsdb.ts_inverted_index (
	id bigint,
	type varchar,
	tsid uuid,
	PRIMARY KEY ((id, type), tsid)
);

// An index for resolving which IDs a timeseries is composed of.
CREATE TABLE tsdb.ts_index (
	tsid uuid,
	id bigint,
	type varchar,
	PRIMARY KEY tsid
);
