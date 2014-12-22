
CASSANDRA_VERSION := 2.1.3
CASSANDRA := third_party/cassandra/cassandra-driver-core-$(CASSANDRA_VERSION).jar
CASSANDRA_BASE_URL := http://central.maven.org/maven2/com/datastax/cassandra/cassandra-driver-core/$(CASSANDRA_VERSION)

$(CASSANDRA): $(CASSANDRA).md5
	set dummy "$(CASSANDRA_BASE_URL)" "$(CASSANDRA)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(CASSANDRA)
