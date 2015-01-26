
METRICS_VERSION := 3.1.0

#
# Metrics core
#
METRICS_VERSION := 3.1.0
METRICS_CORE := third_party/metrics/metrics-core-$(METRICS_VERSION).jar
METRICS_CORE_BASE_URL := http://central.maven.org/maven2/io/dropwizard/metrics/metrics-core/$(METRICS_VERSION)

$(METRICS_CORE): $(METRICS_CORE).md5
	set dummy "$(METRICS_CORE_BASE_URL)" "$(METRICS_CORE)"; shift; $(FETCH_DEPENDENCY)


#
# Metrics JVM
#
METRICS_JVM := third_party/metrics/metrics-jvm-$(METRICS_VERSION).jar
METRICS_JVM_BASE_URL := http://central.maven.org/maven2/io/dropwizard/metrics/metrics-jvm/$(METRICS_VERSION)

$(METRICS_JVM): $(METRICS_JVM).md5
	set dummy "$(METRICS_JVM_BASE_URL)" "$(METRICS_JVM)"; shift; $(FETCH_DEPENDENCY)


THIRD_PARTY += $(METRICS_CORE) $(METRICS_JVM)
