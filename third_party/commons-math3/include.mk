

COMMONS_MATH3_VERSION := 3.5
COMMONS_MATH3 := third_party/commons-math3/commons-math3-$(MATH3_VERSION).jar
COMMONS_MATH3_BASE_URL := http://central.maven.org/maven2/org/apache/commons/commons-math3/$(MATH3_VERSION)

$(COMMONS_MATH3): $(COMMONS_MATH3).md5
	set dummy "$(COMMONS_MATH3_BASE_URL)" "$(COMMONS_MATH3)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(COMMONS_MATH3)
