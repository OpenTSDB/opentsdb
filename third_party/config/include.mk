
CONFIG_VERSION := 1.2.1
CONFIG := third_party/config/config-$(CONFIG_VERSION).jar
CONFIG_BASE_URL := http://central.maven.org/maven2/com/typesafe/config/$(CONFIG_VERSION)

$(CONFIG): $(CONFIG).md5
	set dummy "$(CONFIG_BASE_URL)" "$(CONFIG)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(CONFIG)
