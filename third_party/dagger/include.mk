
DAGGER_VERSION := 1.2.2

#
# Dagger runtime
#
DAGGER_RUNTIME := third_party/dagger/dagger-$(DAGGER_VERSION).jar
DAGGER_RUNTIME_BASE_URL := http://central.maven.org/maven2/com/squareup/dagger/dagger/$(DAGGER_VERSION)

$(DAGGER_RUNTIME): $(DAGGER_RUNTIME).md5
	set dummy "$(DAGGER_RUNTIME_BASE_URL)" "$(DAGGER_RUNTIME)"; shift; $(FETCH_DEPENDENCY)


#
# Dagger compiler
#
DAGGER_COMPILER := third_party/dagger/dagger-compiler-$(DAGGER_VERSION).jar
DAGGER_COMPILER_BASE_URL := http://central.maven.org/maven2/com/squareup/dagger/dagger-compiler/$(DAGGER_VERSION)

$(DAGGER_COMPILER): $(DAGGER_COMPILER).md5
	set dummy "$(DAGGER_COMPILER_BASE_URL)" "$(DAGGER_COMPILER)"; shift; $(FETCH_DEPENDENCY)


THIRD_PARTY += $(DAGGER_RUNTIME) $(DAGGER_COMPILER)
