# Copyright 2010 StumbleUpon, Inc.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

all: jar
# TODO(tsuna): Use automake to avoid relying on GNU make extensions.

top_builddir = build
package = net.opentsdb
spec_title = OpenTSDB
spec_vendor = StumbleUpon, Inc.
spec_version = 1.0
BUILT_SOURCES = src/BuildData.java
tsdb_JAVA = \
	src/core/Aggregator.java	\
	src/core/Aggregators.java	\
	src/core/Const.java	\
	src/core/DataPoint.java	\
	src/core/DataPoints.java	\
	src/core/DataPointsIterator.java	\
	src/core/IncomingDataPoints.java	\
	src/core/IllegalDataException.java	\
	src/core/Query.java	\
	src/core/RowKey.java	\
	src/core/RowSeq.java	\
	src/core/SeekableView.java	\
	src/core/Span.java	\
	src/core/SpanGroup.java	\
	src/core/TSDB.java	\
	src/core/TSDBInterface.java	\
	src/core/Tags.java	\
	src/core/TsdbQuery.java	\
	src/core/WritableDataPoints.java	\
	src/graph/Plot.java	\
	src/stats/Histogram.java	\
	src/stats/StatsCollector.java	\
	src/tools/ArgP.java	\
	src/tools/CliOptions.java	\
	src/tools/CliQuery.java	\
	src/tools/Core.java	\
	src/tools/DumpSeries.java	\
	src/tools/Fsck.java	\
	src/tools/TSDMain.java	\
	src/tools/TextImporter.java	\
	src/tools/UidManager.java	\
	src/tsd/BadRequestException.java	\
	src/tsd/ConnectionManager.java	\
	src/tsd/GnuplotException.java	\
	src/tsd/GraphHandler.java	\
	src/tsd/HttpQuery.java	\
	src/tsd/HttpRpc.java	\
	src/tsd/LogsRpc.java	\
	src/tsd/PipelineFactory.java	\
	src/tsd/PutDataPointRpc.java	\
	src/tsd/RpcHandler.java	\
	src/tsd/StaticFileRpc.java	\
	src/tsd/TelnetRpc.java	\
	src/tsd/WordSplitter.java	\
	src/uid/NoSuchUniqueId.java	\
	src/uid/NoSuchUniqueName.java	\
	src/uid/UniqueId.java	\
	src/uid/UniqueIdInterface.java	\

tsdb_LIBADD = \
	third_party/hbase/hbaseasync-1.0.jar	\
	third_party/logback/logback-classic-0.9.24.jar	\
	third_party/logback/logback-core-0.9.24.jar	\
	third_party/netty/netty-3.2.3.Final.jar	\
	third_party/slf4j/jcl-over-slf4j-1.6.1.jar	\
	third_party/slf4j/log4j-over-slf4j-1.6.1.jar	\
	third_party/slf4j/slf4j-api-1.6.1.jar	\
	third_party/suasync/suasync-1.0.jar	\
	third_party/zookeeper/zookeeper-3.3.1.jar	\

test_JAVA = \
	src/core/TestTags.java	\
	src/stats/TestHistogram.java	\
	src/tsd/TestGraphHandler.java	\
	src/uid/TestNoSuchUniqueId.java	\
	src/uid/TestUniqueId.java	\

test_LIBADD = \
	$(tsdb_LIBADD) \
	third_party/javassist/javassist-3.13.GA.jar	\
	third_party/junit/junit-4.8.2.jar	\
	third_party/mockito/mockito-1.8.5.jar	\
	third_party/powermock/powermock-mockito-1.4.5.jar	\
        $(jar)

httpui_JAVA = \
	src/tsd/client/DateTimeBox.java	\
	src/tsd/client/EventsHandler.java	\
	src/tsd/client/GotJsonCallback.java	\
	src/tsd/client/MetricForm.java	\
	src/tsd/client/QueryUi.java	\
	src/tsd/client/RemoteOracle.java	\
	src/tsd/client/ValidatedTextBox.java	\

httpui_DEPENDENCIES = src/tsd/QueryUi.gwt.xml

dist_pkgdata_DATA = \
	src/tsd/static/favicon.ico	\

GWT_DEV = third_party/gwt/gwt-dev-2.0.4.jar
GWT_SDK = third_party/gwt/gwt-user-2.0.4.jar
GWTC_JVM_ARGS =  # add jvmarg -Xss16M or similar if you see a StackOverflowError
GWTC_ARGS = -ea  # Additional arguments like -style PRETTY or -logLevel DEBUG

TESTS = $(test_JAVA:src/%.java=$(top_builddir)/$(package_dir)/%.class)
AM_JAVACFLAGS = -Xlint -source 6
JVM_ARGS =
package_dir = $(subst .,/,$(package))
classes=$(tsdb_JAVA:src/%.java=$(top_builddir)/$(package_dir)/%.class) \
	$(BUILT_SOURCES:src/%.java=$(top_builddir)/$(package_dir)/%.class)
jar = $(top_builddir)/tsdb-$(spec_version).jar
test_classes=$(test_JAVA:src/%.java=$(top_builddir)/$(package_dir)/%.class)

src/BuildData.java: .git/HEAD $(tsdb_JAVA) ./buildtools/gen_build_data.sh
	./buildtools/gen_build_data.sh src/BuildData.java $(package)

jar: $(jar) $(TESTS) $(BUILT_SOURCES) $(top_builddir)/.gwtc-stamp

get_dep_classpath = `echo $(tsdb_LIBADD) | tr ' ' ':'`
$(top_builddir)/.javac-stamp: $(tsdb_JAVA) $(BUILT_SOURCES) $(tsdb_LIBADD)
	@mkdir -p $(top_builddir)
	javac $(AM_JAVACFLAGS) -cp $(get_dep_classpath) \
	  -d $(top_builddir) $(tsdb_JAVA) $(BUILT_SOURCES)
	@touch "$@"

# The GWT compiler is way too slow, that's not very Googley.  So we save the
# MD5 of the files we compile in the stamp file and everytime `make' things it
# needs to recompile the GWT code, we verify whether the code really changed
# or whether it's just a file that was touched (which happens frequently when
# using Git while rebasing and whatnot).
gwtc: $(top_builddir)/.gwtc-stamp
MD5 = md5  # TODO(tsuna): Detect the right command to use at configure time.
$(top_builddir)/.gwtc-stamp: $(httpui_JAVA) $(httpui_DEPENDENCIES)
	@mkdir -p $(top_builddir)/gwt
	cat $(httpui_JAVA) | $(MD5) >"$@-t"
	cmp -s "$@" "$@-t" || \
          java $(GWTC_JVM_ARGS) -cp $(GWT_DEV):$(GWT_SDK):src com.google.gwt.dev.Compiler \
            $(GWTC_ARGS) -war $(top_builddir)/gwt tsd.QueryUi
	mv "$@-t" "$@"

DEV_TSD_ARGS = \
  --port=$(DEV_TSD_PORT) \
  --staticroot=$(DEV_TSD_STATICROOT) --cachedir=$(DEV_TSD_CACHEDIR)
DEV_TSD_PORT = 4242
DEV_TSD_STATICROOT = $(top_builddir)/staticroot
DEV_TSD_CACHEDIR = /tmp/tsd
GWT_DEV_URL = http://127.0.0.1:$(DEV_TSD_PORT)/

GWT_DEV_ARGS = -Xmx512m  # The development mode is a memory hog.
gwtdev: $(top_builddir)/.gwtc-stamp
	java $(GWT_DEV_ARGS) -ea -cp $(GWT_DEV):$(GWT_SDK):src com.google.gwt.dev.DevMode \
	  -startupUrl $(GWT_DEV_URL) -noserver -war $(top_builddir)/gwt tsd.QueryUi

staticroot: jar $(top_builddir)/.staticroot-stamp

gwttsd: staticroot
	./src/tsdb tsd $(DEV_TSD_ARGS)

$(top_builddir)/.staticroot-stamp: $(dist_pkgdata_DATA) $(top_builddir)/.gwtc-stamp
	mkdir -p $(DEV_TSD_STATICROOT)
	cp $(dist_pkgdata_DATA) $(DEV_TSD_STATICROOT)
	find -L $(DEV_TSD_STATICROOT) -type l -delete
	p=`pwd`/$(top_builddir)/gwt/queryui && cd $(DEV_TSD_STATICROOT) \
	  && for i in $$p/*; do ln -s -f "$$i" || break; done
	find -L $(DEV_TSD_STATICROOT)/gwt -type f | xargs touch
	@touch $(top_builddir)/.staticroot-stamp

get_runtime_dep_classpath = `echo $(test_LIBADD) | tr ' ' ':'`
$(test_classes): $(jar) $(test_JAVA) $(test_LIBADD)
	javac $(AM_JAVACFLAGS) -cp $(get_runtime_dep_classpath) \
	  -d $(top_builddir) $(test_JAVA)

printcp:
	@echo $(tsdb_LIBADD) $(jar) | tr ' ' '\n' | sed "s:^:`pwd`/:" | tr '\n' ':'

classes_with_nested_classes = $(classes:$(top_builddir)/%.class=%*.class)
test_classes_with_nested_classes = $(test_classes:$(top_builddir)/%.class=%*.class)

# Little set script to make a pretty-ish banner.
BANNER = sed 's/^.*/  &  /;h;s/./=/g;p;x;p;x'
check: $(TESTS)
	classes=`cd $(top_builddir) && echo $(test_classes_with_nested_classes)` && \
        success=: && cp="$(get_runtime_dep_classpath):$(top_builddir)" && \
        for i in $$classes; do \
          case $$i in (*[$$]*) continue;; esac; \
	  echo "Running tests for `basename $$i .class`" | $(BANNER); \
          java -ea $(JVM_ARGS) -cp "$$cp" org.junit.runner.JUnitCore `echo $${i%.class} | tr / .` $(ARGS) || success=false; \
        done && $$success

pkg_version = \
  `git rev-list --pretty=format:%h HEAD --max-count=1 | sed 1d || echo unknown`
$(top_builddir)/manifest: $(top_builddir)/.javac-stamp .git/HEAD
	{ echo "Specification-Title: $(spec_title)"; \
          echo "Specification-Version: $(spec_version)"; \
          echo "Specification-Vendor: $(spec_vendor)"; \
          echo "Implementation-Title: $(package)"; \
          echo "Implementation-Version: $(pkg_version)"; \
          echo "Implementation-Vendor: $(spec_vendor)"; } >"$@"

$(jar): $(top_builddir)/manifest $(top_builddir)/.javac-stamp $(classes)
	cd $(top_builddir) && jar cfm `basename $(jar)` manifest $(classes_with_nested_classes) \
         || { rv=$$? && rm -f `basename $(jar)` && exit $$rv; }
#                       ^^^^^^^^^^^^^^^^^^^^^^^
# I've seen cases where `jar' exits with an error but leaves a partially built .jar file!

doc: $(top_builddir)/api/index.html

JDK_JAVADOC=http://download.oracle.com/javase/6/docs/api
NETTY_JAVADOC=http://docs.jboss.org/netty/3.2/api
$(top_builddir)/api/index.html: $(tsdb_JAVA) $(BUILT_SOURCES)
	javadoc -d $(top_builddir)/api -classpath $(get_dep_classpath) \
          -link $(JDK_JAVADOC) -link $(NETTY_JAVADOC) $(tsdb_JAVA) $(BUILT_SOURCES)

clean:
	@rm -f $(top_builddir)/.javac-stamp $(top_builddir)/.gwtc-stamp* $(top_builddir)/.staticroot-stamp
	rm -rf $(top_builddir)/gwt $(top_builddir)/staticroot
	rm -f $(top_builddir)/manifest $(BUILT_SOURCES)
	cd $(top_builddir) || exit 0 && rm -f $(classes_with_nested_classes) $(test_classes_with_nested_classes)
	cd $(top_builddir) || exit 0 \
	  && test -d $(package_dir) || exit 0 \
	  && find $(package_dir) -depth -type d -exec rmdir {} ';' \
	  && dir=$(package_dir) && dir=$${dir%/*} \
	  && while test x"$$dir" != x"$${dir%/*}"; do \
	       rmdir "$$dir" && dir=$${dir%/*} || break; \
	     done \
	  && rmdir "$$dir"

distclean: clean
	rm -f $(jar)
	rm -rf $(top_builddir)/api
	test ! -d $(top_builddir) || rmdir $(top_builddir)

.PHONY: all jar clean distclean doc check gwtc gwtdev staticroot gwttsd printcp
