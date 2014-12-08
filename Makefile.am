# Copyright (C) 2011-2012  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation; either version 2.1 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

ACLOCAL_AMFLAGS = -I build-aux

all-am: jar staticroot

package = net.opentsdb
spec_title = OpenTSDB
spec_vendor = The OpenTSDB Authors
jar := tsdb-$(PACKAGE_VERSION).jar
plugin_test_jar := plugin_test.jar
builddata_SRC := src/BuildData.java
BUILT_SOURCES = $(builddata_SRC)
nodist_bin_SCRIPTS = tsdb
dist_noinst_SCRIPTS = src/create_table.sh src/upgrade_1to2.sh src/mygnuplot.sh \
  src/mygnuplot.bat src/opentsdb.conf tools/opentsdb_restart.py src/logback.xml
dist_noinst_DATA = pom.xml.in build-aux/rpm/opentsdb.conf \
  build-aux/rpm/logback.xml build-aux/rpm/init.d/opentsdb
tsdb_SRC := \
	src/core/AggregationIterator.java	\
	src/core/Aggregator.java	\
	src/core/Aggregators.java	\
	src/core/BatchedDataPoints.java \
	src/core/ByteBufferList.java	\
	src/core/ColumnDatapointIterator.java	\
	src/core/CompactionQueue.java	\
	src/core/Const.java	\
	src/core/DataPoint.java	\
	src/core/DataPoints.java	\
	src/core/DataPointsIterator.java	\
	src/core/Downsampler.java \
	src/core/IncomingDataPoint.java	\
	src/core/IncomingDataPoints.java	\
	src/core/IllegalDataException.java	\
	src/core/Internal.java	\
	src/core/MutableDataPoint.java \
	src/core/Query.java	\
	src/core/RateOptions.java  \
	src/core/RateSpan.java  \
	src/core/RowKey.java	\
	src/core/RowSeq.java	\
	src/core/SeekableView.java	\
	src/core/Span.java	\
	src/core/SpanGroup.java	\
	src/core/TSDB.java	\
	src/core/Tags.java	\
	src/core/TsdbQuery.java	\
	src/core/TSQuery.java	\
	src/core/TSSubQuery.java	\
	src/core/WritableDataPoints.java	\
	src/graph/Plot.java	\
	src/meta/Annotation.java	\
	src/meta/TSMeta.java	\
	src/meta/TSUIDQuery.java	\
	src/meta/UIDMeta.java	\
	src/search/SearchPlugin.java	\
	src/search/SearchQuery.java	\
	src/search/TimeSeriesLookup.java	\
	src/stats/Histogram.java	\
	src/stats/StatsCollector.java	\
	src/tools/ArgP.java	\
	src/tools/CliOptions.java	\
	src/tools/CliQuery.java	\
	src/tools/CliUtils.java	\
	src/tools/DumpSeries.java	\
	src/tools/Fsck.java	\
	src/tools/FsckOptions.java	\
	src/tools/MetaPurge.java	\
	src/tools/MetaSync.java	\
	src/tools/Search.java	\
	src/tools/TSDMain.java	\
	src/tools/TextImporter.java	\
	src/tools/TreeSync.java	\
	src/tools/UidManager.java	\
	src/tree/Branch.java	\
	src/tree/Leaf.java	\
	src/tree/Tree.java	\
	src/tree/TreeBuilder.java	\
	src/tree/TreeRule.java	\
	src/tsd/AnnotationRpc.java	\
	src/tsd/BadRequestException.java	\
	src/tsd/ConnectionManager.java	\
	src/tsd/GnuplotException.java	\
	src/tsd/GraphHandler.java	\
	src/tsd/HttpJsonSerializer.java	\
	src/tsd/HttpSerializer.java	\
	src/tsd/HttpQuery.java	\
	src/tsd/HttpRpc.java	\
	src/tsd/LineBasedFrameDecoder.java	\
	src/tsd/LogsRpc.java	\
	src/tsd/PipelineFactory.java	\
	src/tsd/PutDataPointRpc.java	\
	src/tsd/QueryRpc.java	\
	src/tsd/RpcHandler.java	\
	src/tsd/RpcPlugin.java	\
	src/tsd/RTPublisher.java	\
	src/tsd/SearchRpc.java	\
	src/tsd/StaticFileRpc.java	\
	src/tsd/StatsRpc.java	\
	src/tsd/SuggestRpc.java	\
	src/tsd/TelnetRpc.java	\
	src/tsd/TreeRpc.java	\
	src/tsd/UniqueIdRpc.java	\
	src/tsd/WordSplitter.java	\
	src/uid/NoSuchUniqueId.java	\
	src/uid/NoSuchUniqueName.java	\
	src/uid/UniqueId.java	\
	src/uid/UniqueIdInterface.java \
	src/utils/ByteArrayPair.java \
	src/utils/Config.java \
	src/utils/DateTime.java \
	src/utils/JSON.java \
	src/utils/JSONException.java	\
	src/utils/Pair.java	\
	src/utils/PluginLoader.java

tsdb_DEPS = \
	$(ASYNCHBASE)	\
	$(GUAVA)	\
	$(LOG4J_OVER_SLF4J)	\
	$(LOGBACK_CLASSIC)	\
	$(LOGBACK_CORE)	\
	$(JACKSON_ANNOTATIONS)	\
	$(JACKSON_CORE)	\
	$(JACKSON_DATABIND)	\
	$(NETTY)	\
	$(PROTOBUF)	\
	$(SLF4J_API)	\
	$(SUASYNC)	\
	$(ZOOKEEPER) \
	$(APACHE_MATH)

test_SRC := \
	test/core/SeekableViewsForTest.java \
	test/core/TestAggregationIterator.java \
	test/core/TestAggregators.java \
	test/core/TestBatchedDataPoints.java \
	test/core/TestCompactionQueue.java	\
	test/core/TestDownsampler.java \
	test/core/TestInternal.java	\
	test/core/TestMutableDataPoint.java	\
	test/core/TestRateSpan.java	\
	test/core/TestRowSeq.java	\
	test/core/TestSpan.java	\
	test/core/TestTags.java	\
	test/core/TestTSDB.java	\
	test/core/TestTsdbQueryDownsample.java	\
	test/core/TestTsdbQuery.java	\
	test/core/TestTSQuery.java	\
	test/core/TestTSSubQuery.java	\
	test/plugin/DummyPlugin.java \
	test/meta/TestAnnotation.java	\
	test/meta/TestTSMeta.java	\
	test/meta/TestTSUIDQuery.java	\
	test/meta/TestUIDMeta.java	\
	test/search/TestSearchPlugin.java	\
	test/search/TestSearchQuery.java	\
	test/search/TestTimeSeriesLookup.java	\
	test/stats/TestHistogram.java	\
	test/storage/MockBase.java	\
	test/tools/TestDumpSeries.java	\
	test/tools/TestFsck.java	\
	test/tools/TestTextImporter.java	\
	test/tools/TestUID.java	\
	test/tree/TestBranch.java	\
	test/tree/TestLeaf.java	\
	test/tree/TestTree.java	\
	test/tree/TestTreeBuilder.java	\
	test/tree/TestTreeRule.java	\
	test/tsd/NettyMocks.java	\
	test/tsd/TestAnnotationRpc.java	\
	test/tsd/TestGraphHandler.java	\
	test/tsd/TestHttpJsonSerializer.java	\
	test/tsd/TestHttpQuery.java	\
	test/tsd/TestPutRpc.java	\
	test/tsd/TestQueryRpc.java	\
	test/tsd/TestRpcHandler.java	\
	test/tsd/TestRpcPlugin.java	\
	test/tsd/TestRTPublisher.java	\
	test/tsd/TestSearchRpc.java	\
	test/tsd/TestSuggestRpc.java	\
	test/tsd/TestTreeRpc.java	\
	test/tsd/TestUniqueIdRpc.java	\
	test/uid/TestNoSuchUniqueId.java	\
	test/uid/TestUniqueId.java \
	test/utils/TestByteArrayPair.java \
	test/utils/TestConfig.java \
	test/utils/TestDateTime.java \
	test/utils/TestJSON.java \
	test/utils/TestPair.java \
	test/utils/TestPluginLoader.java
	
test_plugin_SRC := \
  test/plugin/DummyPluginA.java \
  test/plugin/DummyPluginB.java \
  test/search/DummySearchPlugin.java \
  test/tsd/DummyHttpSerializer.java \
  test/tsd/DummyRpcPlugin.java \
  test/tsd/DummyRTPublisher.java

# Do NOT include the test dir path, just the META portion
test_plugin_SVCS := \
  META-INF/services/net.opentsdb.plugin.DummyPlugin \
  META-INF/services/net.opentsdb.search.SearchPlugin \
  META-INF/services/net.opentsdb.tsd.HttpSerializer \
  META-INF/services/net.opentsdb.tsd.RpcPlugin \
  META-INF/services/net.opentsdb.tsd.RTPublisher
  
test_plugin_MF := \
  test/META-INF/MANIFEST.MF

test_DEPS = \
	$(tsdb_DEPS) \
	$(JAVASSIST)	\
	$(JUNIT)	\
        $(HAMCREST)	\
	$(MOCKITO)	\
        $(OBJENESIS)	\
	$(POWERMOCK_MOCKITO)	\
        $(jar)

httpui_SRC := \
	src/tsd/client/DateTimeBox.java	\
	src/tsd/client/EventsHandler.java	\
	src/tsd/client/GotJsonCallback.java	\
	src/tsd/client/MetricForm.java	\
	src/tsd/client/QueryString.java	\
	src/tsd/client/QueryUi.java	\
	src/tsd/client/RemoteOracle.java	\
	src/tsd/client/ValidatedTextBox.java

httpui_DEPS = src/tsd/QueryUi.gwt.xml

#dist_pkgdata_DATA = src/logback.xml
dist_static_DATA = src/tsd/static/favicon.ico

EXTRA_DIST = tsdb.in $(tsdb_SRC) $(test_SRC) \
        $(test_plugin_SRC) $(test_plugin_MF) $(test_plugin_SVCS:%=test/%) \
        $(THIRD_PARTY) $(THIRD_PARTY:=.md5) \
        $(httpui_SRC) $(httpui_DEPS) \
	tools/check_tsd	\
	tools/clean_cache.sh	\
	tools/tsddrain.py	\
	tools/opentsdb_restart.py	\
	opentsdb.spec	\
        bootstrap build.sh build-aux/gen_build_data.sh $(builddata_SRC)

GWTC_JVM_ARGS =  # add jvmarg -Xss16M or similar if you see a StackOverflowError
GWTC_ARGS = -ea -strict  # Additional arguments like -style PRETTY or -logLevel DEBUG

package_dir := $(subst .,/,$(package))
UNITTESTS := $(test_SRC:test/%.java=$(package_dir)/%.class)
PLUGINTESTS := $(test_plugin_SRC:test/%.java=$(package_dir)/%.class)
PLUGINSVCS := $(test_plugin_SVCS:%=-C $(srcdir)/test %)
AM_JAVACFLAGS = -Xlint -source 6 -encoding utf-8
JVM_ARGS =
classes := $(tsdb_SRC:src/%.java=$(package_dir)/%.class) \
        $(builddata_SRC:src/%.java=$(package_dir)/%.class)

edit_tsdb_script := srcdir=''; test -f ./$$script.in || srcdir=$(srcdir)/; \
        sed -e "s:@pkgdatadir[@]:$$pkgdatadir:g" \
            -e "s:@abs_srcdir[@]:$$abs_srcdir:g" \
            -e "s:@abs_builddir[@]:$$abs_builddir:g" \
            -e "s:@configdir[@]:$$configdir:g" \
            $${srcdir}$$script.in >$$script.tmp

tsdb: $(srcdir)/tsdb.in
tsdb: Makefile
	rm -f $@ $@.tmp
	script=$@; pkgdatadir=''; \
          abs_srcdir='$(abs_srcdir)'; abs_builddir='$(abs_builddir)'; \
          $(edit_tsdb_script)
	chmod +x $@.tmp
	chmod a-w $@.tmp
	mv $@.tmp $@

printdeps:
	@echo $(tsdb_DEPS) $(jar)

# Adjust the paths in the 'tsdb' script after it's been installed.
# This is kind of a hack, but I couldn't find a better way to adjust the paths
# in the script before it gets installed...
install-exec-hook:
	script=tsdb; pkgdatadir='$(pkgdatadir)'; configdir='$(pkgdatadir)/etc/opentsdb'; \
          abs_srcdir=''; abs_builddir=''; $(edit_tsdb_script)
	cat tsdb.tmp >"$(DESTDIR)$(bindir)/tsdb"
	rm -f tsdb.tmp

$(builddata_SRC): .git/HEAD $(tsdb_SRC) $(top_srcdir)/build-aux/gen_build_data.sh
	$(srcdir)/build-aux/gen_build_data.sh $(builddata_SRC) $(package) $(PACKAGE_VERSION)

jar: $(jar) .javac-unittests-stamp .gwtc-stamp

JAVA_COMPILE := $(JAVAC) $(AM_JAVACFLAGS) -d .

# Sets the shell variable $src to the list of `.java' files we depend on
# that need to be recompiled.  Runs "exit 0" if there's nothing to compile,
# which can happen if only non-.java dependencies have been updated.
filter_src = \
 src=''; \
 for i in $?; do \
   case $$i in (*.java) \
     test -f "$(srcdir)/$$i" && i="$(srcdir)/$$i"; \
     src="$$src $$i";; \
   esac; \
 done; \
 test -n "$$src" || exit 0
# Touches all the targets if any of the dependencies are newer.
# This is useful to force-recompile all files if one of the
# dependent libraries gets updated.
refresh_src = \
 for i in $@; do \
   test -f "$$i" && touch "$$i" && continue; \
   i="$(srcdir)/$$i"; \
   test -f "$$i" && touch "$$i" && continue; \
 done; :

$(tsdb_SRC): $(tsdb_DEPS)
	@$(refresh_src)

find_jar = test -f "$$jar" && echo "$$jar" || echo "$(srcdir)/$$jar"
get_dep_classpath = `for jar in $(tsdb_DEPS); do $(find_jar); done | tr '\n' ':'`
.javac-stamp: $(tsdb_SRC) $(builddata_SRC)
	@$(filter_src); cp=$(get_dep_classpath); \
          echo "$(JAVA_COMPILE) -cp $$cp $$src"; \
                $(JAVA_COMPILE) -cp $$cp $$src
	@touch "$@"

VALIDATION_API_CLASSPATH = `jar=$(VALIDATION_API); $(find_jar)`:`jar=$(VALIDATION_API_SOURCES); $(find_jar)`
GWT_CLASSPATH = $(VALIDATION_API_CLASSPATH):`jar=$(GWT_DEV); $(find_jar)`:`jar=$(GWT_USER); $(find_jar)`:$(srcdir)/src
# The GWT compiler is way too slow, that's not very Googley.  So we save the
# MD5 of the files we compile in the stamp file and everytime `make' things it
# needs to recompile the GWT code, we verify whether the code really changed
# or whether it's just a file that was touched (which happens frequently when
# using Git while rebasing and whatnot).
gwtc: .gwtc-stamp
.gwtc-stamp: $(httpui_SRC) $(httpui_DEPS) $(VALIDATION_API) $(VALIDATION_API_SOURCES) $(GWT_DEV) $(GWT_USER)
	@$(mkdir_p) gwt
	{ cd $(srcdir) && cat $(httpui_SRC); } | $(MD5) >"$@-t"
	cmp -s "$@" "$@-t" && exit 0; \
        $(JAVA) $(GWTC_JVM_ARGS) -cp $(GWT_CLASSPATH) com.google.gwt.dev.Compiler \
            $(GWTC_ARGS) -war gwt tsd.QueryUi
	@mv "$@-t" "$@"

DEV_TSD_ARGS = \
  --port=$(DEV_TSD_PORT) \
  --staticroot=$(DEV_TSD_STATICROOT) --cachedir=$(DEV_TSD_CACHEDIR)
DEV_TSD_PORT = 4242
DEV_TSD_STATICROOT = staticroot
DEV_TSD_CACHEDIR = /tmp/tsd
GWT_DEV_URL = http://127.0.0.1:$(DEV_TSD_PORT)/

GWT_DEV_ARGS = -Xmx512m  # The development mode is a memory hog.
gwtdev: .gwtc-stamp
	$(JAVA) $(GWT_DEV_ARGS) -ea -cp $(GWT_CLASSPATH) com.google.gwt.dev.DevMode \
	  -startupUrl $(GWT_DEV_URL) -noserver -war gwt tsd.QueryUi

staticroot: jar .staticroot-stamp

gwttsd: staticroot
	./tsdb tsd $(DEV_TSD_ARGS)

.staticroot-stamp: $(dist_static_DATA) .gwtc-stamp
	$(mkdir_p) $(DEV_TSD_STATICROOT)
	cp $(dist_static_DATA:%=$(srcdir)/%) $(DEV_TSD_STATICROOT)
	find -L $(DEV_TSD_STATICROOT) -type l -exec rm {} \;
	p=`pwd`/gwt/queryui && cd $(DEV_TSD_STATICROOT) \
	  && for i in $$p/*; do ln -s -f "$$i" || break; done
	find -L $(DEV_TSD_STATICROOT)/gwt -type f | xargs touch
	@touch .staticroot-stamp

# Install all the files from the static dir into $staticdir.
# Ideally I'd like Automake to take care of this, but right now I don't see
# how to tell it to install a bunch of files recursively for which I don't
# know ahead of time what the file names are.
install-data-local: staticroot install-data-lib install-data-tools \
    install-data-bin install-data-etc
	@$(NORMAL_INSTALL)
	test -z "$(staticdir)" || $(mkdir_p) "$(DESTDIR)$(staticdir)"
	@set -e; pwd; ls -lFh; cd "$(DEV_TSD_STATICROOT)"; \
          list=`find -L . ! -type d`; for p in $$list; do \
          p=$${p#./}; \
	  if test -f "$$p"; then d=; else d="$(srcdir)/"; fi; \
          dstdir=`dirname "$(DESTDIR)$(staticdir)/$$p"`; \
          if test -d "$$dstdir"; then :; else \
            echo " $(mkdir_p) '$$dstdir'"; $(mkdir_p) "$$dstdir"; fi; \
	  echo " $(INSTALL_DATA) '$$d$$p' '$(DESTDIR)$(staticdir)/$$p'"; \
	  $(INSTALL_DATA) "$$d$$p" "$(DESTDIR)$(staticdir)/$$p"; \
	done

install-data-lib: $(tsdb_DEPS) $(jar)
	@$(NORMAL_INSTALL)
	@list='$(tsdb_DEPS) $(jar)'; test -n "$(pkgdatadir)" || list=; \
	destdatalibdir="$(DESTDIR)$(pkgdatadir)/lib" ; \
	if test -n "$$list"; then \
	  echo " $(mkdir_p) $$destdatalibdir"; \
	  $(mkdir_p) "$$destdatalibdir" || exit 1; \
	fi; \
	for p in $$list; do \
	  if test -f "$$p"; then d=; else d="$(srcdir)/"; fi; \
	  echo "$$d$$p"; \
	done | $(am__base_list) | \
	while read files; do \
	  echo " $(INSTALL_DATA) $$files '$$destdatalibdir'"; \
	  $(INSTALL_DATA) $$files "$$destdatalibdir" || exit $$?; \
	done

uninstall-data-lib:
	@$(NORMAL_UNINSTALL)
	destdatalibdir="$(DESTDIR)$(pkgdatadir)/lib" ; \
	echo " rm -rf $$destdatalibdir" ; \
	rm -rf "$$destdatalibdir"

install-data-tools: $(tsdb_DEPS) $(jar)
	@$(NORMAL_INSTALL)
	destdatatoolsdir="$(DESTDIR)$(pkgdatadir)/tools" ; \
	echo " $(mkdir_p) $$destdatatoolsdir"; \
	$(mkdir_p) "$$destdatatoolsdir" || exit 1; \
	tools="$$tools $(top_srcdir)/tools/*" ; \
	tools="$$tools $(top_srcdir)/src/create_table.sh" ; \
	tools="$$tools $(top_srcdir)/src/upgrade_1to2.sh" ; \
	echo " $(INSTALL_SCRIPT)" $$tools "$$destdatatoolsdir" ; \
	$(INSTALL_SCRIPT) $$tools "$$destdatatoolsdir" || exit 1;

uninstall-data-tools:
	@$(NORMAL_UNINSTALL)
	destdatatoolsdir="$(DESTDIR)$(pkgdatadir)/tools" ; \
	echo " rm -rf $$destdatatoolsdir" ; \
	rm -rf "$$destdatatoolsdir"

install-data-bin: tsdb
	@$(NORMAL_INSTALL)
	destdatabindir="$(DESTDIR)$(pkgdatadir)/bin" ; \
	echo " $(mkdir_p) $$destdatabindir"; \
	$(mkdir_p) "$$destdatabindir" || exit 1; \
	bins="$$bins $(top_srcdir)/src/mygnuplot.sh" ; \
	bins="$$bins $(top_srcdir)/src/mygnuplot.bat" ; \
	bins="$$bins $(DESTDIR)$(bindir)/tsdb" ; \
	echo " $(INSTALL_SCRIPT)" $$bins "$$destdatabindir" ; \
	$(INSTALL_SCRIPT) $$bins "$$destdatabindir" || exit 1;

uninstall-data-bin:
	@$(NORMAL_UNINSTALL)
	destdatabindir="$(DESTDIR)$(pkgdatadir)/bin" ; \
	echo " rm -rf $$destdatabindir" ; \
	rm -rf "$$destdatabindir"

# NOTE: We keep a copy of /etc files at the package data directory.
# Users should create symlinks to etc/init.d/opentsdb and
# etc/opentsdb/opentsdb.conf if they want to use the stock script and
# configuration.
install-data-etc:
	@$(NORMAL_INSTALL)
	destdataetcdir="$(DESTDIR)$(pkgdatadir)/etc" ; \
	destdataconfdir="$$destdataetcdir/opentsdb" ; \
	destdatainitdir="$$destdataetcdir/init.d" ; \
	echo " $(mkdir_p) $$destdataconfdir"; \
	$(mkdir_p) "$$destdataconfdir" || exit 1; \
	echo " $(mkdir_p) $$destdatainitdir"; \
	$(mkdir_p) "$$destdatainitdir" || exit 1; \
	conf_files="$$conf_files $(top_srcdir)/build-aux/rpm/opentsdb.conf" ; \
	conf_files="$$conf_files $(top_srcdir)/build-aux/rpm/logback.xml" ; \
	echo " $(INSTALL_SCRIPT)" $$conf_files "$$destdataconfdir" ; \
	$(INSTALL_DATA) $$conf_files "$$destdataconfdir" || exit 1; \
	init_file="$(top_srcdir)/build-aux/rpm/init.d/opentsdb" ; \
	echo " $(INSTALL_SCRIPT)" $$init_file "$$destdatainitdir" ; \
	$(INSTALL_SCRIPT) $$init_file "$$destdatainitdir" || exit 1;

uninstall-data-etc:
	@$(NORMAL_UNINSTALL)
	destdataetcdir="$(DESTDIR)$(pkgdatadir)/etc" ; \
	echo " rm -rf $$destdataetcdir" ; \
	rm -rf "$$destdataetcdir"

uninstall-local: uninstall-data-lib uninstall-data-tools uninstall-data-bin \
	uninstall-data-etc
	@$(NORMAL_UNINSTALL)
	rm -rf "$(DESTDIR)$(staticdir)"

# I don't know why Automake leaves this directory behind.
uninstall-hook:
	test ! -d "$(pkgdatadir)" || rmdir "$(pkgdatadir)"

get_runtime_dep_classpath = `for jar in $(test_DEPS); do $(find_jar); done | tr '\n' ':'`
$(test_SRC): $(test_DEPS)
	@$(refresh_src)
	
$(test_plugin_SRC): $(test_DEPS)
	@$(refresh_src)

# compile the plugin unittest jar before the unittests
.javac-unittests-plugin-stamp: $(jar) $(test_plugin_SRC)
	@$(filter_src); cp=$(get_runtime_dep_classpath); \
          echo "$(JAVA_COMPILE) -cp $$cp $$src"; \
                $(JAVA_COMPILE) -cp $$cp $$src
	@touch "$@"
	@touch .javac-unittests-plugin-stamp

.javac-unittests-stamp: $(jar) $(test_SRC)
	@$(filter_src); cp=$(get_runtime_dep_classpath); \
          echo "$(JAVA_COMPILE) -cp $$cp $$src"; \
                $(JAVA_COMPILE) -cp $$cp $$src
	@touch "$@"
	@touch .javac-unittests-stamp

classes_with_nested_classes := $(classes:.class=*.class)
test_classes_with_nested_classes := $(UNITTESTS:.class=*.class)
test_plugin_classes := $(PLUGINTESTS:.class=*.class)

# Little set script to make a pretty-ish banner.
BANNER := sed 's/^.*/  &  /;h;s/./=/g;p;x;p;x'
check-local: .javac-unittests-stamp .javac-unittests-plugin-stamp $(plugin_test_jar)
	classes=`echo $(test_classes_with_nested_classes)` \
        && tests=0 && failures=0 \
        && cp="$(get_runtime_dep_classpath):$(srcdir)/src" && \
        for i in $$classes; do \
          case $$i in (*[$$]*) continue;; esac; \
          tests=$$((tests + 1)); \
          echo "Running tests for `basename $$i .class`" | $(BANNER); \
          $(JAVA) -ea -esa $(JVM_ARGS) -cp "$$cp" org.junit.runner.JUnitCore `echo $${i%.class} | tr / .` $(ARGS) \
          || failures=$$((failures + 1)); \
        done; \
        if test "$$failures" -eq 0; then \
          echo "All $$tests tests passed" | $(BANNER); \
        else \
          echo "$$failures out of $$tests failed, please send a report to $(PACKAGE_BUGREPORT)" | $(BANNER); \
        fi

git_version := \
  `git rev-list --pretty=format:%h HEAD --max-count=1 | sed 1d || echo unknown`
manifest: .javac-stamp .git/HEAD
	{ echo "Specification-Title: $(spec_title)"; \
          echo "Specification-Version: $(PACKAGE_VERSION)"; \
          echo "Specification-Vendor: $(spec_vendor)"; \
          echo "Implementation-Title: $(package)"; \
          echo "Implementation-Version: $(git_version)"; \
          echo "Implementation-Vendor: $(spec_vendor)"; } >"$@"

$(jar): manifest .javac-stamp $(classes)
	$(JAR) cfm `basename $(jar)` manifest $(classes_with_nested_classes) \
         || { rv=$$? && rm -f `basename $(jar)` && exit $$rv; }
#                       ^^^^^^^^^^^^^^^^^^^^^^^
# I've seen cases where `jar' exits with an error but leaves a partially built .jar file!

$(plugin_test_jar): .javac-unittests-plugin-stamp
	$(JAR) cvfm $(plugin_test_jar) $(srcdir)/$(test_plugin_MF) $(test_plugin_classes) $(PLUGINSVCS)

# Generate the file for those who get a tarball without it.  This happens if
# you download a tarball off GitHub for instance.
.git/HEAD:
	$(mkdir_p) .git && echo unknown >$@

JAVADOC_DIR = api
doc: $(JAVADOC_DIR)/index.html

JDK_JAVADOC = http://download.oracle.com/javase/6/docs/api
NETTY_JAVADOC = http://netty.io/$(NETTY_MAJOR_VERSION)/api
SUASYNC_JAVADOC = http://tsunanet.net/~tsuna/async/$(SUASYNC_VERSION)
$(JAVADOC_DIR)/index.html: $(tsdb_SRC)
	$(JAVADOC) -d $(JAVADOC_DIR) -classpath $(get_dep_classpath) \
          -link $(JDK_JAVADOC) -link $(NETTY_JAVADOC) -link $(SUASYNC_JAVADOC) \
          $? $(builddata_SRC)

dist-hook:
	$(mkdir_p) $(distdir)/.git
	echo $(git_version) >$(distdir)/.git/HEAD

mostlyclean-local:
	@rm -f .javac-stamp .javac-unittests-stamp .javac-unittests-plugin-stamp .gwtc-stamp* .staticroot-stamp
	rm -rf gwt gwt-unitCache staticroot
	rm -f manifest $(BUILT_SOURCES)
	rm -f $(classes_with_nested_classes) $(test_classes_with_nested_classes) $(test_plugin_classes)
	test -d $(package_dir) || exit 0 \
	  && find $(package_dir) -depth -type d -exec rmdir {} ';' \
	  && dir=$(package_dir) && dir=$${dir%/*} \
	  && while test x"$$dir" != x"$${dir%/*}"; do \
	       rmdir "$$dir" && dir=$${dir%/*} || break; \
	     done \
	  && rmdir "$$dir"

clean-local:
	rm -f $(jar) $(plugin_test_jar) tsdb $(srcdir)/pom.xml
	rm -rf $(JAVADOC_DIR)

distclean-local:
	rm -rf $(srcdir)/target

pom.xml: pom.xml.in Makefile
	(cd $(top_srcdir) ; ./build-aux/create-src-dir-overlay.sh)
	{ \
	  echo '<!-- Generated by Makefile on '`date`' -->'; \
	  sed <$< \
	    -e 's/@ASYNCHBASE_VERSION@/$(ASYNCHBASE_VERSION)/' \
	    -e 's/@GUAVA_VERSION@/$(GUAVA_VERSION)/' \
	    -e 's/@GWT_VERSION@/$(GWT_VERSION)/' \
	    -e 's/@HAMCREST_VERSION@/$(HAMCREST_VERSION)/' \
	    -e 's/@JACKSON_VERSION@/$(JACKSON_VERSION)/' \
	    -e 's/@JAVASSIST_VERSION@/$(JAVASSIST_VERSION)/' \
	    -e 's/@JUNIT_VERSION@/$(JUNIT_VERSION)/' \
	    -e 's/@LOG4J_OVER_SLF4J_VERSION@/$(LOG4J_OVER_SLF4J_VERSION)/' \
	    -e 's/@LOGBACK_CLASSIC_VERSION@/$(LOGBACK_CLASSIC_VERSION)/' \
	    -e 's/@LOGBACK_CORE_VERSION@/$(LOGBACK_CORE_VERSION)/' \
	    -e 's/@MOCKITO_VERSION@/$(MOCKITO_VERSION)/' \
	    -e 's/@NETTY_VERSION@/$(NETTY_VERSION)/' \
	    -e 's/@OBJENESIS_VERSION@/$(OBJENESIS_VERSION)/' \
	    -e 's/@POWERMOCK_MOCKITO_VERSION@/$(POWERMOCK_MOCKITO_VERSION)/' \
	    -e 's/@SLF4J_API_VERSION@/$(SLF4J_API_VERSION)/' \
	    -e 's/@SUASYNC_VERSION@/$(SUASYNC_VERSION)/' \
	    -e 's/@ZOOKEEPER_VERSION@/$(ZOOKEEPER_VERSION)/' \
	    -e 's/@APACHE_MATH_VERSION@/$(APACHE_MATH_VERSION)/' \
	    -e 's/@spec_title@/$(spec_title)/' \
	    -e 's/@spec_vendor@/$(spec_vendor)/' \
	    -e 's/@spec_version@/$(PACKAGE_VERSION)/' \
	    ; \
	} >$@-t
	mv $@-t ../$@

TIMESTAMP := $(shell date +"%Y%m%d%H%M%S")
RPM_REVISION := 1
RPM_TARGET := noarch
RPM := opentsdb-$(PACKAGE_VERSION)-$(RPM_REVISION).$(RPM_TARGET).rpm
RPM_SNAPSHOT := opentsdb-$(PACKAGE_VERSION)-$(RPM_REVISION)-$(TIMESTAMP)-"`whoami`".$(RPM_TARGET).rpm
SOURCE_TARBALL := opentsdb-$(PACKAGE_VERSION).tar.gz
rpm: $(RPM)

$(SOURCE_TARBALL): dist
$(RPM): $(SOURCE_TARBALL)
$(RPM): opentsdb.spec
	rpmbuild --target=$(RPM_TARGET) --buildroot=`pwd`/rpmbuildroot -bb $<
	test -f $@ || for rpm in noarch/$@ \
		`awk '$$1=="Name:"{print $$2}' $<`.`awk '$$1=="BuildArch:"{print $$2}' $<`.rpm; do \
	  test -f "$$rpm" && mv "$$rpm" $(RPM_SNAPSHOT) && break; \
	done
	if test -d $(RPM_TARGET); then rmdir $(RPM_TARGET); fi

debian: dist staticroot
	$(mkdir_p) $(distdir)/debian
	$(mkdir_p) $(distdir)/debian/DEBIAN
	$(mkdir_p) $(distdir)/debian/etc/init.d
	$(mkdir_p) $(distdir)/debian/etc/opentsdb
	$(mkdir_p) $(distdir)/debian/usr/share/opentsdb/bin
	$(mkdir_p) $(distdir)/debian/usr/share/opentsdb/lib
	$(mkdir_p) $(distdir)/debian/usr/share/opentsdb/plugins
	$(mkdir_p) $(distdir)/debian/usr/share/opentsdb/static
	$(mkdir_p) $(distdir)/debian/usr/share/opentsdb/tools
	cp $(top_srcdir)/build-aux/deb/logback.xml $(distdir)/debian/etc/opentsdb
	cp $(top_srcdir)/build-aux/deb/opentsdb.conf $(distdir)/debian/etc/opentsdb
	cp $(srcdir)/src/create_table.sh $(distdir)/debian/usr/share/opentsdb/tools
	cp $(srcdir)/src/upgrade_1to2.sh $(distdir)/debian/usr/share/opentsdb/tools
	cp $(srcdir)/src/mygnuplot.sh $(distdir)/debian/usr/share/opentsdb/bin
	script=tsdb; pkgdatadir='/usr/share/opentsdb'; configdir='/etc/opentsdb'; \
       abs_srcdir=''; abs_builddir=''; $(edit_tsdb_script)
	cat tsdb.tmp >"$(distdir)/debian/usr/share/opentsdb/bin/tsdb"
	rm -f tsdb.tmp
	chmod 755 $(distdir)/debian/usr/share/opentsdb/bin/tsdb
	cp $(top_srcdir)/build-aux/deb/control/* $(distdir)/debian/DEBIAN
	sed -e "s:@version[@]:$(PACKAGE_VERSION):g" \
	     $(distdir)/debian/DEBIAN/control >$(distdir)/debian/DEBIAN/control.tmp
	mv $(distdir)/debian/DEBIAN/control.tmp $(distdir)/debian/DEBIAN/control
	chmod 755 $(distdir)/debian/DEBIAN/*
	cp $(top_srcdir)/build-aux/deb/init.d/opentsdb $(distdir)/debian/etc/init.d
	cp $(jar) $(distdir)/debian/usr/share/opentsdb/lib
	cp -r staticroot/favicon.ico $(distdir)/debian/usr/share/opentsdb/static
	cp -r gwt/queryui/* $(distdir)/debian/usr/share/opentsdb/static
	`for dep_jar in $(tsdb_DEPS); do cp $$dep_jar \
	     $(distdir)/debian/usr/share/opentsdb/lib; done;`
	cp $(top_srcdir)/tools/* $(distdir)/debian/usr/share/opentsdb/tools
	dpkg -b $(distdir)/debian $(distdir)/opentsdb-$(PACKAGE_VERSION)_all.deb

.PHONY: jar doc check gwtc gwtdev printdeps staticroot gwttsd rpm
include third_party/include.mk
