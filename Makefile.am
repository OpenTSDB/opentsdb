# Copyright (C) 2011  The OpenTSDB Authors.
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

ACLOCAL_AMFLAGS = -I build-aux

all-am: jar staticroot

package = net.opentsdb
spec_title = OpenTSDB
spec_vendor = StumbleUpon, Inc.
jar := tsdb-$(PACKAGE_VERSION).jar
builddata_SRC := src/BuildData.java
BUILT_SOURCES = $(builddata_SRC)
nodist_bin_SCRIPTS = tsdb
dist_noinst_SCRIPTS = src/create_table.sh
dist_pkgdata_SCRIPTS = src/mygnuplot.sh
tsdb_SRC := \
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
	src/uid/UniqueIdInterface.java

tsdb_SRC_srcdir := $(tsdb_SRC:%=$(srcdir)/%)

tsdb_DEPS := \
	$(srcdir)/third_party/hbase/hbaseasync-1.0.jar	\
	$(srcdir)/third_party/logback/logback-classic-0.9.24.jar	\
	$(srcdir)/third_party/logback/logback-core-0.9.24.jar	\
	$(srcdir)/third_party/netty/netty-3.2.3.Final.jar	\
	$(srcdir)/third_party/slf4j/jcl-over-slf4j-1.6.1.jar	\
	$(srcdir)/third_party/slf4j/log4j-over-slf4j-1.6.1.jar	\
	$(srcdir)/third_party/slf4j/slf4j-api-1.6.1.jar	\
	$(srcdir)/third_party/suasync/suasync-1.0.jar	\
	$(srcdir)/third_party/zookeeper/zookeeper-3.3.1.jar

# Install all the .jar files in pkgdatadir.
pkgdata_DATA = $(tsdb_DEPS) $(jar)

test_SRC := \
	src/core/TestTags.java	\
	src/stats/TestHistogram.java	\
	src/tsd/TestGraphHandler.java	\
	src/uid/TestNoSuchUniqueId.java	\
	src/uid/TestUniqueId.java

test_DEPS := \
	$(tsdb_DEPS) \
	$(srcdir)/third_party/javassist/javassist-3.13.GA.jar	\
	$(srcdir)/third_party/junit/junit-4.8.2.jar	\
	$(srcdir)/third_party/mockito/mockito-1.8.5.jar	\
	$(srcdir)/third_party/powermock/powermock-mockito-1.4.5.jar	\
        $(jar)

httpui_SRC := \
	src/tsd/client/DateTimeBox.java	\
	src/tsd/client/EventsHandler.java	\
	src/tsd/client/GotJsonCallback.java	\
	src/tsd/client/MetricForm.java	\
	src/tsd/client/QueryUi.java	\
	src/tsd/client/RemoteOracle.java	\
	src/tsd/client/ValidatedTextBox.java

httpui_DEPS = src/tsd/QueryUi.gwt.xml

dist_pkgdata_DATA = src/logback.xml
dist_static_DATA = src/tsd/static/favicon.ico

EXTRA_DIST = tsdb.in $(tsdb_SRC) $(test_SRC) third_party \
        $(httpui_SRC) $(httpui_DEPS) \
        bootstrap build.sh build-aux/gen_build_data.sh $(builddata_SRC)

GWT_DEV := $(srcdir)/third_party/gwt/gwt-dev-2.0.4.jar
GWT_SDK := $(srcdir)/third_party/gwt/gwt-user-2.0.4.jar
GWTC_JVM_ARGS =  # add jvmarg -Xss16M or similar if you see a StackOverflowError
GWTC_ARGS = -ea  # Additional arguments like -style PRETTY or -logLevel DEBUG

package_dir := $(subst .,/,$(package))
UNITTESTS := $(test_SRC:src/%.java=$(package_dir)/%.class)
AM_JAVACFLAGS = -Xlint -source 6
JVM_ARGS =
classes := $(tsdb_SRC:src/%.java=$(package_dir)/%.class) \
        $(builddata_SRC:src/%.java=$(package_dir)/%.class)
test_classes := $(test_SRC:src/%.java=$(package_dir)/%.class)

edit_tsdb_script := srcdir=''; test -f ./$$script.in || srcdir=$(srcdir)/; \
        sed -e "s:@pkgdatadir[@]:$$pkgdatadir:g" \
            -e "s:@abs_srcdir[@]:$$abs_srcdir:g" \
            -e "s:@abs_builddir[@]:$$abs_builddir:g" \
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

# Adjust the paths in the 'tsdb' script after it's been installed.
# This is kind of a hack, but I couldn't find a better way to adjust the paths
# in the script before it gets installed...
install-exec-hook:
	script=tsdb; pkgdatadir='$(pkgdatadir)'; abs_srcdir=''; abs_builddir=''; \
          $(edit_tsdb_script)
	cat tsdb.tmp >"$(DESTDIR)$(bindir)/tsdb"
	rm -f tsdb.tmp

$(builddata_SRC): .git/HEAD $(tsdb_SRC) $(top_srcdir)/build-aux/gen_build_data.sh
	$(MKDIR_P) `dirname "$(builddata_SRC)"`
	$(srcdir)/build-aux/gen_build_data.sh $(builddata_SRC) $(package)

jar: $(jar) $(UNITTESTS) .gwtc-stamp

JAVA_COMPILE := $(JAVAC) $(AM_JAVACFLAGS) -d $(builddir)

get_dep_classpath := `echo $(tsdb_DEPS) | tr ' ' ':'`
.javac-stamp: $(tsdb_SRC) $(tsdb_DEPS)
	@$(MKDIR_P) $(builddir)
	$(JAVA_COMPILE) -cp $(get_dep_classpath) $(tsdb_SRC_srcdir) $(builddata_SRC)
	@touch "$@"

# The GWT compiler is way too slow, that's not very Googley.  So we save the
# MD5 of the files we compile in the stamp file and everytime `make' things it
# needs to recompile the GWT code, we verify whether the code really changed
# or whether it's just a file that was touched (which happens frequently when
# using Git while rebasing and whatnot).
gwtc: .gwtc-stamp
.gwtc-stamp: $(httpui_SRC) $(httpui_DEPS)
	@$(MKDIR_P) gwt
	{ cd $(srcdir) && cat $(httpui_SRC); } | $(MD5) >"$@-t"
	cmp -s "$@" "$@-t" && exit 0; \
        $(JAVA) $(GWTC_JVM_ARGS) -cp $(GWT_DEV):$(GWT_SDK):$(srcdir)/src com.google.gwt.dev.Compiler \
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
	$(JAVA) $(GWT_DEV_ARGS) -ea -cp $(GWT_DEV):$(GWT_SDK):src com.google.gwt.dev.DevMode \
	  -startupUrl $(GWT_DEV_URL) -noserver -war gwt tsd.QueryUi

staticroot: jar .staticroot-stamp

gwttsd: staticroot
	./tsdb tsd $(DEV_TSD_ARGS)

.staticroot-stamp: $(dist_static_DATA) .gwtc-stamp
	$(MKDIR_P) $(DEV_TSD_STATICROOT)
	cp $(dist_static_DATA:%=$(srcdir)/%) $(DEV_TSD_STATICROOT)
	find -L $(DEV_TSD_STATICROOT) -type l -delete
	p=`pwd`/gwt/queryui && cd $(DEV_TSD_STATICROOT) \
	  && for i in $$p/*; do ln -s -f "$$i" || break; done
	find -L $(DEV_TSD_STATICROOT)/gwt -type f | xargs touch
	@touch .staticroot-stamp

# Install all the files from the static dir into $staticdir.
# Ideally I'd like Automake to take care of this, but right now I don't see
# how to tell it to install a bunch of files recursively for which I don't
# know ahead of time what the file names are.
install-data-local: staticroot
	@$(NORMAL_INSTALL)
	test -z "$(staticdir)" || $(MKDIR_P) "$(DESTDIR)$(staticdir)"
	@set -e; pwd; ls -lFh; cd "$(DEV_TSD_STATICROOT)"; \
          list=`find -L . ! -type d`; for p in $$list; do \
          p=$${p#./}; \
	  if test -f "$$p"; then d=; else d="$(srcdir)/"; fi; \
          dstdir=`dirname "$(DESTDIR)$(staticdir)/$$p"`; \
          if test -d "$$dstdir"; then :; else \
            echo " $(MKDIR_P) '$$dstdir'"; $(MKDIR_P) "$$dstdir"; fi; \
	  echo " $(dist_staticDATA_INSTALL) '$$d$$p' '$(DESTDIR)$(staticdir)/$$p'"; \
	  $(dist_staticDATA_INSTALL) "$$d$$p" "$(DESTDIR)$(staticdir)/$$p"; \
	done

uninstall-local:
	@$(NORMAL_UNINSTALL)
	rm -rf "$(DESTDIR)$(staticdir)"

# I don't know why Automake leaves this directory behind.
uninstall-hook:
	test ! -d "$(pkgdatadir)" || rmdir "$(pkgdatadir)"

get_runtime_dep_classpath := `echo $(test_DEPS) | tr ' ' ':'`
$(test_classes): $(jar) $(test_SRC) $(test_DEPS)
	$(JAVA_COMPILE) -cp $(get_runtime_dep_classpath) $(test_SRC:%=$(srcdir)/%)

classes_with_nested_classes := $(classes:.class=*.class)
test_classes_with_nested_classes := $(test_classes:.class=*.class)

# Little set script to make a pretty-ish banner.
BANNER := sed 's/^.*/  &  /;h;s/./=/g;p;x;p;x'
check-local: $(UNITTESTS)
	classes=`cd $(builddir) && echo $(test_classes_with_nested_classes)` && \
        success=: && cp="$(get_runtime_dep_classpath):$(builddir)" && \
        for i in $$classes; do \
          case $$i in (*[$$]*) continue;; esac; \
	  echo "Running tests for `basename $$i .class`" | $(BANNER); \
          $(JAVA) -ea $(JVM_ARGS) -cp "$$cp" org.junit.runner.JUnitCore `echo $${i%.class} | tr / .` $(ARGS) || success=false; \
        done && $$success

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

JAVADOC_DIR = api
doc: $(JAVADOC_DIR)/index.html

JDK_JAVADOC = http://download.oracle.com/javase/6/docs/api
NETTY_JAVADOC = http://docs.jboss.org/netty/3.2/api
$(JAVADOC_DIR)/index.html: $(tsdb_SRC)
	$(JAVADOC) -d $(JAVADOC_DIR) -classpath $(get_dep_classpath) \
          -link $(JDK_JAVADOC) -link $(NETTY_JAVADOC) $(tsdb_SRC_srcdir)

dist-hook:
	$(MKDIR_P) $(distdir)/.git
	echo $(git_version) >$(distdir)/.git/HEAD

mostlyclean-local:
	@rm -f .javac-stamp .gwtc-stamp* .staticroot-stamp
	rm -rf gwt staticroot
	rm -f manifest $(BUILT_SOURCES)
	rm -f $(classes_with_nested_classes) $(test_classes_with_nested_classes)
	test -d $(package_dir) || exit 0 \
	  && find $(package_dir) -depth -type d -exec rmdir {} ';' \
	  && dir=$(package_dir) && dir=$${dir%/*} \
	  && while test x"$$dir" != x"$${dir%/*}"; do \
	       rmdir "$$dir" && dir=$${dir%/*} || break; \
	     done \
	  && rmdir "$$dir"

clean-local:
	rm -f $(jar) tsdb
	rm -rf $(JAVADOC_DIR)

.PHONY: jar doc check gwtc gwtdev staticroot gwttsd
