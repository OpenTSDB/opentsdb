// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.search;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;

import com.stumbleupon.async.Deferred;

public final class DummySearchPlugin extends SearchPlugin {

  @Override
  public void initialize(TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("The TSDB object was null");
    }
    // some dummy configs to check to throw exceptions
    if (!tsdb.getConfig().hasProperty("tsd.search.DummySearchPlugin.hosts")) {
      throw new IllegalArgumentException("Missing hosts config");
    }
    if (tsdb.getConfig().getString("tsd.search.DummySearchPlugin.hosts")
        .isEmpty()) {
      throw new IllegalArgumentException("Empty Hosts config");
    }
    // throw an NFE for fun
    tsdb.getConfig().getInt("tsd.search.DummySearchPlugin.port");
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(new Object());
  }

  @Override
  public String version() {
    return "2.0.0";
  }

  @Override
  public Deferred<Object> indexTSMeta(TSMeta meta) {
    if (meta == null) {
      return Deferred.fromError(new IllegalArgumentException("Meta was null"));
    } else {
      return Deferred.fromResult(new Object());
    }
  }

  @Override
  public Deferred<Object> deleteTSMeta(String tsuid) {
    if (tsuid == null || tsuid.isEmpty()) {
      return Deferred.fromError(
          new IllegalArgumentException("tsuid was null or empty"));
    } else {
      return Deferred.fromResult(new Object());
    }
  }

  @Override
  public Deferred<Object> indexUIDMeta(UIDMeta meta) {
    if (meta == null) {
      return Deferred.fromError(new IllegalArgumentException("Meta was null"));
    } else {
      return Deferred.fromResult(new Object());
    }
  }

  @Override
  public Deferred<Object> deleteUIDMeta(UIDMeta meta) {
    if (meta == null) {
      return Deferred.fromError(new IllegalArgumentException("Meta was null"));
    } else {
      return Deferred.fromResult(new Object());
    }
  }

  @Override
  public Deferred<Object> indexAnnotation(Annotation note) {
    if (note == null) {
      return Deferred.fromError(new IllegalArgumentException("Meta was null"));
    } else {
      return Deferred.fromResult(new Object());
    }
  }

  @Override
  public Deferred<Object> deleteAnnotation(Annotation note) {
    if (note == null) {
      return Deferred.fromError(new IllegalArgumentException("Meta was null"));
    } else {
      return Deferred.fromResult(new Object());
    }
  }

  public Deferred<SearchQuery> executeQuery(final SearchQuery query) {
    if (query == null) {
      return Deferred.fromError(new IllegalArgumentException("Query was null"));
    } else {
      query.setTime(1.42F);
      query.setTotalResults(42);
      return Deferred.fromResult(query);
    }
  }
  
}
