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
package net.opentsdb.tsd;

import net.opentsdb.core.TSDB;

import com.stumbleupon.async.Deferred;
import org.junit.Ignore;

/**
 * This is a dummy HTTP plugin seralizer implementation for unit test purposes
 * @since 2.0
 */
@Ignore
public class DummyHttpSerializer extends HttpSerializer {

  public DummyHttpSerializer() {
    super();
    this.request_content_type = "application/tsdbdummy";
    this.response_content_type = "application/tsdbdummy; charset=UTF-8";
  }
  
  public DummyHttpSerializer(final HttpQuery query) {
    super(query);
    this.request_content_type = "application/tsdbdummy";
    this.response_content_type = "application/tsdbdummy; charset=UTF-8";
  }

  @Override
  public void initialize(final TSDB tsdb) {
    // nothing to do
  }
  
  @Override
  public Deferred<Object> shutdown() {
    return new Deferred<Object>();
  }

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public String shortName() {
    return "dummy";
  }

}
