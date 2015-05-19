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

import com.stumbleupon.async.Callback;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.uid.LabelId;
import org.junit.Ignore;
import org.junit.Test;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public abstract class TestSearchPlugin {
  protected SearchPlugin search;
  
  @Test
  public void indexTSMeta() {
    assertNotNull(search.indexTSMeta(new TSMeta()));
  }
  
  @Test
  public void indexTSMetaNull() {
    assertNotNull(search.indexTSMeta(null));
  }
  
  @Test
  public void indexTSMetaNullErrBack() {
    assertNotNull(search.indexTSMeta(null).addErrback(new Errback()));
  }
  
  @Test
  public void deleteTSMeta() {
    assertNotNull(search.deleteTSMeta("hello"));
  }
  
  @Test
  public void deleteTSMetaNull() {
    assertNotNull(search.deleteTSMeta(null));
  }
  
  @Test
  public void deleteTSMetaNullErrBack() {
    assertNotNull(search.deleteTSMeta(null).addErrback(new Errback()));
  }
  
  @Test
  public void indexLabelMetaNull() {
    assertNotNull(search.indexLabelMeta(null));
  }
  
  @Test
  public void IndexLabelMetaNullErrBack() {
    assertNotNull(search.indexLabelMeta(null).addErrback(new Errback()));
  }
  
  @Test
  public void deleteLabelMetaNullId() {
    assertNotNull(search.deleteLabelMeta(null, METRIC));
  }

  @Test
  public void deleteLabelMetaNullType() {
    assertNotNull(search.deleteLabelMeta(mock(LabelId.class), null));
  }
  
  @Test
  public void indexAnnotation() {
    assertNotNull(search.indexAnnotation(new Annotation()));
  }
  
  @Test
  public void indexAnnotationNull() {
    assertNotNull(search.indexAnnotation(null));
  }
  
  @Test
  public void indexAnnotationNullErrBack() {
    assertNotNull(search.indexAnnotation(null).addErrback(new Errback()));
  }
  
  @Test
  public void deleteAnnotation() {
    assertNotNull(search.deleteAnnotation(new Annotation()));
  }
  
  @Test
  public void deleteAnnotationNull() {
    assertNotNull(search.deleteAnnotation(null));
  }
  
  @Test
  public void deleteAnnotationNullErrBack() {
    assertNotNull(search.deleteAnnotation(null).addErrback(new Errback()));
  }
  
  /**
   * Helper Deferred Errback handler just to make sure the dummy plugin (and
   * hopefully implementers) use errbacks for exceptions in the proper spots
   */
  @Ignore
  final class Errback implements Callback<Object, Exception> {
    @Override
    public Object call(final Exception e) {
      assertNotNull(e);
      return new Object();
    }
  }
}
