// This file is part of OpenTSDB.
// Copyright (C) 2011-2021  The OpenTSDB Authors.
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

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.jboss.netty.channel.Channel;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.graph.Plot;

import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ GraphHandler.class, HttpQuery.class, Plot.class })
public final class TestGraphHandler {

  private final static Method sm;
  static {
    try {
      sm = GraphHandler.class.getDeclaredMethod("staleCacheFile",
          HttpQuery.class, long.class, long.class, File.class);
      sm.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Test  
  public void setYRangeParams() throws Exception {
    assertPlotParam("yrange","[0:1]");
    assertPlotParam("yrange", "[:]");
    assertPlotParam("yrange", "[:0]");
    assertPlotParam("yrange", "[:42]");
    assertPlotParam("yrange", "[:-42]");
    assertPlotParam("yrange", "[:0.8]");
    assertPlotParam("yrange", "[:-0.8]");
    assertPlotParam("yrange", "[:42.4]");
    assertPlotParam("yrange", "[:-42.4]");
    assertPlotParam("yrange", "[:4e4]");
    assertPlotParam("yrange", "[:-4e4]");
    assertPlotParam("yrange", "[:4e-4]");
    assertPlotParam("yrange", "[:-4e-4]");
    assertPlotParam("yrange", "[:4.2e4]");
    assertPlotParam("yrange", "[:-4.2e4]");
    assertPlotParam("yrange", "[0:]");
    assertPlotParam("yrange", "[5:]");
    assertPlotParam("yrange", "[-5:]");
    assertPlotParam("yrange", "[0.5:]");
    assertPlotParam("yrange", "[-0.5:]");
    assertPlotParam("yrange", "[10.5:]");
    assertPlotParam("yrange", "[-10.5:]");
    assertPlotParam("yrange", "[10e5:]");
    assertPlotParam("yrange", "[-10e5:]");
    assertPlotParam("yrange", "[10e-5:]");
    assertPlotParam("yrange", "[-10e-5:]");
    assertPlotParam("yrange", "[10.1e-5:]");
    assertPlotParam("yrange", "[-10.1e-5:]");
    assertPlotParam("yrange", "[-10.1e-5:-10.1e-6]");
    assertInvalidPlotParam("yrange", "[33:system('touch /tmp/poc.txt')]");
  }

  @Test
  public void setKeyParams() throws Exception {
    assertPlotParam("key", "out");
    assertPlotParam("key", "left");
    assertPlotParam("key", "top");
    assertPlotParam("key", "center");
    assertPlotParam("key", "right");
    assertPlotParam("key", "horiz");
    assertPlotParam("key", "box");
    assertPlotParam("key", "bottom");
    assertInvalidPlotParam("yrange", "out%20right%20top%0aset%20yrange%20[33:system(%20");
  }

  @Test
  public void setStyleParams() throws Exception {
    assertPlotParam("style", "linespoint");
    assertPlotParam("style", "points");
    assertPlotParam("style", "circles");
    assertPlotParam("style", "dots");
    assertInvalidPlotParam("style", "dots%20[33:system(%20");
  }

  @Test
  public void setLabelParams() throws Exception {
    assertPlotParam("ylabel", "This is good");
    assertPlotParam("ylabel", " and so Is this - _ yay");
    assertInvalidPlotParam("ylabel", "[33:system(%20");
    assertInvalidPlotParam("title", "[33:system(%20");
    assertInvalidPlotParam("y2label", "[33:system(%20");
  }

  @Test
  public void setColorParams() throws Exception {
    assertPlotParam("bgcolor", "x000000");
    assertPlotParam("bgcolor", "XDEADBE");
    assertPlotParam("bgcolor", "%58DEADBE");
    assertInvalidPlotParam("bgcolor", "XDEADBEF");
    assertInvalidPlotParam("bgcolor", "%5BDEADBE");

    assertPlotParam("fgcolor", "x000000");
    assertPlotParam("fgcolor", "XDEADBE");
    assertPlotParam("fgcolor", "%58DEADBE");
    assertInvalidPlotParam("fgcolor", "XDEADBEF");
    assertInvalidPlotParam("fgcolor", "%5BDEADBE");
  }

  @Test
  public void setSmoothParams() throws Exception {
    assertPlotParam("smooth", "unique");
    assertPlotParam("smooth", "frequency");
    assertPlotParam("smooth", "fnormal");
    assertPlotParam("smooth", "cumulative");
    assertPlotParam("smooth", "cnormal");
    assertPlotParam("smooth", "bins");
    assertPlotParam("smooth", "csplines");
    assertPlotParam("smooth", "acsplines");
    assertPlotParam("smooth", "mcsplines");
    assertPlotParam("smooth", "bezier");
    assertPlotParam("smooth", "sbezier");
    assertPlotParam("smooth", "unwrap");
    assertPlotParam("smooth", "zsort");
    assertInvalidPlotParam("smooth", "[33:system(%20");
  }

  @Test
  public void setFormatParams() throws Exception {
    assertPlotParam("yformat", "%25.2f");
    assertPlotParam("y2format", "%25.2f");
    assertPlotParam("xformat", "%25.2f");
    assertPlotParam("yformat", "%253.0em");
    assertPlotParam("yformat", "%253.0em%25%25");
    assertPlotParam("yformat", "%25.2f seconds");
    assertPlotParam("yformat", "%25.0f ms");
    assertInvalidPlotParam("yformat", "%252.[33:system");
  }

  @Test  // If the file doesn't exist, we don't use it, obviously.
  public void staleCacheFileDoesntExist() throws Exception {
    final File cachedfile = fakeFile("/cache/fake-file");
    // From the JDK manual: "returns 0L if the file does not exist
    // or if an I/O error occurs"
    when(cachedfile.lastModified()).thenReturn(0L);

    assertTrue("File is stale", staleCacheFile(null, 0, 10, cachedfile));

    verify(cachedfile).lastModified();  // Ensure we do a single stat() call.
  }

  @Test  // If the mtime of a file is in the future, we don't use it.
  public void staleCacheFileInTheFuture() throws Exception {
    PowerMockito.mockStatic(System.class);

    final HttpQuery query = fakeHttpQuery();
    final File cachedfile = fakeFile("/cache/fake-file");

    final long now = 1000L;
    when(System.currentTimeMillis()).thenReturn(now);
    when(cachedfile.lastModified()).thenReturn(now + 1000L);
    final long end_time = now;

    assertTrue("File is stale",
               staleCacheFile(query, end_time, 10, cachedfile));

    verify(cachedfile).lastModified();  // Ensure we do a single stat() call.
    PowerMockito.verifyStatic(); // Verify that ...
    System.currentTimeMillis();  // ... this was called only once.
  }

//  @Test  // End time in the future => OK to serve stale file up to max_age.
//  public void staleCacheFileEndTimeInFuture() throws Exception {
//    PowerMockito.mockStatic(System.class);
//
//    final HttpQuery query = fakeHttpQuery();
//    final File cachedfile = fakeFile("/cache/fake-file");
//
//    final long end_time = 20000L;
//    when(System.currentTimeMillis()).thenReturn(10000L);
//    when(cachedfile.lastModified()).thenReturn(8000L);
//
//    assertFalse("File is not more than 3s stale",
//                staleCacheFile(query, end_time, 3, cachedfile));
//    assertFalse("File is more than 2s stale",
//               staleCacheFile(query, end_time, 2, cachedfile));
//    assertTrue("File is more than 1s stale",
//               staleCacheFile(query, end_time, 1, cachedfile));
//
//    // Ensure that we stat() the file and look at the current time once per
//    // invocation of staleCacheFile().
//    verify(cachedfile, times(3)).lastModified();
//    PowerMockito.verifyStatic(times(3));
//    System.currentTimeMillis();
//  }

//  @Test  // No end time = end time is now.
//  public void staleCacheFileEndTimeIsNow() throws Exception {
//    PowerMockito.mockStatic(System.class);
//
//    final HttpQuery query = fakeHttpQuery();
//    final File cachedfile = fakeFile("/cache/fake-file");
//
//    final long now = 10000L;
//    final long end_time = now;
//    when(System.currentTimeMillis()).thenReturn(now);
//    when(cachedfile.lastModified()).thenReturn(8000L);
//
//    assertFalse("File is not more than 3s stale",
//                staleCacheFile(query, end_time, 3, cachedfile));
//    assertFalse("File is more than 2s stale",
//               staleCacheFile(query, end_time, 2, cachedfile));
//    assertTrue("File is more than 1s stale",
//               staleCacheFile(query, end_time, 1, cachedfile));
//
//    // Ensure that we stat() the file and look at the current time once per
//    // invocation of staleCacheFile().
//    verify(cachedfile, times(3)).lastModified();
//    PowerMockito.verifyStatic(times(3));
//    System.currentTimeMillis();
//  }

  @Test  // End time in the past, file's mtime predates it.
  public void staleCacheFileEndTimeInPastOlderFile() throws Exception {
    PowerMockito.mockStatic(System.class);

    final HttpQuery query = fakeHttpQuery();
    final File cachedfile = fakeFile("/cache/fake-file");

    final long end_time = 8000L;
    final long now = end_time + 2000L;
    when(System.currentTimeMillis()).thenReturn(now);
    when(cachedfile.lastModified()).thenReturn(5000L);

    assertTrue("File predates end-time and cannot be re-used",
               staleCacheFile(query, end_time, 4, cachedfile));

    verify(cachedfile).lastModified();  // Ensure we do a single stat() call.
    PowerMockito.verifyStatic(); // Verify that ...
    System.currentTimeMillis();  // ... this was called only once.
  }

//  @Test  // End time in the past, file's mtime is after it.
//  public void staleCacheFileEndTimeInPastCacheableFile() throws Exception {
//    PowerMockito.mockStatic(System.class);
//
//    final HttpQuery query = fakeHttpQuery();
//    final File cachedfile = fakeFile("/cache/fake-file");
//
//    final long end_time = 8000L;
//    final long now = end_time + 2000L;
//    when(System.currentTimeMillis()).thenReturn(now);
//    when(cachedfile.lastModified()).thenReturn(end_time + 1000L);
//
//    assertFalse("File was created after end-time and can be re-used",
//               staleCacheFile(query, end_time, 1, cachedfile));
//
//    verify(cachedfile).lastModified();  // Ensure we do a single stat() call.
//    PowerMockito.verifyStatic(); // Verify that ...
//    System.currentTimeMillis();  // ... this was called only once.
//  }

  /**
   * Helper to call private static method.
   * There's one slight difference: the {@code end_time} parameter is in
   * milliseconds here, instead of seconds.
   */
  private static boolean staleCacheFile(final HttpQuery query,
                                        final long end_time,
                                        final long max_age,
                                        final File cachedfile) throws Exception {
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.getProperty(anyString(), anyString())).thenReturn("");
    PowerMockito.when(System.getProperty(anyString())).thenReturn("");
    PowerMockito.spy(GraphHandler.class);
    PowerMockito.doReturn("").when(GraphHandler.class, "findGnuplotHelperScript");
    
    return Whitebox.<Boolean>invokeMethod(GraphHandler.class, "staleCacheFile",
                                          query, end_time / 1000, max_age,
                                          cachedfile);
    
    //return (Boolean)sm.invoke(null, query, end_time / 1000, max_age, cachedfile);
  }

  private static HttpQuery fakeHttpQuery() {
    final HttpQuery query = mock(HttpQuery.class);
    final Channel chan = NettyMocks.fakeChannel();
    when(query.channel()).thenReturn(chan);
    return query;
  }

  private static File fakeFile(final String path) {
    final File file = mock(File.class);
    when(file.getPath()).thenReturn(path);
    when(file.toString()).thenReturn(path);
    return file;
  }

  private static void assertPlotParam(String param, String value) {
    Plot plot = mock(Plot.class);
    HttpQuery query = mock(HttpQuery.class);
    Map<String, List<String>> params = Maps.newHashMap();
    when(query.getQueryString()).thenReturn(params);

    params.put(param, Lists.newArrayList(value));
    GraphHandler.setPlotParams(query, plot);
  }

  private static void assertInvalidPlotParam(String param, String value) {
    Plot plot = mock(Plot.class);
    HttpQuery query = mock(HttpQuery.class);
    Map<String, List<String>> params = Maps.newHashMap();
    when(query.getQueryString()).thenReturn(params);

    params.put(param, Lists.newArrayList(value));
    try {
      GraphHandler.setPlotParams(query, plot);
      fail("Expected BadRequestException");
    } catch (BadRequestException e) { }
  }

}
