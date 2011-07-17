// This file is part of OpenTSDB.
// Copyright (C) 2011  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.jboss.netty.channel.Channel;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ GraphHandler.class, HttpQuery.class })
public final class TestGraphHandler {

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

  @Test  // End time in the future => OK to serve stale file up to max_age.
  public void staleCacheFileEndTimeInFuture() throws Exception {
    PowerMockito.mockStatic(System.class);

    final HttpQuery query = fakeHttpQuery();
    final File cachedfile = fakeFile("/cache/fake-file");

    final long end_time = 20000L;
    when(System.currentTimeMillis()).thenReturn(10000L);
    when(cachedfile.lastModified()).thenReturn(8000L);

    assertFalse("File is not more than 3s stale",
                staleCacheFile(query, end_time, 3, cachedfile));
    assertFalse("File is more than 2s stale",
               staleCacheFile(query, end_time, 2, cachedfile));
    assertTrue("File is more than 1s stale",
               staleCacheFile(query, end_time, 1, cachedfile));

    // Ensure that we stat() the file and look at the current time once per
    // invocation of staleCacheFile().
    verify(cachedfile, times(3)).lastModified();
    PowerMockito.verifyStatic(times(3));
    System.currentTimeMillis();
  }

  @Test  // No end time = end time is now.
  public void staleCacheFileEndTimeIsNow() throws Exception {
    PowerMockito.mockStatic(System.class);

    final HttpQuery query = fakeHttpQuery();
    final File cachedfile = fakeFile("/cache/fake-file");

    final long now = 10000L;
    final long end_time = now;
    when(System.currentTimeMillis()).thenReturn(now);
    when(cachedfile.lastModified()).thenReturn(8000L);

    assertFalse("File is not more than 3s stale",
                staleCacheFile(query, end_time, 3, cachedfile));
    assertFalse("File is more than 2s stale",
               staleCacheFile(query, end_time, 2, cachedfile));
    assertTrue("File is more than 1s stale",
               staleCacheFile(query, end_time, 1, cachedfile));

    // Ensure that we stat() the file and look at the current time once per
    // invocation of staleCacheFile().
    verify(cachedfile, times(3)).lastModified();
    PowerMockito.verifyStatic(times(3));
    System.currentTimeMillis();
  }

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

  @Test  // End time in the past, file's mtime is after it.
  public void staleCacheFileEndTimeInPastCacheableFile() throws Exception {
    PowerMockito.mockStatic(System.class);

    final HttpQuery query = fakeHttpQuery();
    final File cachedfile = fakeFile("/cache/fake-file");

    final long end_time = 8000L;
    final long now = end_time + 2000L;
    when(System.currentTimeMillis()).thenReturn(now);
    when(cachedfile.lastModified()).thenReturn(end_time + 1000L);

    assertFalse("File was created after end-time and can be re-used",
               staleCacheFile(query, end_time, 1, cachedfile));

    verify(cachedfile).lastModified();  // Ensure we do a single stat() call.
    PowerMockito.verifyStatic(); // Verify that ...
    System.currentTimeMillis();  // ... this was called only once.
  }

  private static String mktime(final long millis) {
    final SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
    return fmt.format(new Date(millis));
  }

  /**
   * Helper to call private static method.
   * There's one slight difference: the {@code end_time} parameter is in
   * milliseconds here, instead of seconds.
   */
  private static boolean staleCacheFile(final HttpQuery query,
                                        final long end_time,
                                        final long max_age,
                                        final File cachedfile) throws Exception {
    return Whitebox.<Boolean>invokeMethod(GraphHandler.class, "staleCacheFile",
                                          query, end_time / 1000, max_age,
                                          cachedfile);
  }

  private static HttpQuery fakeHttpQuery() {
    final HttpQuery query = mock(HttpQuery.class);
    final Channel chan = fakeChannel();
    when(query.channel()).thenReturn(chan);
    return query;
  }

  private static Channel fakeChannel() {
    final Channel chan = mock(Channel.class);
    when(chan.toString()).thenReturn("[fake channel]");
    return chan;
  }

  private static File fakeFile(final String path) {
    final File file = mock(File.class);
    when(file.getPath()).thenReturn(path);
    when(file.toString()).thenReturn(path);
    return file;
  }

}
