// This file is part of OpenTSDB.
// Copyright (C) 2011-2014  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;

/**
* Accumulates byte buffers, analogous to a StringBuffer.
*/
class ByteBufferList {

  private static class BufferSegment {
    public final byte[] buf;
    public final int offset;
    public final int len;

    public BufferSegment(byte[] buf, int offset, int len) {
      this.buf = buf;
      this.offset = offset;
      this.len = len;
    }
  }

  private final ArrayList<BufferSegment> segments;
  private int totLen;

  /**
   * Create a ByteBufferList.
   *
   * @param n estimated number of segments (an overestimate is better than an underestimate)
   */
  public ByteBufferList(int n) {
    segments = new ArrayList<BufferSegment>(n);
  }

  public void add(byte[] buf, int offset, int len) {
    segments.add(new BufferSegment(buf, offset, len));
    totLen += len;
  }

  public byte[] getLastAdd() {
    if (segments.isEmpty()) {
      return null;
    }
    BufferSegment seg = segments.get(segments.size() - 1);
    return Arrays.copyOfRange(seg.buf, seg.offset, seg.offset + seg.len);
  }

  public int count() {
    return segments.size();
  }

  public byte[] toBytes(int extraSpace) {
    // special case a single entry
    if (extraSpace == 0 && segments.size() == 1) {
      BufferSegment seg = segments.get(0);
      if (seg.offset == 0 && seg.len == seg.buf.length) {
        return seg.buf;
      }
      return Arrays.copyOfRange(seg.buf, seg.offset, seg.offset + seg.len);
    }
    byte[] result = new byte[totLen + extraSpace];
    int ofs = 0;
    for (BufferSegment seg : segments) {
      System.arraycopy(seg.buf, seg.offset, result, ofs, seg.len);
      ofs += seg.len;
    }
    return result;
  }
}
