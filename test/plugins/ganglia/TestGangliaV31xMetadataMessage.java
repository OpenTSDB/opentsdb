// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
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
package net.opentsdb.plugins.ganglia;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import info.ganglia.gmetric4j.xdr.v31x.Ganglia_msg_formats;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrDecodingStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


/** Tests {@link GangliaV31xMetadataMessage}. */
public class TestGangliaV31xMetadataMessage {

  @Mock private XdrDecodingStream xdr_decoding;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCanDecode() {
    assertTrue(GangliaV31xMetadataMessage.canDecode(
        Ganglia_msg_formats.gmetadata_full));
    assertTrue(GangliaV31xMetadataMessage.canDecode(
        Ganglia_msg_formats.gmetadata_request));
    for (int id = Ganglia_msg_formats.gmetadata_full + 1;
        id < Ganglia_msg_formats.gmetadata_request; ++id) {
      assertFalse("id=" + id, GangliaV31xMetadataMessage.canDecode(id));
    }
  }

  @Test
  public void testDecode() throws OncRpcException, IOException {
    GangliaV31xMetadataMessage msg =
        GangliaV31xMetadataMessage.decode(xdr_decoding);
    assertSame(GangliaMessage.ProtocolVersion.V31x, msg.protocolVersion());
    assertFalse(msg.hasError());
    assertTrue(msg instanceof GangliaMessage.EmptyMessage);
    assertTrue(msg.isMetadata());
  }
}
