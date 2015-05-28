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

import java.io.IOException;

import info.ganglia.gmetric4j.xdr.v31x.Ganglia_metadata_msg;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_msg_formats;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrDecodingStream;


/**
 * wrapper of a {@link Ganglia_metadata_msg}. We do not support it, so it is
 * always an empty message.
 */
public class GangliaV31xMetadataMessage extends GangliaMessage.EmptyMessage {

  /** A static empty message. */
  private static GangliaV31xMetadataMessage EMPTY_V31_METADATA_MESSAGE =
      new GangliaV31xMetadataMessage();

  @Override
  ProtocolVersion protocolVersion() {
    return ProtocolVersion.V31x;
  }

  @Override
  boolean hasError() {
    // We don't process metadata and assume every metadata message is good.
    return false;
  }

  @Override
  boolean isMetadata() {
    return true;
  }

  /** @return true if the message id is one of v31x metadata messages. */
  static boolean canDecode(int id) {
    return Ganglia_msg_formats.gmetadata_full == id ||
        Ganglia_msg_formats.gmetadata_request == id;
  }

  /**
   * We do not support Ganglia_metadata_msg yet, and always return an empty
   * message.
   * @param xdr XDR stream to decode a Ganglia v31x metadata message
   * @return A {@link GangliaV31xMetadataMessage} instance.
   * @throws OncRpcException
   * @throws IOException
   */
  static GangliaV31xMetadataMessage decode(final XdrDecodingStream xdr)
      throws OncRpcException, IOException {
    // TODO: Support metadata.
    return EMPTY_V31_METADATA_MESSAGE;
  }
}
