// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Manages the histogram decoder singletons.
 * </p>
 * <p>
 * This manage accepts the full class name of the decoder, use reflection to create the decoder,
 * and it ensures each type of the decoder will be created only once, after that, the cached decoder
 * instance will be returned.
 * </p>
 * <p>
 * This behavior actually makes each decoder a singleton.
 * </p>
 *
 * <p>This class is thread safe</p>
 */
public class HistogramDataPointDecoderManager {

    private static final Map<String, HistogramDataPointDecoder> decoders = 
        new HashMap<String, HistogramDataPointDecoder>();

    /**
     * Return the singleton instance of the given decoder.
     * @param decoder_name The full class name of the decoder
     * @return The singleton instance of the given decoder
     *
     * @throws RuntimeException If failed to create the decoder
     */
    public static HistogramDataPointDecoder getDecoder(final String decoder_name) {
        HistogramDataPointDecoder decoder = decoders.get(decoder_name);
        if (decoder == null) {
            synchronized(decoders) {
                decoder = decoders.get(decoder_name);
                if (decoder == null) {
                    decoder = createInstance(decoder_name);
                    decoders.put(decoder_name, decoder);
                }
            }
        }

        return decoder;
    }

    private static HistogramDataPointDecoder createInstance(final String decoder_name) {
        try {
            Class c = Class.forName(decoder_name);
            return (HistogramDataPointDecoder) c.newInstance();
        } catch (Exception exp) {
            throw new RuntimeException("Failed to create the decoder instance of "
                    + decoder_name, exp);
        }
    }

    private HistogramDataPointDecoderManager() {
    }
}
