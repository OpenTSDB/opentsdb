// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;

import com.google.gson.stream.JsonWriter;

/**
 * Writer for a JSON output stream of OpenTSDB data points
 * 
 * @author Martin Jansen <martin@divbyzero.net>
 */
public class JSONWriter {
	protected JsonWriter writer;

	public JSONWriter(final OutputStreamWriter out) throws UnsupportedEncodingException {
		writer = new JsonWriter(out);
	}

	public void start() throws IOException {
		writer.beginArray();
	}

  public void finish() throws IOException {
    writer.endArray();
}

	public void writeDataPoints(final DataPoints dp) throws IOException {
		for (final DataPoint d : dp) {
			writer.beginObject();
			writer.name("metric").value(dp.metricName());
			writer.name("timestamp").value(d.timestamp());

			if (d.isInteger()) {
				writer.name("value").value(d.longValue());
			} else {
				final double value = d.doubleValue();
				if (value != value || Double.isInfinite(value)) {
					throw new IllegalStateException("NaN or Infinity:" + value
							+ " d=" + d);
				}
				writer.name("value").value(value);
			}

			writer.name("tags");
			writer.beginArray();
			for (final Map.Entry<String, String> tag : dp.getTags().entrySet()) {
				writer.beginObject();
				writer.name(tag.getKey()).value(tag.getValue());
				writer.endObject();
			}
			writer.endArray();

			writer.endObject();
		}
	}
}
