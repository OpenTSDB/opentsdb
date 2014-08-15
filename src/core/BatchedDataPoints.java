// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.stumbleupon.async.Deferred;

import org.hbase.async.Bytes;

import net.opentsdb.meta.Annotation;

/**
 * Receives new data points and stores them in compacted form. No points are written until
 * {@code flushNow} is called. This ensures that true batch dynamics can be leveraged. This
 * implementation will allow an entire hours worth of data to be written in a single transaction to
 * the data table.
 */
final class BatchedDataPoints implements WritableDataPoints {

	/**
	 * The {@code TSDB} instance we belong to.
	 */
	private final TSDB tsdb;

	/**
	 * The row key. 3 bytes for the metric name, 4 bytes for the base timestamp, 6 bytes per tag (3
	 * for the name, 3 for the value).
	 */
	private byte[] rowKey;

	/**
	 * Track the last timestamp written for this series.
	 */
	private long lastTimestamp;

	/**
	 * Number of data points in this row.
	 */
	private short size = 0;

	/**
	 * Storage of the compacted qualifier.
	 */
	private byte[] batchedQualifier = new byte[Const.MAX_TIMESPAN * 4];

	/**
	 * Storage of the compacted value.
	 */
	private byte[] batchedValue = new byte[Const.MAX_TIMESPAN * 8];

	/**
	 * Track the index position where the next qualifier gets written.
	 */
	private int qualifierIndex = 0;

	/**
	 * Track the index position where the next value gets written.
	 */
	private int valueIndex = 0;

	/**
	 * Track the base time for this batch of points.
	 */
	private long baseTime;

	/**
	 * Constructor.
	 *
	 * @param tsdb The TSDB we belong to.
	 */
	BatchedDataPoints(final TSDB tsdb, final String metric, final Map<String, String> tags) {
		this.tsdb = tsdb;
		setSeries(metric, tags);
	}

	/**
	 * Sets the metric name and tags of this batch. This method only need be called if there is a
	 * desire to reuse the data structure after the data has been flushed. This will reset all
	 * cached information in this data structure.
	 *
	 * @throws IllegalArgumentException if the metric name is empty or contains illegal characters.
	 * @throws IllegalArgumentException if the tags list is empty or one of the elements contains
	 * illegal characters.
	 */
	@Override
	public void setSeries(final String metric, final Map<String, String> tags) {
		IncomingDataPoints.checkMetricAndTags(metric, tags);
		try {
			rowKey = IncomingDataPoints.rowKeyTemplate(tsdb, metric, tags);
			reset();
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException("Should never happen", e);
		}
	}

	private void reset() {
		size = 0;
		qualifierIndex = 0;
		valueIndex = 0;
		baseTime = Long.MIN_VALUE;
		lastTimestamp = Long.MIN_VALUE;
	}

	/**
	 * A copy of the values is created and sent with a put request. A reset is initialized which
	 * makes this data structure ready to be reused for the same metric and tags but for a different
	 * hour of data.
	 *
	 * @return {@inheritDoc}
	 */
	@Override
	public Deferred<Object> persist() {
		final byte[] q = Arrays.copyOfRange(batchedQualifier, 0, qualifierIndex);
		final byte[] v = Arrays.copyOfRange(batchedValue, 0, valueIndex);
		final byte[] r = Arrays.copyOfRange(rowKey, 0, rowKey.length);
		reset();
		return tsdb.put(r, q, v);
	}

	@Override
	public void setBufferingTime(short time) {
		// does nothing
	}

	@Override
	public void setBatchImport(boolean batchornot) {
		// does nothing
	}

	@Override
	public Deferred<Object> addPoint(final long timestamp, final long value) {
		final byte[] v;
		if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
			v = new byte[] {(byte) value};
		}
		else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
			v = Bytes.fromShort((short) value);
		}
		else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
			v = Bytes.fromInt((int) value);
		}
		else {
			v = Bytes.fromLong(value);
		}
		final short flags = (short) (v.length - 1);  // Just the length.
		return addPointInternal(timestamp, v, flags);
	}

	@Override
	public Deferred<Object> addPoint(final long timestamp, final float value) {
		if (Float.isNaN(value) || Float.isInfinite(value)) {
			throw new IllegalArgumentException("value is NaN or Infinite: " + value
					+ " for timestamp=" + timestamp);
		}
		final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
		return addPointInternal(timestamp, Bytes.fromInt(Float.floatToRawIntBits(value)), flags);
	}

	/**
	 * Implements {@link #addPoint} by storing a value with a specific flag.
	 *
	 * @param timestamp The timestamp to associate with the value.
	 * @param value The value to store.
	 * @param flags Flags to store in the qualifier (size and type of the data point).
	 */
	private Deferred<Object> addPointInternal(final long timestamp, final byte[] value, final short flags)
			throws IllegalDataException {
		final boolean ms_timestamp = (timestamp & Const.SECOND_MASK) != 0;

		// we only accept unix epoch timestamps in seconds or milliseconds
		if (timestamp < 0 || (ms_timestamp && timestamp > 9999999999999L)) {
			throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
					+ " timestamp=" + timestamp
					+ " when trying to add value=" + Arrays.toString(value) + " to " + this);
		}

		// always maintain lastTimestamp in milliseconds
		if ((ms_timestamp ? timestamp : timestamp * 1000) <= lastTimestamp) {
			throw new IllegalArgumentException("New timestamp=" + timestamp
					+ " is less than or equal to previous=" + lastTimestamp
					+ " when trying to add value=" + Arrays.toString(value)
					+ " to " + this);
		}
		lastTimestamp = (ms_timestamp ? timestamp : timestamp * 1000);

		long incomingBaseTime;
		if (ms_timestamp) {
			// drop the ms timestamp to seconds to calculate the base timestamp
			incomingBaseTime = ((timestamp / 1000) - ((timestamp / 1000) % Const.MAX_TIMESPAN));
		}
		else {
			incomingBaseTime = (timestamp - (timestamp % Const.MAX_TIMESPAN));
		}

		/**
		 * First time we add a point initialize the rows timestamp.
		 */
		if (baseTime == Long.MIN_VALUE) {
			baseTime = incomingBaseTime;
			Bytes.setInt(rowKey, (int) baseTime, tsdb.metrics.width());
		}

		if (incomingBaseTime - baseTime >= Const.MAX_TIMESPAN) {
			throw new IllegalDataException("The timestamp is beyond the boundary of this batch of data points");
		}
		if (incomingBaseTime < baseTime) {
			throw new IllegalDataException("The timestamp is prior to the boundary of this batch of data points");
		}

		// Java is so stupid with its auto-promotion of int to float.
		final byte[] newQualifier = Internal.buildQualifier(timestamp, flags);

		// compact this data point with the previously compacted data points.
		append(newQualifier, value);
		size++;

		/**
		 * Satisfies the interface.
		 */
		return Deferred.fromResult((Object) null);
	}

	private void ensureCapacity(final byte[] nextQualifier, final byte[] nextValue) {
		if (qualifierIndex + nextQualifier.length >= batchedQualifier.length) {
			batchedQualifier = Arrays.copyOf(batchedQualifier, batchedQualifier.length * 2);
		}
		if (valueIndex + nextValue.length >= batchedValue.length) {
			batchedValue = Arrays.copyOf(batchedValue, batchedValue.length * 2);
		}
	}

	private void append(final byte[] nextQualifier, final byte[] nextValue) {
		ensureCapacity(nextQualifier, nextValue);

		// Now let's simply concatenate all the values together.
		System.arraycopy(nextValue, 0, batchedValue, valueIndex, nextValue.length);
		valueIndex += nextValue.length;

		// Now let's concatenate all the qualifiers together.
		System.arraycopy(nextQualifier, 0, batchedQualifier, qualifierIndex, nextQualifier.length);
		qualifierIndex += nextQualifier.length;
	}

	@Override
	public String metricName() {
		try {
			return metricNameAsync().joinUninterruptibly();
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException("Should never be here", e);
		}
	}

	@Override
	public Deferred<String> metricNameAsync() {
		if (rowKey == null) {
			throw new IllegalStateException("Instance was not properly constructed!");
		}
		final byte[] id = Arrays.copyOfRange(rowKey, 0, tsdb.metrics.width());
		return tsdb.metrics.getNameAsync(id);
	}

	@Override
	public Map<String, String> getTags() {
		try {
			return getTagsAsync().joinUninterruptibly();
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException("Should never be here", e);
		}
	}

	@Override
	public Deferred<Map<String, String>> getTagsAsync() {
		return Tags.getTagsAsync(tsdb, rowKey);
	}

	@Override
	public List<String> getAggregatedTags() {
		return Collections.emptyList();
	}

	@Override
	public Deferred<List<String>> getAggregatedTagsAsync() {
		final List<String> empty = Collections.emptyList();
		return Deferred.fromResult(empty);
	}

	@Override
	public List<String> getTSUIDs() {
		return Collections.emptyList();
	}

	@Override
	public List<Annotation> getAnnotations() {
		return null;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public int aggregatedSize() {
		return 0;
	}

	@Override
	public SeekableView iterator() {
		return new DataPointsIterator(this);
	}

	/**
	 * @throws IndexOutOfBoundsException if {@code i} is out of bounds.
	 */
	private void checkIndex(final int i) {
		if (i > size) {
			throw new IndexOutOfBoundsException("index " + i + " > " + size
					+ " for this=" + this);
		}
		if (i < 0) {
			throw new IndexOutOfBoundsException("negative index " + i
					+ " for this=" + this);
		}
	}

	private static short delta(final short qualifier) {
		return (short) ((qualifier & 0xFFFF) >>> Const.FLAG_BITS);
	}

	@Override
	public long timestamp(final int i) {
		checkIndex(i);
		return baseTime + (delta(batchedQualifier[i]) & 0xFFFF);
	}

	@Override
	public boolean isInteger(final int i) {
		checkIndex(i);
		return (batchedQualifier[i] & Const.FLAG_FLOAT) == 0x0;
	}

	@Override
	public long longValue(final int i) {
		// Don't call checkIndex(i) because isInteger(i) already calls it.
		if (isInteger(i)) {
			return batchedValue[i];
		}
		throw new ClassCastException("value #" + i + " is not a long in " + this);
	}

	@Override
	public double doubleValue(final int i) {
		// Don't call checkIndex(i) because isInteger(i) already calls it.
		if (!isInteger(i)) {
			return Float.intBitsToFloat((int) batchedValue[i]);
		}
		throw new ClassCastException("value #" + i + " is not a float in " + this);
	}

	/**
	 * Returns a human readable string representation of the object.
	 */
	@Override
	public String toString() {
		// The argument passed to StringBuilder is a pretty good estimate of the
		// length of the final string based on the row key and number of elements.
		final String metric = metricName();
		final StringBuilder buf = new StringBuilder(80 + metric.length()
				+ rowKey.length * 4 + size * 16);
		buf.append("BatchedDataPoints(")
				.append(rowKey == null ? "<null>" : Arrays.toString(rowKey))
				.append(" (metric=")
				.append(metric)
				.append("), base_time=")
				.append(baseTime)
				.append(" (")
				.append(baseTime > 0 ? new Date(baseTime * 1000) : "no date")
				.append("), [");
		for (short i = 0; i < size; i++) {
			buf.append('+').append(delta(batchedQualifier[i]));
			if (isInteger(i)) {
				buf.append(":long(").append(longValue(i));
			}
			else {
				buf.append(":float(").append(doubleValue(i));
			}
			buf.append(')');
			if (i != size - 1) {
				buf.append(", ");
			}
		}
		buf.append("])");
		return buf.toString();
	}
}
