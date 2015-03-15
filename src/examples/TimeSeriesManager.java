package net.opentsdb.examples;

import java.util.HashMap;
import java.util.Map;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;


/**
 * A class to manage writing multiple data points to one or more timeseries.  This is specifically for the case when
 * we may try to write two data to the same timeseries at the same time (in ms).  Rather than writing over the first
 * point though, what we'd like is to "bump" the second point later (in time) by 1 ms.  That way, it will be as close
 * as possible to its correct time, but we won't lose the point that was written first.
 * More explicitly, if we recieve two data, d1, and d2 both at time=t1.  Then d1 will be written to t1 and d2 will be 
 * written to t1+1, where t1 is given in milliseconds.  
 *
 */
public class TimeSeriesManager {

	private final TSDB tsdb;
	private final boolean batchImport;
	private final short bufferingTime;
	private HashMap<String, WritableDataPoints> wdpPool;
	

	/**
	 * 
	 * @param tsdb TSDB object
	 * @param batchImport Whether to set batchImport on our instantiation of WritableDataPoints 
	 * @param bufferingTime What buffering time to use if batchImport is true.
	 */
	public TimeSeriesManager(TSDB tsdb, boolean batchImport, short bufferingTime) {
		this.tsdb = tsdb;
		this.batchImport = batchImport;
		this.bufferingTime = bufferingTime;
		this.wdpPool = new HashMap<String, WritableDataPoints>();
	}

	/**
	 * Add a point to the input metric for the input tags.  Metric + tags define the timeseries.
	 * Internally, this will first check to see if we already have an instance of WritableDataPoints for the desired
	 * timeseries.  If so it will reuse it, otherwise it will create a new one and cache it for later use.  
	 * @param metricName
	 * @param tags
	 * @param timestamp
	 * @param value
	 * @return
	 * @throws InterruptedException
	 * @throws Exception
	 */
	public Deferred<Object> addPoint(String metricName, Map<String, String> tags, long timestamp, long value) {
		String id = makeId(metricName, tags);
		WritableDataPoints wdp;
		if (wdpPool.containsKey(id) ) {
			wdp = wdpPool.get(id);
		} else {
			wdp = tsdb.newDataPoints();
			wdp.setBatchImport(batchImport);
			wdp.setBufferingTime(bufferingTime);
			wdp.setSeries(metricName, tags);
			wdpPool.put(id, wdp);
		}
		
		return retryAddPointUntilSuccess(wdp, timestamp, value);
	}

	/**
	 * Will try to write the point at the input timestamp.  However, if an IllegalArgumentException is thrown, as will
	 * be the case when there already was a data point written to that timeseries at that specific time, this will add
	 * 1 to the time and try again until it succeeds. 
	 * @param wdp
	 * @param timestamp
	 * @param value
	 * @return
	 * @throws InterruptedException
	 * @throws Exception
	 */
	private Deferred<Object> retryAddPointUntilSuccess(final WritableDataPoints wdp, final long timestamp, 
			final long value) {
		Deferred<Object> deferred = null;
		
		boolean accepted = false;
		for (long t = timestamp; !accepted ; t++) {
			try {
				deferred = wdp.addPoint(t, value);
				
				deferred.addErrback(new RetryErrBack(wdp, timestamp, value, 10, 0));
				
				deferred.addCallback(new Callback<Object, Object>() {
						public Object call(Object results) {
							if (results == null) {
								// log success to debug
							} else {
								if (results.toString().equals("MultiActionSuccess")) {
									// log to debug
								} else {
									// log to error
									System.err.println(">>>>>>>>>>>" + results.getClass() + ">>>>>>>>>>>");
								}
							}
							return null;
						}
				});
				accepted = true;
				
			} catch (IllegalArgumentException iae) {
//				System.err.println("Tried to write on the same ms. Bumping to 1 ms later.");
			}
		}
		return deferred;
	}
	
	private String makeId(String metricName, Map<String, String> tags) {
		return metricName + tags.toString();
	}

}
