package net.opentsdb.examples;

import java.io.IOException;

import net.opentsdb.core.WritableDataPoints;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * An ErrBack specifically made for the case when we want to retry an addPoint() call when an Exception is returned to
 * the Deferred result.  This implementation requires a maxNumRetries input.  If the call to addPoint fails after this 
 * many attempts, we will give up, finally returning an IOException.  This is likely to happen when the connection to 
 * HBase is lost. 
 *
 */
public class RetryErrBack implements Callback<Deferred<Object>, Exception> {

	private final WritableDataPoints wdp;
	private final long timestamp;
	private final long value;
	private final int maxNumRetries;
	private int numRetries;
	
	
	
	/**
	 * @param wdp
	 * @param timestamp
	 * @param value
	 * @param maxNumRetries
	 * @param numRetries
	 */
	public RetryErrBack(WritableDataPoints wdp, long timestamp, long value, int maxNumRetries, int numRetries) {
		this.wdp = wdp;
		this.timestamp = timestamp;
		this.value = value;
		this.maxNumRetries = maxNumRetries;
		this.numRetries = numRetries;
	}



	@Override
	public Deferred<Object> call(Exception e) throws Exception {
		Deferred<Object> def;
		if (numRetries < maxNumRetries) {
			System.err.println("Retrying addPoint because Deferred returned an exception!!");
			numRetries++;
			try {
				Thread.sleep(1 * 1000); // in millis
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			def = wdp.addPoint(timestamp, value);
			def.addErrback(new RetryErrBack(wdp, timestamp, value, maxNumRetries, numRetries));
		} else {
			def = Deferred.fromError(new IOException("addPoint for datum: timestamp=" + timestamp + ", value=" + value 
					+ " failed after " + maxNumRetries + " attempts."));
		}
		return def;
	}
}
