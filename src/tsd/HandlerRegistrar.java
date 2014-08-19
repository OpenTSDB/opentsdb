// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;






/**
 * <p>Title: HandlerRegistrar</p>
 * <p>Description: Singleton to allow limit access to the RpcHandler to register custom handlers.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.HandlerRegistrar</code></p>
 */
public class HandlerRegistrar {
	/** The singleton instance */
	private static volatile HandlerRegistrar instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** Static class loggger */
	private static final Logger LOG = LoggerFactory.getLogger(HandlerRegistrar.class);	
	
	/** Commands we can serve on the simple, telnet-style RPC interface. */
	private static final Map<String, TelnetRpc> pendingTelnetRegistrations = new ConcurrentHashMap<String, TelnetRpc>();
	/** Commands we serve on the HTTP interface. */
	private static final Map<String, HttpRpc> pendingHttpRegistrations = new ConcurrentHashMap<String, HttpRpc>();
	
	/** CAS lock opene by the RpcHandler initing */
	private static final AtomicBoolean rpcHanlderInited = new AtomicBoolean(false);
	
	/** Reference to the actual RpcHandler's registered telnet handlers */
	private final Map<String, TelnetRpc> telnetRegistrations;
	/** Reference to the actual RpcHandler's registered http handlers */
	private final Map<String, HttpRpc> httpRegistrations;

	
	private HandlerRegistrar(final Map<String, TelnetRpc> telnetRegistrations, final Map<String, HttpRpc> httpRegistrations) {
		this.telnetRegistrations = telnetRegistrations;
		this.httpRegistrations = httpRegistrations;		
		LOG.info("HTTP Map: {}", System.identityHashCode(this.httpRegistrations));
		synch();		
	}
	
	
	private void synch() {
		synchronized(lock) {
			final Set<String> keys = new HashSet<String>();
			for(Map.Entry<String, TelnetRpc> handler: pendingTelnetRegistrations.entrySet()) {
				if(telnetRegistrations.containsKey(handler.getKey())) {
					LOG.error("The requested Telnet RPC key [" + handler.getKey() + "] has already been registered");
				} else {
					telnetRegistrations.put(handler.getKey(), handler.getValue());
					LOG.info("Registered Telnet Handler with key [{}]  --  [{}]", handler.getKey(), handler.getValue());
					keys.add(handler.getKey());
				}
			}
			for(String key: keys) {
				pendingTelnetRegistrations.remove(key);
			}
			keys.clear();
			
			
			
			for(Map.Entry<String, HttpRpc> handler: pendingHttpRegistrations.entrySet()) {
				if(httpRegistrations.containsKey(handler.getKey())) {
					LOG.error("The requested HTTP RPC key [" + handler.getKey() + "] has already been registered");
				} else {
					httpRegistrations.put(handler.getKey(), handler.getValue());
					LOG.info("Registered Telnet Handler with key [{}]  --  [{}]", handler.getKey(), handler.getValue());
					keys.add(handler.getKey());
				}
			}
			for(String key: keys) {
				pendingHttpRegistrations.remove(key);
			}
			keys.clear();

		}
	}
	
	/**
	 * Acquires the HandlerRegistrar singleton instance.
	 * Only to be called by the RpcHandler on startup.
 	 * @param telnetRegistrations Reference to the actual RpcHandler's registered telnet handlers 
	 * @param httpRegistrations Reference to the actual RpcHandler's registered http handlers
	 * @return the HandlerRegistrar singleton instance
	 */
	static HandlerRegistrar getInstance(final Map<String, TelnetRpc> telnetRegistrations, final Map<String, HttpRpc> httpRegistrations) {
		if(instance == null) {
			synchronized(lock) {
				if(instance == null) {
					instance = new HandlerRegistrar(telnetRegistrations, httpRegistrations);
				}
			}
		}
		rpcHanlderInited.set(true);
		instance.synch();
		return instance;
	}
	
	
	/**
	 * Registers a telnet rpc handler
	 * @param key The invocation key
	 * @param rpc The telnet rpc instance
	 */
	public static void registerHandler(final String key, final TelnetRpc rpc) {
		if(key==null) throw new IllegalArgumentException("The handler key was null");
		if(rpc==null) throw new IllegalArgumentException("The handler was null");
		synchronized(lock) {
			if(rpcHanlderInited.get()) {
				regTelnet(key, rpc, instance.telnetRegistrations);
				LOG.info("Registered Direct TelnetRpc at [{}] -- [{}]", key, rpc.getClass().getSimpleName());
			} else {
				regTelnet(key, rpc, pendingTelnetRegistrations);
				LOG.info("Registered Deferred TelnetRpc at [{}] -- [{}]", key, rpc.getClass().getSimpleName());
			}
		}
	}
	
	/**
	 * Registers an http rpc handler
	 * @param key The invocation key
	 * @param rpc The http rpc instance
	 */
	public static void registerHandler(final String key, final HttpRpc rpc) {
		if(key==null) throw new IllegalArgumentException("The handler key was null");
		if(rpc==null) throw new IllegalArgumentException("The handler was null");
		synchronized(lock) {
			if(rpcHanlderInited.get()) {
				regHttp(key, rpc, instance.httpRegistrations);
				LOG.info("Registered Direct HttpRpc at [{}] -- [{}]   id:({})", key, rpc.getClass().getSimpleName(), System.identityHashCode(instance.httpRegistrations));
			} else {
				regHttp(key, rpc, pendingHttpRegistrations);
				LOG.info("Registered Deferred HttpRpc at [{}] -- [{}]", key, rpc.getClass().getSimpleName());
			}				
		}
	}
	
	private static void regHttp(final String key, final HttpRpc rpc, final Map<String, HttpRpc> httpRegistrations) {
		if(!httpRegistrations.containsKey(key)) {
			synchronized(httpRegistrations) {
				if(!httpRegistrations.containsKey(key)) {
					httpRegistrations.put(key, rpc);
					LOG.info("Inner Reg - [{}] -- [{}]  id:({})", key, rpc.getClass().getSimpleName(), System.identityHashCode(httpRegistrations));
					return;
				}
			}
		}	  
		final HttpRpc registeredRpc = httpRegistrations.get(key);
		throw new IllegalStateException("The requested Http RPC key [" + key + "] has already been registered for [" + registeredRpc.getClass().getName() + "]");	  
	}

	private static void regTelnet(final String key, final TelnetRpc rpc, final Map<String, TelnetRpc> telnetRegistrations) {
		if(!telnetRegistrations.containsKey(key)) {
			synchronized(telnetRegistrations) {
				if(!telnetRegistrations.containsKey(key)) {
					telnetRegistrations.put(key, rpc);
					return;
				}
			}
		}	  
		final TelnetRpc registeredRpc = telnetRegistrations.get(key);
		throw new IllegalStateException("The requested Telnet RPC key [" + key + "] has already been registered for [" + registeredRpc.getClass().getName() + "]");	  
	}
	
	
}