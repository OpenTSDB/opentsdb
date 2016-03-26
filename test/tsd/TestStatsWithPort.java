// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tools.TSDPort;

public class TestStatsWithPort {
	
	static final Pattern PORT_MATCH = Pattern.compile(" port=");

	@Test
	public void testNoPort()  {
		setPortConfig(4242, false);
	}
	
	@Test
	public void testDefaultPort()  {
		setPortConfig(4242, true);
	}
	
	
	protected void doTest() {
		final List<String> lines = new ArrayList<String>(); 
		StatsCollector sc = new StatsCollector("tsd") {
	        @Override
	        public final void emit(final String line) {
	          lines.add(line);
	        }
	      };
	   sc.record("foo", -1);
		
	}
	
	protected void validateStats(final List<String> lines) {
		Pattern portMatch = Pattern.compile(" port=" + TSDPort.getTSDPort());
		for(String s: lines) {
			if(!TSDPort.isStatsWithPort()) {
				Assert.assertFalse("Stat had a port", PORT_MATCH.matcher(s).find());
			} else {
				Assert.assertTrue("Stat did not have port", portMatch.matcher(s).find());
			}
		}
	}
	  
	  
	public void setPortConfig(final Integer port, final Boolean statsWithPort) {
		try {			
			Field portField = TSDPort.class.getDeclaredField("rpcPort");
			portField.setAccessible(true);
			portField.set(null, port);
			Field statsWPortField = TSDPort.class.getDeclaredField("statsWithPort");
			statsWPortField.setAccessible(true);
			statsWPortField.set(null, statsWithPort);			
		} catch (Exception ex) {
			throw new RuntimeException("Failed to set TCPPort fields", ex);
		}
	}

}
