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
package net.opentsdb.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.DynamicChannelBuffer;

/**
 * <p>Title: GnuplotInstaller</p>
 * <p>Description: Installs the gnuplot invocation shell file</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tools.GnuplotInstaller</code></p>
 */

public class GnuplotInstaller {
	  private static final boolean IS_WINDOWS =
			    System.getProperty("os.name", "").contains("Windows");
	
	/** The name of the shell file */
	public static final String GP_NAME = IS_WINDOWS ? "mygnuplot.bat" : "mygnuplot.sh";
	/** The directory where shell file will be installed */
	public static final String GP_DIR = System.getProperty("java.io.tmpdir") + File.separator + ".tsdb" + File.separator + "tsdb-gnuplot";
	/** The java/io file representing the gnuplot shell file */
	public static final File GP_FILE = new File(GP_DIR, GP_NAME);

	private GnuplotInstaller() {

	}
	
	  /**
	   * Installs the mygnuplot shell file 
	   */
	  public static void installMyGnuPlot() {
		  if(!GP_FILE.exists()) {
			  if(!GP_FILE.getParentFile().exists()) {
				  GP_FILE.getParentFile().mkdirs();
			  }
			  InputStream is = null;
			  FileOutputStream fos = null;
			  try {
				  is = GnuplotInstaller.class.getClassLoader().getResourceAsStream("shell/" + GP_NAME);
				  ChannelBuffer buff = new DynamicChannelBuffer(is.available());
				  buff.writeBytes(is, is.available());
				  is.close(); is = null;
				  fos = new FileOutputStream(GP_FILE);
				  buff.readBytes(fos, buff.readableBytes());
				  fos.close(); fos = null;
				  GP_FILE.setExecutable(true);
			  } catch (Exception ex) {
				  throw new IllegalArgumentException("Failed to install mygnuplot", ex);
			  } finally {
				  if( is!=null ) try { is.close(); } catch (Exception x) { /* No Op */ }
				  if( fos!=null ) try { fos.close(); } catch (Exception x) { /* No Op */ }
			  }
		  }
	  }
	

}
