// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: GnuplotInstaller</p>
 * <p>Description: Installs the gnuplot invocation shell file</p> 
 */

public class GnuplotInstaller {
    private static final boolean IS_WINDOWS =
          System.getProperty("os.name", "").contains("Windows");
    
  /** Static class logger */
  private static final Logger LOG = LoggerFactory.getLogger(GnuplotInstaller.class);    
  
  /** The name of the shell file */
  public static final String GP_BATCH_FILE_NAME = IS_WINDOWS ? "mygnuplot.bat" : "mygnuplot.sh";
  /** The name of the gnuplot executable */
  public static final String GP_NAME = IS_WINDOWS ? "gnuplot.exe" : "gnuplot";
  
  /** The directory where shell file will be installed */
  public static final String GP_DIR = System.getProperty("java.io.tmpdir") + File.separator + ".tsdb" + File.separator + "tsdb-gnuplot";
  /** The java/io file representing the gnuplot shell file */
  public static final File GP_FILE = new File(GP_DIR, GP_BATCH_FILE_NAME);
  /** Indicates if gnuplot was found on the path */
  public static final boolean FOUND_GP;
  
  static {
    boolean found = false;
    final String PATH = System.getenv("PATH");
    if(PATH!=null) {
      final String[] paths = PATH.split(File.pathSeparator);
      for(String path: paths) {
        LOG.debug("Inspecting PATH for Gnuplot Exe: [{}]", path);
        File dir = new File(path.trim());
        if(dir.exists() && dir.isDirectory()) {
          File gp = new File(dir, GP_NAME);
          if(gp.exists()) {
            found = true;
            LOG.info("Found gnuplot at [{}]", gp.getAbsolutePath());
            break;
          }
          
        }
      }
    }
    FOUND_GP = found;
    if(!found) LOG.warn("Failed to locate Gnuplot executable");
  }
  
  private GnuplotInstaller() {

  }
  
    /**
     * Installs the mygnuplot shell file 
     */
    public static void installMyGnuPlot() {
      if(!FOUND_GP) {
        LOG.warn("Skipping Gnuplot Shell Script Install since Gnuplot executable was not found");
        return;
      }
      if(!GP_FILE.exists()) {
        if(!GP_FILE.getParentFile().exists()) {
          GP_FILE.getParentFile().mkdirs();
        }
        InputStream is = null;
        FileOutputStream fos = null;
        try {
          is = GnuplotInstaller.class.getClassLoader().getResourceAsStream(GP_BATCH_FILE_NAME);
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