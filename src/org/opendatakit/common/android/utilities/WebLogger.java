/*
 * Copyright (C) 2012-2013 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.common.android.utilities;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import android.util.Log;

/**
 * Logger that emits logs to the LOGGING_PATH and recycles them as needed.
 * Useful to separate out ODK log entries from the overall logging stream,
 * especially on heavily logged 4.x systems.
 *
 * @author mitchellsundt@gmail.com
 */
public class WebLogger {
  private static long MILLISECONDS_DAY = 86400000L;

  private static final int ASSERT = 1;
  private static final int VERBOSE = 2;
  private static final int DEBUG = 3;
  private static final int INFO = 4;
  private static final int WARN = 5;
  private static final int ERROR = 6;
  private static final int SUCCESS = 7;
  private static final int TIP = 8;

  private static long lastStaleScan = 0L;
  private static Map<String, WebLogger> loggers = new HashMap<String, WebLogger>();

  /**
   * Instance variables
   */

  // appName under which to write log
  private final String appName;
  // dateStamp (filename) of opened stream
  private String dateStamp = null;
  // opened stream
  private OutputStreamWriter logFile = null;

  public synchronized static WebLogger getLogger(String appName) {
    WebLogger logger = loggers.get(appName);
    if (logger == null) {
      logger = new WebLogger(ODKFileUtils.getLoggingFolder(appName));
    }

    long now = System.currentTimeMillis();
    if (lastStaleScan + MILLISECONDS_DAY < now) {
      try {
        // scan for stale logs...
        String loggingPath = ODKFileUtils.getLoggingFolder(appName);
        final long distantPast = now - 30L * MILLISECONDS_DAY; // thirty days
                                                               // ago...
        File loggingDirectory = new File(loggingPath);
        loggingDirectory.mkdirs();

        File[] stale = loggingDirectory.listFiles(new FileFilter() {
          @Override
          public boolean accept(File pathname) {
            return (pathname.lastModified() < distantPast);
          }
        });

        if (stale != null) {
          for (File f : stale) {
            f.delete();
          }
        }
      } catch (Exception e) {
        // no exceptions are claimed, but since we can mount/unmount
        // the SDCard, there might be an external storage unavailable
        // exception that would otherwise percolate up.
        e.printStackTrace();
      } finally {
        // whether or not we failed, record that we did the scan.
        lastStaleScan = now;
      }
    }
    return logger;
  }

  private WebLogger(String appName) {
    this.appName = appName;
  }

  private synchronized void log(String logMsg) throws IOException {
    String curDateStamp = (new SimpleDateFormat("yyyy-MM-dd_HH", Locale.ENGLISH)).format(new Date());
    if  ( logFile == null ||
          dateStamp == null ||
          !curDateStamp.equals(dateStamp) ) {
      // the file we should log to has changed.
      // or has not yet been opened.

      if ( logFile != null ) {
        // close existing writer...
        OutputStreamWriter writer = logFile;
        logFile = null;
        try {
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      String loggingPath = ODKFileUtils.getLoggingFolder(appName);
      File loggingDirectory = new File(loggingPath);
      loggingDirectory.mkdirs();

      File f = new File(loggingPath + File.separator + curDateStamp + ".log");
      try {
        FileOutputStream fo = new FileOutputStream(f, true);
        logFile = new OutputStreamWriter(fo, "UTF-8");
        dateStamp = curDateStamp;
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        throw new IllegalStateException(e.toString());
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
        throw new IllegalStateException(e.toString());
      }
    }

    if ( logFile != null ) {
      logFile.write(logMsg + "\n");
      logFile.flush();
    }
  }

  private void log(int severity, String t, String logMsg) {
    try {
      switch (severity) {
      case ASSERT:
        Log.d(t, logMsg);
        logMsg = "A/" + t + ": " + logMsg;
        break;
      case DEBUG:
        Log.d(t, logMsg);
        logMsg = "D/" + t + ": " + logMsg;
        break;
      case ERROR:
        Log.e(t, logMsg);
        logMsg = "E/" + t + ": " + logMsg;
        break;
      case INFO:
        Log.i(t, logMsg);
        logMsg = "I/" + t + ": " + logMsg;
        break;
      case SUCCESS:
        Log.d(t, logMsg);
        logMsg = "S/" + t + ": " + logMsg;
        break;
      case VERBOSE:
        Log.d(t, logMsg);
        logMsg = "V/" + t + ": " + logMsg;
        break;
      case TIP:
        Log.d(t, logMsg);
        logMsg = "T/" + t + ": " + logMsg;
        break;
      case WARN:
        Log.w(t, logMsg);
        logMsg = "W/" + t + ": " + logMsg;
        break;
      default:
        Log.d(t, logMsg);
        logMsg = "?/" + t + ": " + logMsg;
        break;
      }
      log(logMsg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void a(String t, String logMsg) {
    log(ASSERT, t, logMsg);
  }

  public void t(String t, String logMsg) {
    log(TIP, t, logMsg);
  }

  public void v(String t, String logMsg) {
    log(VERBOSE, t, logMsg);
  }

  public void d(String t, String logMsg) {
    log(DEBUG, t, logMsg);
  }

  public void i(String t, String logMsg) {
    log(INFO, t, logMsg);
  }

  public void w(String t, String logMsg) {
    log(WARN, t, logMsg);
  }

  public void e(String t, String logMsg) {
    log(ERROR, t, logMsg);
  }

  public void s(String t, String logMsg) {
    log(SUCCESS, t, logMsg);
  }

}
