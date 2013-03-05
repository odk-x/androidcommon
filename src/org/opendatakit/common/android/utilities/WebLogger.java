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
	private OutputStreamWriter logFile = null;

	public static final int ASSERT = 1;
	public static final int VERBOSE = 2;
	public static final int DEBUG = 3;
	public static final int INFO = 4;
	public static final int WARN = 5;
	public static final int ERROR = 6;
	public static final int SUCCESS = 7;
	public static final int TIP = 8;

	private static Map<String,WebLogger> loggers = new HashMap<String, WebLogger>();

   public synchronized static WebLogger getLogger(String appName) {
     WebLogger logger = loggers.get(appName);
     if (logger == null) {
        logger = new WebLogger(ODKFileUtils.getLoggingFolder(appName));
     }
     return logger;
  }

	public WebLogger(String loggingPath) {
		long now = System.currentTimeMillis();
		final long distantPast = now - 30L * 86400000L; // thirty days ago...
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

		String datestamp = (new SimpleDateFormat("yyyy-MM-dd_HH",
				Locale.ENGLISH)).format(new Date());
		File f = new File(loggingPath + File.separator + datestamp
				+ ".log");
		try {
			FileOutputStream fo = new FileOutputStream(f, true);
			logFile = new OutputStreamWriter(fo, "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new IllegalStateException(e.toString());
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new IllegalStateException(e.toString());
		}
	}

	public synchronized void log(int severity, String t, String logMsg) {
		try {
			switch (severity) {
			case ASSERT:
				logMsg = "A/" + logMsg;
				break;
			case DEBUG:
				logMsg = "D/" + logMsg;
				break;
			case ERROR:
				Log.e(t, logMsg);
				logMsg = "E/" + logMsg;
				break;
			case INFO:
				logMsg = "I/" + logMsg;
				break;
			case SUCCESS:
				logMsg = "S/" + logMsg;
				break;
			case VERBOSE:
				logMsg = "V/" + logMsg;
				break;
			case TIP:
				logMsg = "T/" + logMsg;
				break;
			case WARN:
				Log.w(t, logMsg);
				logMsg = "W/" + logMsg;
				break;
			default:
				logMsg = "?/" + logMsg;
				break;
			}
			logFile.write(logMsg + '\n');
			logFile.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
