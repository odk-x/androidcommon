/*
 * Copyright (C) 2012 The Android Open Source Project
 * Copyright (C) 2009-2013 University of Washington
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.kxml2.kdom.Node;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Environment;
import android.util.Log;

/**
 * Static methods used for common file operations.
 *
 * @author Carl Hartung (carlhartung@gmail.com)
 */
public class ODKFileUtils {
  private final static String t = "FileUtils";

  /**
   * Directories at the application name level that are inaccessible. e.g.,
   * legacy ODK Collect directories.
   */
  private static final List<String> LEGACY_DIRECTORIES;
  static {
    LEGACY_DIRECTORIES = new ArrayList<String>();
    LEGACY_DIRECTORIES.add("forms");
    LEGACY_DIRECTORIES.add("instances");
    LEGACY_DIRECTORIES.add(".cache");
    LEGACY_DIRECTORIES.add("metadata");
    LEGACY_DIRECTORIES.add("config");
  }

  public static final ObjectMapper mapper = new ObjectMapper();

  public static final String MD5_COLON_PREFIX = "md5:";

  // filename of the xforms.xml instance and bind definitions if there is one.
  // NOTE: this file may be missing if the form was not downloaded
  // via the ODK1 compatibility path.
  public static final String FILENAME_XFORMS_XML = "xforms.xml";

  // special filename
  public static final String FORMDEF_JSON_FILENAME = "formDef.json";

  // Used to validate and display valid form names.
  public static final String VALID_FILENAME = "[ _\\-A-Za-z0-9]*.x[ht]*ml";

  public static void verifyExternalStorageAvailability() {
    String cardstatus = Environment.getExternalStorageState();
    if (cardstatus.equals(Environment.MEDIA_REMOVED)
        || cardstatus.equals(Environment.MEDIA_UNMOUNTABLE)
        || cardstatus.equals(Environment.MEDIA_UNMOUNTED)
        || cardstatus.equals(Environment.MEDIA_MOUNTED_READ_ONLY)
        || cardstatus.equals(Environment.MEDIA_SHARED)) {
      RuntimeException e = new RuntimeException("ODK reports :: SDCard error: "
          + Environment.getExternalStorageState());
      throw e;
    }
  }

  public static boolean createFolder(String path) {
    boolean made = true;
    File dir = new File(path);
    if (!dir.exists()) {
      made = dir.mkdirs();
    }
    return made;
  }

  public static String getOdkFolder() {
    String path = Environment.getExternalStorageDirectory() + File.separator + "odk";
    return path;
  }

  public static boolean isValidAppName(String name) {
    return !LEGACY_DIRECTORIES.contains(name);
  }

  public static File[] getAppFolders() {
    File odk = new File(getOdkFolder());

    File[] results = odk.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        if (!pathname.isDirectory())
          return false;
        return !LEGACY_DIRECTORIES.contains(pathname.getName());
      }
    });

    return results;
  }

  public static File fromAppPath(String appPath) {
    String[] terms = appPath.split(File.separator);
    if (terms == null || terms.length < 1) {
      return null;
    }
    // exclude LEGACY_DIRECTORIES...
    if (LEGACY_DIRECTORIES.contains(terms[0])) {
      return null;
    }
    File f = new File(new File(getOdkFolder()), appPath);
    return f;
  }

  public static String toAppPath(String fullpath) {
    String path = getOdkFolder() + File.separator;
    if (fullpath.startsWith(path)) {
      String partialPath = fullpath.substring(path.length());
      String[] app = partialPath.split(File.separator);
      if (app == null || app.length < 1) {
        return null;
      }
      if (LEGACY_DIRECTORIES.contains(app[0])) {
        return null;
      }
      return partialPath;
    }
    return null;
  }

  public static String getAppFolder(String appName) {
    String path = getOdkFolder() + File.separator + appName;
    return path;
  }

  public static String getFormsFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + "forms";
    return path;
  }

  /**
   * Construct the path to the directory containing the default form (holding
   * the Common Javascript Framework).
   *
   * @param appName
   * @return
   */
  public static String getFrameworkFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + "framework";
    return path;
  }

  public static String getStaleFormsFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + "forms.old";
    return path;
  }

  public static String getStaleFrameworkFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + "framework.old";
    return path;
  }

  public static String getLoggingFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + "logging";
    return path;
  }

  public static String getMetadataFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + "metadata";
    return path;
  }

  public static String getAppCacheFolder(String appName) {
    String path = getMetadataFolder(appName) + File.separator + "appCache";
    return path;
  }

  public static String getGeoCacheFolder(String appName) {
    String path = getMetadataFolder(appName) + File.separator + "geoCache";
    return path;
  }

  public static String getWebDbFolder(String appName) {
    String path = getMetadataFolder(appName) + File.separator + "webDb";
    return path;
  }

  public static String getAndroidObbFolder(String packageName) {
    String path = Environment.getExternalStorageDirectory() + File.separator +
        "Android" + File.separator + "obb" + File.separator + packageName;
    return path;
  }

  public static String getRelativeFormPath(String appName, File formDefFile) {

    // compute FORM_PATH...
    // we need to do this relative to the AppFolder, as the
    // common javascript framework (default form) is no longer
    // in the forms directory, but in the Framework folder.
    File parentDir = new File(ODKFileUtils.getAppFolder(appName));

    ArrayList<String> pathElements = new ArrayList<String>();

    File f = formDefFile.getParentFile();

    while (f != null && !f.equals(parentDir)) {
      pathElements.add(f.getName());
      f = f.getParentFile();
    }

    StringBuilder b = new StringBuilder();
    if (f == null) {
      // OK we have had to go all the way up to /
      b.append("..");
      b.append(File.separator); // to get from ./framework/defaultDir to appName
      b.append("..");
      b.append(File.separator);

      while (parentDir != null) {
        b.append("..");
        b.append(File.separator);
        parentDir = parentDir.getParentFile();
      }

    } else {
      b.append("..");
      b.append(File.separator); // to get from ./framework/defaultDir to appName
      b.append("..");
      b.append(File.separator);
    }

    for (int i = pathElements.size() - 1; i >= 0; --i) {
      String element = pathElements.get(i);
      b.append(element);
      b.append(File.separator);
    }
    return b.toString();
  }

  public static String getInstanceFolder(String appName, String tableId, String instanceId) {
    if (instanceId == null || tableId == null) {
      String path = getAppFolder(appName) + File.separator + "instances.undef";
      return path;
    } else {
      String instanceFolder = instanceId.replaceAll("[\\p{Punct}\\p{Space}]", "_");
      String tableFolder = tableId.replaceAll("[\\p{Punct}\\p{Space}]", "_");

      String path = getAppFolder(appName) + File.separator + "instances" + File.separator
          + tableFolder + File.separator + instanceFolder;

      File f = new File(path);
      f.mkdirs();
      return f.getAbsolutePath();
    }
  }

  public static byte[] getFileAsBytes(File file) {
    byte[] bytes = null;
    InputStream is = null;
    try {
      is = new FileInputStream(file);

      // Get the size of the file
      long length = file.length();
      if (length > Integer.MAX_VALUE) {
        Log.e(t, "File " + file.getName() + "is too large");
        return null;
      }

      // Create the byte array to hold the data
      bytes = new byte[(int) length];

      // Read in the bytes
      int offset = 0;
      int read = 0;
      try {
        while (offset < bytes.length && read >= 0) {
          read = is.read(bytes, offset, bytes.length - offset);
          offset += read;
        }
      } catch (IOException e) {
        Log.e(t, "Cannot read " + file.getName());
        e.printStackTrace();
        return null;
      }

      // Ensure all the bytes have been read in
      if (offset < bytes.length) {
        try {
          throw new IOException("Could not completely read file " + file.getName());
        } catch (IOException e) {
          e.printStackTrace();
          return null;
        }
      }

      return bytes;

    } catch (FileNotFoundException e) {
      Log.e(t, "Cannot find " + file.getName());
      e.printStackTrace();
      return null;

    } finally {
      // Close the input stream
      try {
        is.close();
      } catch (IOException e) {
        Log.e(t, "Cannot close input stream for " + file.getName());
        e.printStackTrace();
        return null;
      }
    }
  }

  public static String getMd5Hash(File file) {
    return MD5_COLON_PREFIX + getNakedMd5Hash(file);
  }

  public static String getNakedMd5Hash(File file) {
    try {
      // CTS (6/15/2010) : stream file through digest instead of handing
      // it the byte[]
      MessageDigest md = MessageDigest.getInstance("MD5");
      int chunkSize = 256;

      byte[] chunk = new byte[chunkSize];

      // Get the size of the file
      long lLength = file.length();

      if (lLength > Integer.MAX_VALUE) {
        Log.e(t, "File " + file.getName() + "is too large");
        return null;
      }

      int length = (int) lLength;

      InputStream is = null;
      is = new FileInputStream(file);

      int l = 0;
      for (l = 0; l + chunkSize < length; l += chunkSize) {
        is.read(chunk, 0, chunkSize);
        md.update(chunk, 0, chunkSize);
      }

      int remaining = length - l;
      if (remaining > 0) {
        is.read(chunk, 0, remaining);
        md.update(chunk, 0, remaining);
      }
      byte[] messageDigest = md.digest();

      BigInteger number = new BigInteger(1, messageDigest);
      String md5 = number.toString(16);
      while (md5.length() < 32)
        md5 = "0" + md5;
      is.close();
      return md5;

    } catch (NoSuchAlgorithmException e) {
      Log.e("MD5", e.getMessage());
      return null;

    } catch (FileNotFoundException e) {
      Log.e("No Cache File", e.getMessage());
      return null;
    } catch (IOException e) {
      Log.e("Problem reading from file", e.getMessage());
      return null;
    }

  }

  public static String getNakedMd5Hash(String contents) {
    try {
      // CTS (6/15/2010) : stream file through digest instead of handing
      // it the byte[]
      MessageDigest md = MessageDigest.getInstance("MD5");
      int chunkSize = 256;

      byte[] chunk = new byte[chunkSize];

      // Get the size of the file
      long lLength = contents.length();

      if (lLength > Integer.MAX_VALUE) {
        Log.e(t, "Contents is too large");
        return null;
      }

      int length = (int) lLength;

      InputStream is = null;
      is = new ByteArrayInputStream(contents.getBytes("UTF-8"));

      int l = 0;
      for (l = 0; l + chunkSize < length; l += chunkSize) {
        is.read(chunk, 0, chunkSize);
        md.update(chunk, 0, chunkSize);
      }

      int remaining = length - l;
      if (remaining > 0) {
        is.read(chunk, 0, remaining);
        md.update(chunk, 0, remaining);
      }
      byte[] messageDigest = md.digest();

      BigInteger number = new BigInteger(1, messageDigest);
      String md5 = number.toString(16);
      while (md5.length() < 32)
        md5 = "0" + md5;
      is.close();
      return md5;

    } catch (NoSuchAlgorithmException e) {
      Log.e("MD5", e.getMessage());
      return null;

    } catch (FileNotFoundException e) {
      Log.e("No Cache File", e.getMessage());
      return null;
    } catch (IOException e) {
      Log.e("Problem reading from file", e.getMessage());
      return null;
    }

  }

  public static Bitmap getBitmapScaledToDisplay(File f, int screenHeight, int screenWidth) {
    // Determine image size of f
    BitmapFactory.Options o = new BitmapFactory.Options();
    o.inJustDecodeBounds = true;
    BitmapFactory.decodeFile(f.getAbsolutePath(), o);

    int heightScale = o.outHeight / screenHeight;
    int widthScale = o.outWidth / screenWidth;

    // Powers of 2 work faster, sometimes, according to the doc.
    // We're just doing closest size that still fills the screen.
    int scale = Math.max(widthScale, heightScale);

    // get bitmap with scale ( < 1 is the same as 1)
    BitmapFactory.Options options = new BitmapFactory.Options();
    options.inSampleSize = scale;
    Bitmap b = BitmapFactory.decodeFile(f.getAbsolutePath(), options);
    if (b != null) {
      Log.i(t,
          "Screen is " + screenHeight + "x" + screenWidth + ".  Image has been scaled down by "
              + scale + " to " + b.getHeight() + "x" + b.getWidth());
    }
    return b;
  }

  public static String getXMLText(Node n, boolean trim) {
    return (n.getChildCount() == 0 ? null : getXMLText(n, 0, trim));
  }

  /**
   * reads all subsequent text nodes and returns the combined string needed
   * because escape sequences are parsed into consecutive text nodes e.g.
   * "abc&amp;123" --> (abc)(&)(123)
   **/
  public static String getXMLText(Node node, int i, boolean trim) {
    StringBuffer strBuff = null;

    String text = node.getText(i);
    if (text == null)
      return null;

    for (i++; i < node.getChildCount() && node.getType(i) == Node.TEXT; i++) {
      if (strBuff == null)
        strBuff = new StringBuffer(text);

      strBuff.append(node.getText(i));
    }
    if (strBuff != null)
      text = strBuff.toString();

    if (trim)
      text = text.trim();

    return text;
  }

  /**
   * Copyright (C) 2012 The Android Open Source Project
   *
   * Licensed under the Apache License, Version 2.0 (the "License"); you may not
   * use this file except in compliance with the License. You may obtain a copy
   * of the License at
   *
   * http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
   * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
   * License for the specific language governing permissions and limitations
   * under the License.
   *
   * Returns the file name (without full path) for an Expansion APK file from
   * the given context.
   *
   * Taken verbatim from the Android SDK:
   * extras/google/play_apk_expansion/downloader_library/src
   * com.google.android.vending.expansion.downloader.Helpers.java
   *
   * @param c
   *          the context
   * @param mainFile
   *          true for main file, false for patch file
   * @param versionCode
   *          the version of the file
   * @return String the file name of the expansion file
   */
  public static String getExpansionAPKFileName(Context c, boolean mainFile, int versionCode) {
    return (mainFile ? "main." : "patch.") + versionCode + "." + c.getPackageName() + ".obb";
  }

}
