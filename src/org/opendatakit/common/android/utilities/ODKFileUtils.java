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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
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
  private static final String ODK_FOLDER_NAME = "opendatakit";

  private static final String TABLES_FOLDER_NAME = "tables";
  public static final String FORMS_FOLDER_NAME = "forms";
  private static final String INSTANCES_FOLDER_NAME = "instances";

  private static final String METADATA_FOLDER_NAME = "metadata";
  private static final String WEB_DB_FOLDER_NAME = "webDb";
  private static final String GEO_CACHE_FOLDER_NAME = "geoCache";
  private static final String APP_CACHE_FOLDER_NAME = "appCache";

  private static final String LOGGING_FOLDER_NAME = "logging";

  private static final String FRAMEWORK_FOLDER_NAME = "framework";

  private static final String STALE_FRAMEWORK_FOLDER_NAME = "framework.old";

  private static final String STALE_FORMS_FOLDER_NAME = "forms.old";

  private final static String t = "FileUtils";

  /**
   * Directories at the application name level that are inaccessible. e.g.,
   * legacy ODK Collect directories.
   */
  private static final List<String> LEGACY_DIRECTORIES;
  static {
    LEGACY_DIRECTORIES = new ArrayList<String>();
//    LEGACY_DIRECTORIES.add(FORMS_FOLDER_NAME);
//    LEGACY_DIRECTORIES.add(INSTANCES_FOLDER_NAME);
//    LEGACY_DIRECTORIES.add(".cache");
//    LEGACY_DIRECTORIES.add(METADATA_FOLDER_NAME);
//    LEGACY_DIRECTORIES.add("config");
  }

  /**
   * Get the name of the logging folder, without a path.
   * @return
   */
  public static String getNameOfLoggingFolder() {
    return LOGGING_FOLDER_NAME;
  }

  /**
   * Get the name of the metadata folder, without a path.
   * @return
   */
  public static String getNameOfMetadataFolder() {
    return METADATA_FOLDER_NAME;
  }

  /**
   * Get the name of the framework folder, without a path.
   * @return
   */
  public static String getNameOfFrameworkFolder() {
    return FRAMEWORK_FOLDER_NAME;
  }

  /**
   * Get the name of the instances folder, without a path.
   * @return
   */
  public static String getNameOfInstancesFolder() {
    return INSTANCES_FOLDER_NAME;
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
    String path = Environment.getExternalStorageDirectory() + File.separator + ODK_FOLDER_NAME;
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

  public static void assertDirectoryStructure(String appName) {
    String[] dirs = { ODKFileUtils.getAppFolder(appName), ODKFileUtils.getTablesFolder(appName),
        ODKFileUtils.getStaleFormsFolder(appName), ODKFileUtils.getFrameworkFolder(appName),
        ODKFileUtils.getStaleFrameworkFolder(appName), ODKFileUtils.getLoggingFolder(appName),
        ODKFileUtils.getMetadataFolder(appName), ODKFileUtils.getAppCacheFolder(appName),
        ODKFileUtils.getGeoCacheFolder(appName), ODKFileUtils.getWebDbFolder(appName) };

    for (String dirName : dirs) {
      File dir = new File(dirName);
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          RuntimeException e = new RuntimeException("Cannot create directory: " + dirName);
          throw e;
        }
      } else {
        if (!dir.isDirectory()) {
          RuntimeException e = new RuntimeException(dirName + " exists, but is not a directory");
          throw e;
        }
      }
    }
  }

  public static boolean isConfiguredApp(String appName) {
    File[] files = new File(getTablesFolder(appName)).listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        if ( !pathname.isDirectory() ) {
          return false;
        }
        File forms = new File(pathname, FORMS_FOLDER_NAME);
        if ( !forms.exists() || !forms.isDirectory() ) {
          return false;
        }
        File[] formDirs = forms.listFiles(new FileFilter() {

          @Override
          public boolean accept(File formDirName) {
            return formDirName.isDirectory()
                && new File(formDirName, FORMDEF_JSON_FILENAME).exists();
          }});

        return formDirs.length != 0;
      }
    });

    return (files.length != 0);
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
        Log.w(t, "Missing file path (nothing under '" + ODK_FOLDER_NAME + "'): " + fullpath);
        return null;
      }
      if (LEGACY_DIRECTORIES.contains(app[0])) {
        Log.w(t, "File path detected as legacy directory: " + fullpath);
        return null;
      }
      return partialPath;
    } else {

      String[] parts = fullpath.split(File.separator);
      int i = 0;
      while (parts.length > i && !parts[i].equals(ODK_FOLDER_NAME)) {
        ++i;
      }
      if (i == parts.length) {
        Log.w(t, "File path is not under expected '" + ODK_FOLDER_NAME +
            "' Folder (" + path + ") conversion failed for: " + fullpath);
        return null;
      }
      int len = 0; // trailing slash
      while (i >= 0) {
        len += parts[i].length() + 1;
        --i;
      }

      String partialPath = fullpath.substring(len);
      String[] app = partialPath.split(File.separator);
      if (app == null || app.length < 1) {
        Log.w(t, "File path is not under expected '" + ODK_FOLDER_NAME +
            "' Folder (" + path + ") missing file path (nothing under '" +
            ODK_FOLDER_NAME + "'): " + fullpath);
        return null;
      }
      if (LEGACY_DIRECTORIES.contains(app[0])) {
        Log.w(t, "File path is not under expected '" + ODK_FOLDER_NAME +
            "' Folder (" + path + ") detected as legacy directory: " + fullpath);
        return null;
      }

      Log.w(t, "File path is not under expected '" + ODK_FOLDER_NAME +
            "' Folder -- remapped " + fullpath + " as: " + path + partialPath);
      return partialPath;
    }
  }

  public static String getAppFolder(String appName) {
    String path = getOdkFolder() + File.separator + appName;
    return path;
  }

  public static String getTablesFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + TABLES_FOLDER_NAME;
    return path;
  }

  public static String getTablesFolder(String appName, String tableId) {
    String path;
    if (tableId == null || tableId.length() == 0) {
      throw new IllegalArgumentException("getTablesFolder: tableId is null or the empty string!");
    } else {
      if ( !tableId.matches("^\\p{L}\\p{M}*(\\p{L}\\p{M}*|\\p{Nd}|_)+$") ) {
        throw new IllegalArgumentException(
            "getFormFolder: tableId does not begin with a letter and contain only letters, digits or underscores!");
      }
      path = getTablesFolder(appName) + File.separator + tableId;
    }
    File f = new File(path);
    f.mkdirs();
    return f.getAbsolutePath();
  }

  public static String getFormsFolder(String appName, String tableId) {
    String path = getTablesFolder(appName, tableId) + File.separator + FORMS_FOLDER_NAME;
    return path;
  }

  public static String getFormFolder(String appName, String tableId, String formId) {
    if (formId == null || formId.length() == 0) {
      throw new IllegalArgumentException("getFormFolder: formId is null or the empty string!");
    } else {
      if ( !formId.matches("^\\p{L}\\p{M}*(\\p{L}\\p{M}*|\\p{Nd}|_)+$") ) {
        throw new IllegalArgumentException(
            "getFormFolder: formId does not begin with a letter and contain only letters, digits or underscores!");
      }
      String path = getFormsFolder(appName, tableId) + File.separator + formId;
      return path;
    }
  }

  public static String getInstanceFolder(String appName, String tableId, String instanceId) {
    String path;
    if (instanceId == null || instanceId.length() == 0) {
      throw new IllegalArgumentException("getInstanceFolder: instanceId is null or the empty string!");
    } else {
      String instanceFolder = instanceId.replaceAll("[\\p{Punct}\\p{Space}]", "_");

      path = getTablesFolder(appName, tableId) + File.separator + INSTANCES_FOLDER_NAME + File.separator + instanceFolder;
    }

    File f = new File(path);
    f.mkdirs();
    return f.getAbsolutePath();
  }

  /**
   * Construct the path to the directory containing the default form (holding
   * the Common Javascript Framework).
   *
   * @param appName
   * @return
   */
  public static String getFrameworkFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + FRAMEWORK_FOLDER_NAME;
    return path;
  }

  public static String getStaleFormsFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + STALE_FORMS_FOLDER_NAME;
    return path;
  }

  public static String getStaleFrameworkFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + STALE_FRAMEWORK_FOLDER_NAME;
    return path;
  }

  public static String getLoggingFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + LOGGING_FOLDER_NAME;
    return path;
  }

  public static String getMetadataFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + METADATA_FOLDER_NAME;
    return path;
  }

  public static String getAppCacheFolder(String appName) {
    String path = getMetadataFolder(appName) + File.separator + APP_CACHE_FOLDER_NAME;
    return path;
  }

  public static String getGeoCacheFolder(String appName) {
    String path = getMetadataFolder(appName) + File.separator + GEO_CACHE_FOLDER_NAME;
    return path;
  }

  public static String getWebDbFolder(String appName) {
    String path = getMetadataFolder(appName) + File.separator + WEB_DB_FOLDER_NAME;
    return path;
  }

  public static String getAndroidObbFolder(String packageName) {
    String path = Environment.getExternalStorageDirectory() + File.separator + "Android"
        + File.separator + "obb" + File.separator + packageName;
    return path;
  }

  public static boolean isPathUnderAppName(String appName, File path) {

    File parentDir = new File(ODKFileUtils.getAppFolder(appName));

    while (path != null && !path.equals(parentDir)) {
      path = path.getParentFile();
    }

    return (path != null);
  }

  public static String extractAppNameFromPath(File path) {

    if ( path == null ) {
      return null;
    }

    File parent = path.getParentFile();
    File odkDir = new File(getOdkFolder());
    while (parent != null && !parent.equals(odkDir)) {
      path = parent;
      parent = path.getParentFile();
    }

    if ( parent == null ) {
      return null;
    } else {
      return path.getName();
    }
  }

  /**
   * Returns the relative path beginning after the getAppFolder(appName) directory.
   * The relative path does not start or end with a '/'
   *
   * @param appName
   * @param fileUnderAppName
   * @return
   */
  public static String asRelativePath(String appName, File fileUnderAppName) {
    // convert fileUnderAppName to a relative path such that if
    // we just append it to the AppFolder, we have a full path.
    File parentDir = new File(ODKFileUtils.getAppFolder(appName));

    ArrayList<String> pathElements = new ArrayList<String>();

    File f = fileUnderAppName;
    while (f != null && !f.equals(parentDir)) {
      pathElements.add(f.getName());
      f = f.getParentFile();
    }

    if (f == null) {
      throw new IllegalArgumentException("file is not located under this appName (" + appName + ")!");
    }

    StringBuilder b = new StringBuilder();
    for (int i = pathElements.size() - 1; i >= 0; --i) {
      String element = pathElements.get(i);
      b.append(element);
      if ( i != 0 ) {
        b.append(File.separator);
      }
    }
    return b.toString();

  }

  public static String asUriFragment(String appName, File fileUnderAppName) {
    String relativePath = asRelativePath( appName, fileUnderAppName);
    String[] segments = relativePath.split(File.separator);
    StringBuilder b = new StringBuilder();
    boolean first = true;
    for ( String s : segments ) {
      if ( !first ) {
        b.append("/"); // uris have forward slashes
      }
      first = false;
      b.append(s);
    }
    return b.toString();
  }

  /**
   * Convert a relative path into an application filename
   *
   * @param appName
   * @param relativePath
   * @return
   */
  public static File asAppFile(String appName, String relativePath) {
    return new File(ODKFileUtils.getAppFolder(appName) + File.separator + relativePath);
  }

  /**
   * The formPath is relative to the framework directory and is passed into
   * the WebKit to specify the form to display.
   *
   * @param appName
   * @param formDefFile
   * @return
   */
  public static String getRelativeFormPath(String appName, File formDefFile) {

    // compute FORM_PATH...
    // we need to do this relative to the AppFolder, as the
    // common javascript framework (default form) is no longer
    // in the forms directory, but in the Framework folder.

    String relativePath = asRelativePath(appName, formDefFile.getParentFile());
    // adjust for relative path from ./framework...
    relativePath = ".." + File.separator + relativePath + File.separator;
    return relativePath;
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

  /**
   * Recursively traverse the directory to find the most recently modified
   * file within it.
   *
   * @param formDir
   * @return lastModifiedDate of the most recently modified file.
   */
  public static long getMostRecentlyModifiedDate(File formDir) {
    long lastModifiedDate = formDir.lastModified();
    Iterator<File> allFiles = FileUtils.iterateFiles(formDir, null, true);
    while (allFiles.hasNext()) {
      File f = allFiles.next();
      if (f.lastModified() > lastModifiedDate) {
        lastModifiedDate = f.lastModified();
      }
    }
    return lastModifiedDate;
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
