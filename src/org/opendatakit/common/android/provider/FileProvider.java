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

package org.opendatakit.common.android.provider;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.ParcelFileDescriptor;

/**
 * The WebKit does better if there is a content provider vending files to it.
 * This provider vends files under the Forms and Instances directories (only).
 * The url is of the form:
 * content://org.opendatakit.common.android.provider.file/appname/forms/...
 * content
 * ://org.opendatakit.common.android.provider.file/appname/instances/tableid
 * /instanceid/...
 *
 * @author mitchellsundt@gmail.com
 *
 */
public abstract class FileProvider extends ContentProvider {
  public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/vnd.opendatakit.file";

  public static String getApkPart(Context c) {
	    String pkgName = c.getApplicationInfo().packageName;
	    String trailing = pkgName.substring(pkgName.lastIndexOf('.')+1);
	    if ( trailing.equals("android") ) {
	    	pkgName = pkgName.substring(0,pkgName.lastIndexOf('.'));
	    }
	    trailing = pkgName.substring(pkgName.lastIndexOf('.')+1);
	    return trailing;
  }

  public static String getFileAuthority(Context c) {
	  return "org.opendatakit." + getApkPart(c) + ".android.provider.file";
  }

  public static Uri getContentUri(Context c) {
	  return Uri.parse("content://" + getFileAuthority(c) + "/");
  }

  public static String getFileOriginString(Context c) {
    return ContentResolver.SCHEME_CONTENT + "_" + getFileAuthority(c) + "_0";
  }

  /**
   * directories within an application that are inaccessible via the file
   * provider.
   */
  private static final List<String> INACCESSIBLE_DIRECTORIES;
  static {
    INACCESSIBLE_DIRECTORIES = new ArrayList<String>();
    INACCESSIBLE_DIRECTORIES.add("metadata"); // where the database lives...
  }

  /**
   * Internal routine that does not require that the returned File exists or is
   * of a particular type.
   *
   * @param context
   * @param uriString
   * @return File corresponding to the specified uri
   */
  private static File getAsFileObject(Context context, String uriString) {
    Uri uri = Uri.parse(uriString);
    if (!uri.getAuthority().equals(getFileAuthority(context))) {
      throw new IllegalArgumentException("Not a valid uri: " + uriString);
    }
    List<String> segments = uri.getPathSegments();
    if (segments.size() < 2) {
      throw new IllegalArgumentException("Not a valid uri: " + uriString
          + " application or subdirectory not specified.");
    }

    if (segments.get(0).contains(File.separator)) {
      throw new IllegalArgumentException("Not a valid uri: " + uriString + " invalid application.");
    }
    File f = ODKFileUtils.fromAppPath(segments.get(0));
    if (f == null || !f.exists() || !f.isDirectory()) {
      throw new IllegalArgumentException("Not a valid uri: " + uriString + " invalid application.");
    }
    f = new File(f, segments.get(1));
    if (!f.exists() || !f.isDirectory() || INACCESSIBLE_DIRECTORIES.contains(segments.get(1))) {
      throw new IllegalArgumentException("Not a valid uri: " + uriString + " invalid subdirectory.");
    }
    for (int i = 2; i < segments.size(); ++i) {
      f = new File(f, segments.get(i));
    }
    return f;
  }

  public static File getAsDirectory(Context context, String uriString) {
    File f = getAsFileObject(context, uriString);
    if (!f.exists() || !f.isDirectory()) {
      throw new IllegalArgumentException("Not a valid uri: " + uriString
          + " file does not exist or is not a valid directory.");
    }
    return f;
  }

  public static File getAsFile(Context context, String uriString) {
    File f = getAsFileObject(context, uriString);
    return f;
  }
  /**
   * The constructed URI may be invalid if it references a file that is in a
   * legacy directory or an inaccessible directory.
   *
   * Typical usage:
   *
   * File file;
   *
   * FileProvider.getAsUri(this, appName, ODKFileUtils.asUriFragment(appName, file));
   *
   * @param context
   * @param appName
   * @param uriFragment
   * @return
   */
  public static String getAsUri(Context context, String appName, String uriFragment) {
    Uri u = FileProvider.getContentUri(context);
    // we need to escape the segments.
    u = Uri.withAppendedPath(u, Uri.encode(appName));
    String[] segments = uriFragment.split("/");
    for (String s : segments) {
      u = Uri.withAppendedPath(u, Uri.encode(s));
    }
    return u.toString();
  }

  @Override
  public ParcelFileDescriptor openFile(Uri uri, String mode) throws FileNotFoundException {
    String path = uri.getPath();
    if (!uri.getAuthority().equalsIgnoreCase(getFileAuthority(getContext()))) {
      throw new FileNotFoundException("Not a valid uri: " + uri
          + " file does not exists or is not a file.");
    }

    File realFile = ODKFileUtils.fromAppPath(path);

    if (!realFile.isFile()) {
      throw new FileNotFoundException("Not a valid uri: " + uri + " is not a file.");
    }
    return ParcelFileDescriptor.open(realFile, ParcelFileDescriptor.MODE_READ_ONLY);
  }

  @Override
  public int delete(Uri uri, String selection, String[] selectionArgs) {
    return 0;
  }

  @Override
  public String getType(Uri uri) {
    return FileProvider.CONTENT_ITEM_TYPE;
  }

  @Override
  public Uri insert(Uri uri, ContentValues values) {
    return null;
  }

  @Override
  public boolean onCreate() {
    return true;
  }

  @Override
  public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
      String sortOrder) {
    return null;
  }

  @Override
  public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
    return 0;
  }

}
