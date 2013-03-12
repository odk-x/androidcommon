/*
 * Copyright (C) 2007 The Android Open Source Project
 * Copyright (C) 2011-2013 University of Washington
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

package org.opendatakit.common.android.provider.impl;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opendatakit.common.android.R;
import org.opendatakit.common.android.database.DataModelDatabaseHelper;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;

/**
 *
 */
public abstract class FormsProviderImpl extends CommonContentProvider {
  static final String t = "FormsProvider";

  public abstract String getFormsAuthority();

  private static ODKFolderObserver observer = null;
  static ExecutorService executor = Executors.newFixedThreadPool(1);
  private static boolean bInitialScan = false; // set to true during first scan

  /**
   * During initialization, a pool of content providers are created.
   * We only need to fire off one initial app scan. Use this synchronized
   * method to do that one scan and to set up the single FileObserver
   * that listens for changes to the odk tree and fires off subsequent
   * scans.
   *
   * @param self
   */
  private static synchronized void doInitialAppsScan(final FormsProviderImpl self) {
    if ( !bInitialScan ) {
      // observer will start monitoring and trigger forms discovery
      try {
        bInitialScan = true;
        observer = new ODKFolderObserver(self);
      } catch ( Exception e ) {
        Log.e(t, "Exception: " + e.toString());
        bInitialScan = false;
        stopScan();
      }
    }
  }

  static synchronized void stopScan() {
    observer.stopWatching();
    bInitialScan = false;
  }

  @Override
  public boolean onCreate() {
    // fire off background thread to scan directories...
    final FormsProviderImpl self = this;
    Thread r = new Thread() {
      @Override
      public void run() {
        doInitialAppsScan(self);
      }};
    r.start();
    return true;
  }

  @Override
  public Cursor query(Uri uri, String[] projection, String where, String[] whereArgs,
      String sortOrder) {
    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1 || segments.size() > 2) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }

    String appName = segments.get(0);
    String uriFormId = ((segments.size() == 2) ? segments.get(1) : null);
    boolean isNumericId = StringUtils.isNumeric(uriFormId);

    // Modify the where clause to account for the presence of
    // a form id. Accept either:
    // (1) numeric _ID value
    // (2) string FORM_ID value.
    String whereId;
    String[] whereIdArgs;

    if (uriFormId == null) {
      whereId = where;
      whereIdArgs = whereArgs;
    } else {
      if ( TextUtils.isEmpty(where) ) {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=?";
        whereIdArgs = new String[1];
        whereIdArgs[0] = uriFormId;
      } else {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=? AND (" + where + ")";
        whereIdArgs = new String[whereArgs.length+1];
        whereIdArgs[0] = uriFormId;
        for ( int i = 0 ; i < whereArgs.length ; ++i ) {
          whereIdArgs[i+1] = whereArgs[i];
        }
      }
    }

    SQLiteDatabase db = getDbHelper(appName).getReadableDatabase();

    // Get the database and run the query
    Cursor c = db.query(DataModelDatabaseHelper.FORMS_TABLE_NAME, projection, whereId, whereIdArgs, null, null, sortOrder);

    // Tell the cursor what uri to watch, so it knows when its source data changes
    c.setNotificationUri(getContext().getContentResolver(), uri);
    return c;
  }

  @Override
  public String getType(Uri uri) {
    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1 || segments.size() > 2) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }
    String uriFormId = ((segments.size() == 2) ? segments.get(1) : null);

    if ( uriFormId == null ) {
      return FormsColumns.CONTENT_TYPE;
    } else {
      return FormsColumns.CONTENT_ITEM_TYPE;
    }
  }

  private void patchUpValues(String appName, ContentValues values) {
    // don't let users put in a manual FORM_FILE_PATH
    if (values.containsKey(FormsColumns.FORM_FILE_PATH)) {
      values.remove(FormsColumns.FORM_FILE_PATH);
    }

    // don't let users put in a manual FORM_PATH
    if (values.containsKey(FormsColumns.FORM_PATH)) {
      values.remove(FormsColumns.FORM_PATH);
    }

    // don't let users put in a manual DATE
    if (values.containsKey(FormsColumns.DATE)) {
      values.remove(FormsColumns.DATE);
    }

    // don't let users put in a manual md5 hash
    if (values.containsKey(FormsColumns.MD5_HASH)) {
      values.remove(FormsColumns.MD5_HASH);
    }

    // if we are not updating FORM_MEDIA_PATH, we don't need to recalc any
    // of the above
    if (!values.containsKey(FormsColumns.FORM_MEDIA_PATH)) {
      return;
    }

    // Normalize path...
    File mediaPath = new File(values.getAsString(FormsColumns.FORM_MEDIA_PATH));

    // require that the form directory actually exists
    if (!mediaPath.exists()) {
      throw new IllegalArgumentException(FormsColumns.FORM_MEDIA_PATH
          + " directory does not exist: " + mediaPath.getAbsolutePath());
    }

    String mediaAppName = mediaPath.getParentFile()/* forms folder */.getParentFile()
        /* app */.getName();
    if (!appName.equals(mediaAppName)) {
      throw new IllegalArgumentException(
          "Form definition is not contained within the application: " + appName);
    }

    values.put(FormsColumns.FORM_MEDIA_PATH, mediaPath.getAbsolutePath());

    // date is the last modification date of the media folder
    Long now = mediaPath.lastModified();
    values.put(FormsColumns.DATE, now);

    // require that it contain a formDef file
    File formDefFile = new File(mediaPath, ODKFileUtils.FORMDEF_JSON_FILENAME);
    if (!formDefFile.exists()) {
      throw new IllegalArgumentException(ODKFileUtils.FORMDEF_JSON_FILENAME
          + " does not exist in: " + mediaPath.getAbsolutePath());
    }

    // ODK2: FILENAME_XFORMS_XML may not exist if non-ODK1 fetch path...
    File xformsFile = new File(mediaPath, ODKFileUtils.FILENAME_XFORMS_XML);
    if (xformsFile.exists()) {
      values.put(FormsColumns.FORM_FILE_PATH, xformsFile.getAbsolutePath());
    }

    // compute FORM_PATH...
    String formPath = relativeFormDefPath(appName, formDefFile);
    values.put(FormsColumns.FORM_PATH, formPath);

    String md5;
    if (xformsFile.exists()) {
      md5 = ODKFileUtils.getMd5Hash(xformsFile);
    } else {
      md5 = "-none-";
    }
    values.put(FormsColumns.MD5_HASH, md5);
  }

  @Override
  public synchronized Uri insert(Uri uri, ContentValues initialValues) {
    List<String> segments = uri.getPathSegments();

    if (segments.size() != 1) {
      throw new IllegalArgumentException("Unknown URI (too many segments!) " + uri);
    }

    String appName = segments.get(0);

    SQLiteDatabase db = getDbHelper(appName).getWritableDatabase();

    ContentValues values;
    if (initialValues != null) {
      values = new ContentValues(initialValues);
    } else {
      values = new ContentValues();
    }

    // ODK2: require FORM_MEDIA_PATH (different behavior -- ODK1 and
    // required FORM_FILE_PATH)
    if (!values.containsKey(FormsColumns.FORM_MEDIA_PATH)) {
      throw new IllegalArgumentException(FormsColumns.FORM_MEDIA_PATH + " must be specified.");
    }

    // Normalize path...
    File mediaPath = new File(values.getAsString(FormsColumns.FORM_MEDIA_PATH));

    // require that the form directory actually exists
    if (!mediaPath.exists()) {
      throw new IllegalArgumentException(FormsColumns.FORM_MEDIA_PATH
          + " directory does not exist: " + mediaPath.getAbsolutePath());
    }

    patchUpValues(appName, values);

    if (values.containsKey(FormsColumns.DISPLAY_SUBTEXT) == false) {
      Date today = new Date();
      String ts = new SimpleDateFormat(getContext().getString(R.string.added_on_date_at_time),
          Locale.getDefault()).format(today);
      values.put(FormsColumns.DISPLAY_SUBTEXT, ts);
    }

    if (values.containsKey(FormsColumns.DISPLAY_NAME) == false) {
      values.put(FormsColumns.DISPLAY_NAME, mediaPath.getName());
    }

    // first try to see if a record with this filename already exists...
    String[] projection = { FormsColumns.FORM_ID, FormsColumns.FORM_MEDIA_PATH };
    String[] selectionArgs = { mediaPath.getAbsolutePath() };
    String selection = FormsColumns.FORM_MEDIA_PATH + "=?";
    Cursor c = null;
    try {
      c = db.query(DataModelDatabaseHelper.FORMS_TABLE_NAME, projection, selection, selectionArgs,
          null, null, null);
      if (c.getCount() > 0) {
        // already exists
        throw new SQLException("FAILED Insert into " + uri
            + " -- row already exists for form directory: " + mediaPath.getAbsolutePath());
      }
    } finally {
      if (c != null) {
        c.close();
      }
    }

    long rowId = db.insert(DataModelDatabaseHelper.FORMS_TABLE_NAME, null, values);
    if (rowId > 0) {
      Uri formUri = Uri.withAppendedPath(
          Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()),
              appName), values.getAsString(FormsColumns.FORM_ID));
      getContext().getContentResolver().notifyChange(formUri, null);
      return formUri;
    }

    throw new SQLException("Failed to insert row into " + uri);
  }

  private String relativeFormDefPath(String appName, File formDefFile) {

    // compute FORM_PATH...
    File parentDir = new File(ODKFileUtils.getFormsFolder(appName));

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
      b.append(File.separator); // to get from ./default to parentDir

      while (parentDir != null) {
        b.append("..");
        b.append(File.separator);
        parentDir = parentDir.getParentFile();
      }

    } else {
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

  private void moveDirectory(String appName, File mediaDirectory) throws IOException {
    if (mediaDirectory.getParentFile().getAbsolutePath()
        .equals(ODKFileUtils.getFormsFolder(appName))
        && mediaDirectory.exists()) {
      // it is a directory under our control -- move it to the stale forms
      // path...
      // otherwise, it is not where we will look for it, so we can ignore
      // it
      // (once the record is gone from our FormsProvider, we will not
      // accidentally
      // DiskSync and locate it).
      String rootName = mediaDirectory.getName();
      int rev = 2;
      String staleMediaPathName = ODKFileUtils.getStaleFormsFolder(appName) + File.separator
          + rootName;
      File staleMediaPath = new File(staleMediaPathName);

      while (staleMediaPath.exists()) {
        try {
          if (staleMediaPath.exists()) {
            FileUtils.deleteDirectory(staleMediaPath);
          }
          Log.i(t, "Successful delete of stale directory: " + staleMediaPathName);
        } catch (IOException ex) {
          ex.printStackTrace();
          Log.i(t, "Unable to delete stale directory: " + staleMediaPathName);
        }
        staleMediaPathName = ODKFileUtils.getFormsFolder(appName) + File.separator + rootName + "_"
            + rev;
        staleMediaPath = new File(staleMediaPathName);
        rev++;
      }
      FileUtils.moveDirectory(mediaDirectory, staleMediaPath);
    }
  }

  /**
   * This method removes the entry from the content provider, and also removes
   * any associated files. files: form.xml, [formmd5].formdef, formname
   * {directory}
   */
  @Override
  public int delete(Uri uri, String where, String[] whereArgs) {
    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1 || segments.size() > 2) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }

    String appName = segments.get(0);
    String uriFormId = ((segments.size() == 2) ? segments.get(1) : null);
    boolean isNumericId = StringUtils.isNumeric(uriFormId);

    // Modify the where clause to account for the presence of
    // a form id. Accept either:
    // (1) numeric _ID value
    // (2) string FORM_ID value.
    String whereId;
    String[] whereIdArgs;

    if (uriFormId == null) {
      whereId = where;
      whereIdArgs = whereArgs;
    } else {
      if ( TextUtils.isEmpty(where) ) {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=?";
        whereIdArgs = new String[1];
        whereIdArgs[0] = uriFormId;
      } else {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=? AND (" + where + ")";
        whereIdArgs = new String[whereArgs.length+1];
        whereIdArgs[0] = uriFormId;
        for ( int i = 0 ; i < whereArgs.length ; ++i ) {
          whereIdArgs[i+1] = whereArgs[i];
        }
      }
    }

    SQLiteDatabase db = getDbHelper(appName).getWritableDatabase();
    Cursor del = null;
    HashSet<File> mediaDirs = new HashSet<File>();
    try {
      del = this.query(uri, null, whereId, whereIdArgs, null);
      del.moveToPosition(-1);
      while (del.moveToNext()) {
        File mediaDir = new File(del.getString(del.getColumnIndex(FormsColumns.FORM_MEDIA_PATH)));
        mediaDirs.add(mediaDir);
      }
    } finally {
      if (del != null) {
        del.close();
      }
    }
    int count = db.delete(DataModelDatabaseHelper.FORMS_TABLE_NAME, whereId, whereIdArgs);

    // and attempt to move these directories to the stale forms location
    // so that they do not immediately get rescanned...

    for (File mediaDir : mediaDirs) {
      try {
        moveDirectory(appName, mediaDir);
      } catch (IOException e) {
        e.printStackTrace();
        Log.e(t, "Unable to move directory " + e.toString());
      }
    }

    getContext().getContentResolver().notifyChange(uri, null);
    return count;
  }

  static class FormIdVersion {
    final String formId;
    final String formVersion;

    FormIdVersion(String formId, String formVersion) {
      this.formId = formId;
      this.formVersion = formVersion;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FormIdVersion))
        return false;
      FormIdVersion that = (FormIdVersion) o;

      // identical if id and version matches...
      return formId.equals(that.formId)
          && ((formVersion == null) ? (that.formVersion == null)
              : (that.formVersion != null && formVersion.equals(that.formVersion)));
    }
  }

  @Override
  public int update(Uri uri, ContentValues values, String where, String[] whereArgs) {
    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1 || segments.size() > 2) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }

    String appName = segments.get(0);
    String uriFormId = ((segments.size() == 2) ? segments.get(1) : null);
    boolean isNumericId = StringUtils.isNumeric(uriFormId);

    // Modify the where clause to account for the presence of
    // a form id. Accept either:
    // (1) numeric _ID value
    // (2) string FORM_ID value.
    String whereId;
    String[] whereIdArgs;

    if (uriFormId == null) {
      whereId = where;
      whereIdArgs = whereArgs;
    } else {
      if ( TextUtils.isEmpty(where) ) {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=?";
        whereIdArgs = new String[1];
        whereIdArgs[0] = uriFormId;
      } else {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=? AND (" + where + ")";
        whereIdArgs = new String[whereArgs.length+1];
        whereIdArgs[0] = uriFormId;
        for ( int i = 0 ; i < whereArgs.length ; ++i ) {
          whereIdArgs[i+1] = whereArgs[i];
        }
      }
    }

    SQLiteDatabase db = getDbHelper(appName).getWritableDatabase();

    /*
     * First, find out what records match this query, and if they refer to two
     * or more (formId,formVersion) tuples, then be sure to remove all
     * FORM_MEDIA_PATH references. Otherwise, if they are all for the same
     * tuple, and the update specifies a FORM_MEDIA_PATH, move all the
     * non-matching directories elsewhere.
     */
    HashSet<File> mediaDirs = new HashSet<File>();
    boolean multiset = false;
    Cursor c = null;
    try {
      c = this.query(uri, null, whereId, whereIdArgs, null);

      if (c.getCount() >= 1) {
        FormIdVersion ref = null;
        c.moveToPosition(-1);
        while (c.moveToNext()) {
          String formId = c.getString(c.getColumnIndex(FormsColumns.FORM_ID));
          String formVersion = c.getString(c.getColumnIndex(FormsColumns.FORM_VERSION));
          FormIdVersion cur = new FormIdVersion(formId, formVersion);

          String mediaPath = c.getString(c.getColumnIndex(FormsColumns.FORM_MEDIA_PATH));
          if (mediaPath != null) {
            mediaDirs.add(new File(mediaPath));
          }

          if (ref != null && !ref.equals(cur)) {
            multiset = true;
            break;
          } else {
            ref = cur;
          }
        }
      }
    } finally {
      if (c != null) {
        c.close();
      }
    }

    if (multiset) {
      // don't let users manually update media path
      // we are referring to two or more (formId,formVersion) tuples.
      if (values.containsKey(FormsColumns.FORM_MEDIA_PATH)) {
        values.remove(FormsColumns.FORM_MEDIA_PATH);
      }
    } else if (values.containsKey(FormsColumns.FORM_MEDIA_PATH)) {
      // we are not a multiset and we are setting the media path
      // try to move all the existing non-matching media paths to
      // somewhere else...
      File mediaPath = new File(values.getAsString(FormsColumns.FORM_MEDIA_PATH));
      for (File altPath : mediaDirs) {
        if (!altPath.equals(mediaPath)) {
          try {
            moveDirectory(appName, altPath);
          } catch (IOException e) {
            e.printStackTrace();
            Log.e(t, "Attempt to move " + altPath.getAbsolutePath() + " failed: " + e.toString());
          }
        }
      }
      // OK. we have moved the existing form definitions elsewhere. We can
      // proceed with update...
    }

    // ensure that all values are correct and ignore some user-supplied
    // values...
    patchUpValues(appName, values);

    // Make sure that the necessary fields are all set
    if (values.containsKey(FormsColumns.DATE) == true) {
      Date today = new Date();
      String ts = new SimpleDateFormat(getContext().getString(R.string.added_on_date_at_time),
          Locale.getDefault()).format(today);
      values.put(FormsColumns.DISPLAY_SUBTEXT, ts);
    }

    // OK Finally, now do the update...

    int count = db.update(DataModelDatabaseHelper.FORMS_TABLE_NAME, values, whereId, whereIdArgs);

    getContext().getContentResolver().notifyChange(uri, null);
    return count;
  }
}
