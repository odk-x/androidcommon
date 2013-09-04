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
import java.util.Date;
import java.util.HashMap;
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
   * During initialization, a pool of content providers are created. We only
   * need to fire off one initial app scan. Use this synchronized method to do
   * that one scan and to set up the single FileObserver that listens for
   * changes to the odk tree and fires off subsequent scans.
   *
   * @param self
   */
  private static synchronized ODKFolderObserver doInitialAppsScan(final FormsProviderImpl self) {
    if (!bInitialScan) {
      // observer will start monitoring and trigger forms discovery
      try {
        observer = new ODKFolderObserver(self);
        bInitialScan = true;
      } catch (Exception e) {
        Log.e(t, "Exception: " + e.toString());
        bInitialScan = false;
        stopScan();
      }
    }
    return observer;
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
        ODKFolderObserver obs = doInitialAppsScan(self);
        obs.start(); // triggers re-evaluation of everything
      }
    };
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
      if (TextUtils.isEmpty(where)) {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=?";
        whereIdArgs = new String[1];
        whereIdArgs[0] = uriFormId;
      } else {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=? AND (" + where
            + ")";
        whereIdArgs = new String[whereArgs.length + 1];
        whereIdArgs[0] = uriFormId;
        for (int i = 0; i < whereArgs.length; ++i) {
          whereIdArgs[i + 1] = whereArgs[i];
        }
      }
    }

    // Get the database and run the query
    Cursor c = null;
    try {
      DataModelDatabaseHelper dbh = getDbHelper(getContext(), appName);
      if ( dbh == null ) {
        Log.w(t, "Unable to access database for appName " + appName);
        return null;
      }

      SQLiteDatabase db = dbh.getReadableDatabase();
      c = db.query(DataModelDatabaseHelper.FORMS_TABLE_NAME, projection, whereId, whereIdArgs,
        null, null, sortOrder);
    } catch ( Exception e ) {
      Log.w(t, "Unable to query database for appName: " + appName);
      return null;
    }

    if ( c == null ) {
      Log.w(t, "Unable to query database for appName: " + appName);
      return null;
    }
    // Tell the cursor what uri to watch, so it knows when its source data
    // changes
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

    if (uriFormId == null) {
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

    if (!ODKFileUtils.isPathUnderAppName(appName, mediaPath)) {
      throw new IllegalArgumentException(
          "Form definition is not contained within the application: " + appName);
    }

    values.put(FormsColumns.FORM_MEDIA_PATH, mediaPath.getAbsolutePath());

    // require that it contain a formDef file
    File formDefFile = new File(mediaPath, ODKFileUtils.FORMDEF_JSON_FILENAME);
    if (!formDefFile.exists()) {
      throw new IllegalArgumentException(ODKFileUtils.FORMDEF_JSON_FILENAME
          + " does not exist in: " + mediaPath.getAbsolutePath());
    }

    // date is the last modification date of the formDef file
    Long now = formDefFile.lastModified();
    values.put(FormsColumns.DATE, now);

    // ODK2: FILENAME_XFORMS_XML may not exist if non-ODK1 fetch path...
    File xformsFile = new File(mediaPath, ODKFileUtils.FILENAME_XFORMS_XML);
    if (xformsFile.exists()) {
      values.put(FormsColumns.FORM_FILE_PATH, xformsFile.getAbsolutePath());
    }

    // compute FORM_PATH...
    String formPath = ODKFileUtils.getRelativeFormPath(appName, formDefFile);
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

    DataModelDatabaseHelper dbh = getDbHelper(getContext(), appName);
    if ( dbh == null ) {
      Log.w(t, "Unable to access database for appName " + appName);
      throw new SQLException("FAILED Insert into " + uri
          + " -- unable to access metadata directory for appName: " + appName);
    }

    try {
      SQLiteDatabase db = dbh.getWritableDatabase();
      c = db.query(DataModelDatabaseHelper.FORMS_TABLE_NAME, projection, selection, selectionArgs,
          null, null, null);
      if (c == null) {
        throw new SQLException("FAILED Insert into " + uri
            + " -- unable to query for existing records: " + mediaPath.getAbsolutePath());
      }
      if (c.getCount() > 0) {
        // already exists
        throw new SQLException("FAILED Insert into " + uri
            + " -- row already exists for form directory: " + mediaPath.getAbsolutePath());
      }
    } catch ( Exception e ) {
      Log.w(t, "FAILED Insert into " + uri +
            " -- query for existing row failed: " + e.toString());

      if ( e instanceof SQLException ) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Insert into " + uri +
            " -- query for existing row failed: " + e.toString());
      }
    } finally {
      if (c != null) {
        c.close();
      }
    }

    try {
      SQLiteDatabase db = dbh.getWritableDatabase();
      long rowId = db.insert(DataModelDatabaseHelper.FORMS_TABLE_NAME, null, values);
      if (rowId > 0) {
        Uri formUri = Uri.withAppendedPath(
            Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), appName),
            values.getAsString(FormsColumns.FORM_ID));
        getContext().getContentResolver().notifyChange(formUri, null);
        Uri idUri = Uri.withAppendedPath(
            Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), appName),
            Long.toString(rowId));
        getContext().getContentResolver().notifyChange(idUri, null);

        return formUri;
      }
    } catch ( Exception e ) {
      Log.w(t, "FAILED Insert into " + uri +
            " -- insert of row failed: " + e.toString());

      if ( e instanceof SQLException ) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Insert into " + uri +
            " -- insert of row failed: " + e.toString());
      }
    }

    throw new SQLException("Failed to insert row into " + uri);
  }

  /** used only within moveDirectory */
  static enum DirType {
    FORMS, FRAMEWORK, OTHER
  };

  private void moveDirectory(String appName, DirType mediaType, File mediaDirectory) throws IOException {

    if (mediaDirectory.exists() && mediaType != DirType.OTHER) {
      // it is a directory under our control
      // -- move it to the stale forms or framework path...
      // otherwise, it is not where we will look for it,
      // so we can ignore it (once the record is gone
      // from our FormsProvider, we will not accidentally
      // detect it).
      String rootName = mediaDirectory.getName();
      int rev = 2;
      String staleMediaPathBase;
      if (mediaType == DirType.FORMS) {
        staleMediaPathBase = ODKFileUtils.getStaleFormsFolder(appName) + File.separator;
      } else {
        staleMediaPathBase = ODKFileUtils.getStaleFrameworkFolder(appName) + File.separator;
      }

      String staleMediaPathName = staleMediaPathBase + rootName;
      File staleMediaPath = new File(staleMediaPathName);

      while (staleMediaPath.exists()) {
        try {
          if (staleMediaPath.exists()) {
            FileUtils.deleteDirectory(staleMediaPath);
            if (!staleMediaPath.exists()) {
              // we successfully deleted an older directory -- reuse it...
              break;
            }
          }
          Log.i(t, "Successful delete of stale directory: " + staleMediaPathName);
        } catch (IOException ex) {
          ex.printStackTrace();
          Log.i(t, "Unable to delete stale directory: " + staleMediaPathName);
        }
        staleMediaPathName = staleMediaPathBase + rootName + "_" + rev;
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
      if (TextUtils.isEmpty(where)) {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=?";
        whereIdArgs = new String[1];
        whereIdArgs[0] = uriFormId;
      } else {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=? AND (" + where
            + ")";
        whereIdArgs = new String[whereArgs.length + 1];
        whereIdArgs[0] = uriFormId;
        for (int i = 0; i < whereArgs.length; ++i) {
          whereIdArgs[i + 1] = whereArgs[i];
        }
      }
    }

    Cursor del = null;
    Integer idValue = null;
    String tableIdValue = null;
    String formIdValue = null;
    HashMap<File,DirType> mediaDirs = new HashMap<File,DirType>();
    try {
      del = this.query(uri, null, whereId, whereIdArgs, null);
      if ( del == null ) {
        throw new SQLException("FAILED Delete into " + uri
            + " -- unable to query for existing records");
      }
      del.moveToPosition(-1);
      while (del.moveToNext()) {
        idValue = del.getInt(del.getColumnIndex(FormsColumns._ID));
        tableIdValue = del.getString(del.getColumnIndex(FormsColumns.TABLE_ID));
        formIdValue = del.getString(del.getColumnIndex(FormsColumns.FORM_ID));
        File mediaDir = new File(del.getString(del.getColumnIndex(FormsColumns.FORM_MEDIA_PATH)));
        mediaDirs.put(mediaDir, (tableIdValue == null) ? DirType.FRAMEWORK : DirType.FORMS );
      }
    } catch ( Exception e ) {
      Log.w(t, "FAILED Delete from " + uri +
            " -- query for existing row failed: " + e.toString());

      if ( e instanceof SQLException ) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Delete from " + uri +
            " -- query for existing row failed: " + e.toString());
      }
    } finally {
      if (del != null) {
        del.close();
      }
    }

    int count;
    try {
      DataModelDatabaseHelper dbh = getDbHelper(getContext(), appName);
      if ( dbh == null ) {
        Log.w(t, "Unable to access database for appName " + appName);
        return 0;
      }

      SQLiteDatabase db = dbh.getWritableDatabase();
      if ( db == null ) {
        Log.w(t, "Unable to access database for appName " + appName);
        return 0;
      }
      count = db.delete(DataModelDatabaseHelper.FORMS_TABLE_NAME, whereId, whereIdArgs);
    } catch ( Exception e ) {
      e.printStackTrace();
      Log.w(t, "Unable to perform deletion " + e.toString());
      return 0;
    }

    // and attempt to move these directories to the stale forms location
    // so that they do not immediately get rescanned...

    for (HashMap.Entry<File,DirType> entry : mediaDirs.entrySet() ) {
      try {
        moveDirectory(appName, entry.getValue(), entry.getKey());
      } catch (IOException e) {
        e.printStackTrace();
        Log.e(t, "Unable to move directory " + e.toString());
      }
    }

    if (count == 1) {
      Uri formUri = Uri
          .withAppendedPath(
              Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), appName),
              formIdValue);
      getContext().getContentResolver().notifyChange(formUri, null);
      Uri idUri = Uri.withAppendedPath(
          Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), appName),
          Long.toString(idValue));
      getContext().getContentResolver().notifyChange(idUri, null);
    } else {
      getContext().getContentResolver().notifyChange(uri, null);
    }
    return count;
  }

  static class FormIdVersion {
    final String tableId;
    final String formId;
    final String formVersion;

    FormIdVersion(String tableId, String formId, String formVersion) {
      this.tableId = tableId;
      this.formId = formId;
      this.formVersion = formVersion;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FormIdVersion))
        return false;
      FormIdVersion that = (FormIdVersion) o;

      // identical if id and version matches...
      return tableId.equals(that.tableId) &&
          formId.equals(that.formId) &&
          ((formVersion == null) ? (that.formVersion == null)
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
      if (TextUtils.isEmpty(where)) {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=?";
        whereIdArgs = new String[1];
        whereIdArgs[0] = uriFormId;
      } else {
        whereId = (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=? AND (" + where
            + ")";
        whereIdArgs = new String[whereArgs.length + 1];
        whereIdArgs[0] = uriFormId;
        for (int i = 0; i < whereArgs.length; ++i) {
          whereIdArgs[i + 1] = whereArgs[i];
        }
      }
    }

    /*
     * First, find out what records match this query, and if they refer to two
     * or more (formId,formVersion) tuples, then be sure to remove all
     * FORM_MEDIA_PATH references. Otherwise, if they are all for the same
     * tuple, and the update specifies a FORM_MEDIA_PATH, move all the
     * non-matching directories elsewhere.
     */
    Integer idValue = null;
    String tableIdValue = null;
    String formIdValue = null;
    HashMap<File, DirType> mediaDirs = new HashMap<File, DirType>();
    boolean multiset = false;
    Cursor c = null;
    try {
      c = this.query(uri, null, whereId, whereIdArgs, null);
      if ( c == null ) {
        throw new SQLException("FAILED Update of " + uri +
            " -- query for existing row did not return a cursor");
      }
      if (c.getCount() >= 1) {
        FormIdVersion ref = null;
        c.moveToPosition(-1);
        while (c.moveToNext()) {
          idValue = c.getInt(c.getColumnIndex(FormsColumns._ID));
          tableIdValue = c.getString(c.getColumnIndex(FormsColumns.TABLE_ID));
          formIdValue = c.getString(c.getColumnIndex(FormsColumns.FORM_ID));
          String tableId = c.getString(c.getColumnIndex(FormsColumns.TABLE_ID));
          String formId = c.getString(c.getColumnIndex(FormsColumns.FORM_ID));
          String formVersion = c.getString(c.getColumnIndex(FormsColumns.FORM_VERSION));
          FormIdVersion cur = new FormIdVersion(tableId, formId, formVersion);

          String mediaPath = c.getString(c.getColumnIndex(FormsColumns.FORM_MEDIA_PATH));
          if (mediaPath != null) {
            mediaDirs.put(new File(mediaPath), (tableIdValue == null) ? DirType.FRAMEWORK : DirType.FORMS);
          }

          if (ref != null && !ref.equals(cur)) {
            multiset = true;
            break;
          } else {
            ref = cur;
          }
        }
      }
    } catch ( Exception e ) {
      Log.w(t, "FAILED Update of " + uri +
            " -- query for existing row failed: " + e.toString());

      if ( e instanceof SQLException ) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Update of " + uri +
            " -- query for existing row failed: " + e.toString());
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
      for (HashMap.Entry<File,DirType> entry : mediaDirs.entrySet()) {
        File altPath = entry.getKey();
        if (!altPath.equals(mediaPath)) {
          try {
            moveDirectory(appName, entry.getValue(), altPath);
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

    int count;
    try {
      // OK Finally, now do the update...
      DataModelDatabaseHelper dbh = getDbHelper(getContext(), appName);
      if ( dbh == null ) {
        Log.w(t, "Unable to access database for appName " + appName);
        return 0;
      }

      SQLiteDatabase db = dbh.getWritableDatabase();
      if ( db == null ) {
        Log.w(t, "Unable to access metadata directory for appName " + appName);
        return 0;
      }

      count = db.update(DataModelDatabaseHelper.FORMS_TABLE_NAME, values, whereId, whereIdArgs);
    } catch ( Exception e ) {
      e.printStackTrace();
      Log.w(t, "Unable to perform update " + uri);
      return 0;
    }

    if (count == 1) {
      Uri formUri = Uri
          .withAppendedPath(
              Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), appName),
              formIdValue);
      getContext().getContentResolver().notifyChange(formUri, null);
      Uri idUri = Uri.withAppendedPath(
          Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), appName),
          Long.toString(idValue));
      getContext().getContentResolver().notifyChange(idUri, null);
    } else {
      getContext().getContentResolver().notifyChange(uri, null);
    }
    return count;
  }
}
