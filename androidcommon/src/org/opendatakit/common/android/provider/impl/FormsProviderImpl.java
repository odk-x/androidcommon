/*
 * Copyright (C) 2007 The Android Open Source Project
 * Copyright (C) 2011-2013 University of Washington
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
 * License for the specific language governing permissions and limitations under
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opendatakit.common.android.R;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.database.DatabaseConstants;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.utilities.ODKDatabaseUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;

import android.content.ContentProvider;
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
public abstract class FormsProviderImpl extends ContentProvider {
  static final String t = "FormsProvider";

  public abstract String getFormsAuthority();

  @Override
  public boolean onCreate() {

    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      File f = new File(ODKFileUtils.getOdkFolder());
      if (!f.exists()) {
        f.mkdir();
      } else if (!f.isDirectory()) {
        Log.e(t, f.getAbsolutePath() + " is not a directory!");
        return false;
      }
    } catch (Exception e) {
      Log.e(t, "External storage not available");
      return false;
    }

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
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLogger log = WebLogger.getLogger(appName);

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
    SQLiteDatabase db = null;
    boolean success = false;
    Cursor c = null;
    try {
      db = DatabaseFactory.get().getDatabase(getContext(), appName);
      c = db.query(DatabaseConstants.FORMS_TABLE_NAME, projection, whereId, whereIdArgs,
          null, null, sortOrder);
      success = true;
    } catch (Exception e) {
      log.w(t, "Unable to query database for appName: " + appName);
      return null;
    } finally {
      if ( !success && db != null ) {
        db.close();
      }
    }

    if (c == null) {
      log.w(t, "Unable to query database for appName: " + appName);
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
    if (values.containsKey(FormsColumns.APP_RELATIVE_FORM_FILE_PATH)) {
      values.remove(FormsColumns.APP_RELATIVE_FORM_FILE_PATH);
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

    // don't let users put in a manual json md5 hash
    if (values.containsKey(FormsColumns.JSON_MD5_HASH)) {
      values.remove(FormsColumns.JSON_MD5_HASH);
    }

    // if we are not updating FORM_MEDIA_PATH, we don't need to recalc any
    // of the above
    if (!values.containsKey(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH)) {
      return;
    }

    // Normalize path...

    // First, construct the full file path...
    String path = values.getAsString(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH);
    File mediaPath;
    if (path.startsWith(File.separator)) {
      mediaPath = new File(path);
    } else {
      mediaPath = ODKFileUtils.asAppFile(appName, path);
    }

    // require that the form directory actually exists
    if (!mediaPath.exists()) {
      throw new IllegalArgumentException(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH
          + " directory does not exist: " + mediaPath.getAbsolutePath());
    }

    if (!ODKFileUtils.isPathUnderAppName(appName, mediaPath)) {
      throw new IllegalArgumentException(
                                         "Form definition is not contained within the application: "
                                             + appName);
    }

    values.put(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH,
        ODKFileUtils.asRelativePath(appName, mediaPath));

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
      values.put(FormsColumns.APP_RELATIVE_FORM_FILE_PATH,
          ODKFileUtils.asRelativePath(appName, xformsFile));
    }

    // compute FORM_PATH...
    String formPath = ODKFileUtils.getRelativeFormPath(appName, formDefFile);
    values.put(FormsColumns.FORM_PATH, formPath);

    String md5;
    if (xformsFile.exists()) {
      md5 = ODKFileUtils.getMd5Hash(appName, xformsFile);
    } else {
      md5 = "-none-";
    }
    values.put(FormsColumns.MD5_HASH, md5);

    md5 = ODKFileUtils.getMd5Hash(appName, formDefFile);
    values.put(FormsColumns.JSON_MD5_HASH, md5);
  }

  @Override
  public synchronized Uri insert(Uri uri, ContentValues initialValues) {
    List<String> segments = uri.getPathSegments();

    if (segments.size() != 1) {
      throw new IllegalArgumentException("Unknown URI (too many segments!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLogger log = WebLogger.getLogger(appName);

    ContentValues values;
    if (initialValues != null) {
      values = new ContentValues(initialValues);
    } else {
      values = new ContentValues();
    }

    // ODK2: require FORM_MEDIA_PATH (different behavior -- ODK1 and
    // required FORM_FILE_PATH)
    if (!values.containsKey(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH)) {
      throw new IllegalArgumentException(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH
          + " must be specified.");
    }

    // Normalize path...
    File mediaPath = ODKFileUtils.asAppFile(appName,
        values.getAsString(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH));

    // require that the form directory actually exists
    if (!mediaPath.exists()) {
      throw new IllegalArgumentException(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH
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
    String[] projection = { FormsColumns.FORM_ID, FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH
    };
    String[] selectionArgs = { ODKFileUtils.asRelativePath(appName, mediaPath)
    };
    String selection = FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH + "=?";
    Cursor c = null;

    SQLiteDatabase db = null;
    try {
      db = DatabaseFactory.get().getDatabase(getContext(), appName);
      db.beginTransaction();
      try {
        c = db.query(DatabaseConstants.FORMS_TABLE_NAME, projection, selection, selectionArgs,
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
      } catch (Exception e) {
        log.w(t, "FAILED Insert into " + uri + " -- query for existing row failed: " + e.toString());

        if (e instanceof SQLException) {
          throw (SQLException) e;
        } else {
          throw new SQLException("FAILED Insert into " + uri + " -- query for existing row failed: "
              + e.toString());
        }
      } finally {
        if (c != null) {
          c.close();
        }
      }

      try {
        long rowId = db.insert(DatabaseConstants.FORMS_TABLE_NAME, null, values);
        db.setTransactionSuccessful();
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
      } catch (Exception e) {
        log.w(t, "FAILED Insert into " + uri + " -- insert of row failed: " + e.toString());

        if (e instanceof SQLException) {
          throw (SQLException) e;
        } else {
          throw new SQLException("FAILED Insert into " + uri + " -- insert of row failed: "
              + e.toString());
        }
      }
    } finally {
      if ( db != null ) {
        db.endTransaction();
        db.close();
      }
    }

    throw new SQLException("Failed to insert row into " + uri);
  }

  /** used only within moveDirectory */
  static enum DirType {
    FORMS, FRAMEWORK, OTHER
  };

  private void moveDirectory(String appName, DirType mediaType, File mediaDirectory)
      throws IOException {
    WebLogger log = WebLogger.getLogger(appName);

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
          log.i(t, "Successful delete of stale directory: " + staleMediaPathName);
        } catch (IOException ex) {
          ex.printStackTrace();
          log.i(t, "Unable to delete stale directory: " + staleMediaPathName);
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
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLogger log = WebLogger.getLogger(appName);

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
    HashMap<File, DirType> mediaDirs = new HashMap<File, DirType>();
    try {
      del = this.query(uri, null, whereId, whereIdArgs, null);
      if (del == null) {
        throw new SQLException("FAILED Delete into " + uri
            + " -- unable to query for existing records");
      }
      del.moveToPosition(-1);
      while (del.moveToNext()) {
        idValue = ODKDatabaseUtils.get().getIndexAsType(del, Integer.class, del.getColumnIndex(FormsColumns._ID));
        tableIdValue = ODKDatabaseUtils.get().getIndexAsString(del, del.getColumnIndex(FormsColumns.TABLE_ID));
        formIdValue = ODKDatabaseUtils.get().getIndexAsString(del, del.getColumnIndex(FormsColumns.FORM_ID));
        File mediaDir = ODKFileUtils.asAppFile(appName,
            ODKDatabaseUtils.get().getIndexAsString(del, del.getColumnIndex(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH)));
        mediaDirs.put(mediaDir, (tableIdValue == null) ? DirType.FRAMEWORK : DirType.FORMS);
      }
    } catch (Exception e) {
      log.w(t, "FAILED Delete from " + uri + " -- query for existing row failed: " + e.toString());

      if (e instanceof SQLException) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Delete from " + uri + " -- query for existing row failed: "
            + e.toString());
      }
    } finally {
      if (del != null && !del.isClosed()) {
        del.close();
      }
    }

    SQLiteDatabase db = null;
    int count;
    try {
      db = DatabaseFactory.get().getDatabase(getContext(), appName);
      db.beginTransaction();
      count = db.delete(DatabaseConstants.FORMS_TABLE_NAME, whereId, whereIdArgs);
      db.setTransactionSuccessful();
    } catch (Exception e) {
      e.printStackTrace();
      log.w(t, "Unable to perform deletion " + e.toString());
      return 0;
    } finally {
      if ( db != null ) {
        db.endTransaction();
        db.close();
      }
    }

    // and attempt to move these directories to the stale forms location
    // so that they do not immediately get rescanned...

    for (HashMap.Entry<File, DirType> entry : mediaDirs.entrySet()) {
      try {
        moveDirectory(appName, entry.getValue(), entry.getKey());
      } catch (IOException e) {
        e.printStackTrace();
        log.e(t, "Unable to move directory " + e.toString());
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
      return tableId.equals(that.tableId)
          && formId.equals(that.formId)
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
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLogger log = WebLogger.getLogger(appName);

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
      if (c == null) {
        throw new SQLException("FAILED Update of " + uri
            + " -- query for existing row did not return a cursor");
      }
      if (c.getCount() >= 1) {
        FormIdVersion ref = null;
        c.moveToPosition(-1);
        while (c.moveToNext()) {
          idValue = ODKDatabaseUtils.get().getIndexAsType(c, Integer.class, c.getColumnIndex(FormsColumns._ID));
          tableIdValue = ODKDatabaseUtils.get().getIndexAsString(c, c.getColumnIndex(FormsColumns.TABLE_ID));
          formIdValue = ODKDatabaseUtils.get().getIndexAsString(c, c.getColumnIndex(FormsColumns.FORM_ID));
          String tableId = ODKDatabaseUtils.get().getIndexAsString(c, c.getColumnIndex(FormsColumns.TABLE_ID));
          String formId = ODKDatabaseUtils.get().getIndexAsString(c, c.getColumnIndex(FormsColumns.FORM_ID));
          String formVersion = ODKDatabaseUtils.get().getIndexAsString(c, c.getColumnIndex(FormsColumns.FORM_VERSION));
          FormIdVersion cur = new FormIdVersion(tableId, formId, formVersion);

          int appRelativeMediaPathIdx = c.getColumnIndex(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH);
          String mediaPath = ODKDatabaseUtils.get().getIndexAsString(c, appRelativeMediaPathIdx);
          if (mediaPath != null) {
            mediaDirs.put(ODKFileUtils.asAppFile(appName, mediaPath),
                (tableIdValue == null) ? DirType.FRAMEWORK : DirType.FORMS);
          }

          if (ref != null && !ref.equals(cur)) {
            multiset = true;
            break;
          } else {
            ref = cur;
          }
        }
      }
    } catch (Exception e) {
      log.w(t, "FAILED Update of " + uri + " -- query for existing row failed: " + e.toString());

      if (e instanceof SQLException) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Update of " + uri + " -- query for existing row failed: "
            + e.toString());
      }
    } finally {
      if (c != null) {
        c.close();
      }
    }

    if (multiset) {
      // don't let users manually update media path
      // we are referring to two or more (formId,formVersion) tuples.
      if (values.containsKey(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH)) {
        values.remove(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH);
      }
    } else if (values.containsKey(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH)) {
      // we are not a multiset and we are setting the media path
      // try to move all the existing non-matching media paths to
      // somewhere else...
      File mediaPath = ODKFileUtils.asAppFile(appName,
          values.getAsString(FormsColumns.APP_RELATIVE_FORM_MEDIA_PATH));
      for (HashMap.Entry<File, DirType> entry : mediaDirs.entrySet()) {
        File altPath = entry.getKey();
        if (!altPath.equals(mediaPath)) {
          try {
            moveDirectory(appName, entry.getValue(), altPath);
          } catch (IOException e) {
            e.printStackTrace();
            log.e(t, "Attempt to move " + altPath.getAbsolutePath() + " failed: " + e.toString());
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

    SQLiteDatabase db = null;
    int count;
    try {
      // OK Finally, now do the update...
      db = DatabaseFactory.get().getDatabase(getContext(), appName);
      db.beginTransaction();
      count = db.update(DatabaseConstants.FORMS_TABLE_NAME, values, whereId, whereIdArgs);
      db.setTransactionSuccessful();
    } catch (Exception e) {
      e.printStackTrace();
      log.w(t, "Unable to perform update " + uri);
      return 0;
    } finally {
      if ( db != null ) {
        db.endTransaction();
        db.close();
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
}
