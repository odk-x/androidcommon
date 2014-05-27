package org.opendatakit.common.android.provider.impl;

import java.io.File;
import java.util.List;

import org.opendatakit.common.android.database.DataModelDatabaseHelper;
import org.opendatakit.common.android.database.DataModelDatabaseHelperFactory;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;

public abstract class TablesProviderImpl extends ContentProvider {
  private static final String t = "TablesProviderImpl";

  public abstract String getTablesAuthority();

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
      Log.e(t, "External storage not available -- purging dbHelpers");
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

    String uriTableId = ((segments.size() == 2) ? segments.get(1) : null);

    // Modify the where clause to account for the presence of a tableId
    String whereId;
    String[] whereIdArgs;

    if (uriTableId == null) {
      whereId = where;
      whereIdArgs = whereArgs;
    } else {
      if (TextUtils.isEmpty(where)) {
        whereId = TableDefinitionsColumns.TABLE_ID + "=?";
        whereIdArgs = new String[1];
        whereIdArgs[0] = uriTableId;
      } else {
        whereId = TableDefinitionsColumns.TABLE_ID + "=? AND (" + where
            + ")";
        whereIdArgs = new String[whereArgs.length + 1];
        whereIdArgs[0] = uriTableId;
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
      DataModelDatabaseHelper dbh = DataModelDatabaseHelperFactory.getDbHelper(getContext(), appName);
      if (dbh == null) {
        log.w(t, "Unable to access database for appName " + appName);
        return null;
      }

      db = dbh.getReadableDatabase();
      c = db.query(DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, projection, whereId, whereIdArgs,
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
    String uriTableId = ((segments.size() == 2) ? segments.get(1) : null);

    if (uriTableId == null) {
      return TableDefinitionsColumns.CONTENT_TYPE;
    } else {
      return TableDefinitionsColumns.CONTENT_ITEM_TYPE;
    }
  }

  @Override
  public Uri insert(Uri uri, ContentValues values) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int delete(Uri uri, String selection, String[] selectionArgs) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
    throw new UnsupportedOperationException("Not implemented");
  }

}
