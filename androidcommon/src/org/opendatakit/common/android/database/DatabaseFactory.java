/*
 * Copyright (C) 2014 University of Washington
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

package org.opendatakit.common.android.database;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;

public class DatabaseFactory {
  
  private static DatabaseFactory databaseFactory = new DatabaseFactory();
  
  public static DatabaseFactory get() {
    return databaseFactory;
  }
 
  /**
   * For mocking -- supply a mocked object.
   * 
   * @param databaseFactory
   */
  public static void set(DatabaseFactory factory) {
    databaseFactory = factory;
  }

  protected DatabaseFactory() {}

  // array of the underlying database handles used by all the content provider
  // instances
  private final Map<String, DataModelDatabaseHelper> dbHelpers = new HashMap<String, DataModelDatabaseHelper>();

  /**
   * Shared accessor to get a database handle.
   *
   * @param appName
   * @return an entry in dbHelpers
   */
  private synchronized DataModelDatabaseHelper getDbHelper(Context context, String appName) {
    WebLogger log = null;

    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      ODKFileUtils.assertDirectoryStructure(appName);
      log = WebLogger.getLogger(appName);
    } catch ( Exception e ) {
      if ( log != null ) {
        log.e("DataModelDatabaseHelperFactory", "External storage not available -- purging dbHelpers");
      }
      dbHelpers.clear();
      return null;
    }

    String path = ODKFileUtils.getWebDbFolder(appName);
    File webDb = new File(path);
    if ( !webDb.exists() || !webDb.isDirectory()) {
      ODKFileUtils.assertDirectoryStructure(appName);
    }

    // the assert above should have created it...
    if ( !webDb.exists() || !webDb.isDirectory()) {
      log.e("DataModelDatabaseHelperFactory", "webDb directory not available -- purging dbHelpers");
      dbHelpers.clear();
      return null;
    }

    DataModelDatabaseHelper dbHelper = dbHelpers.get(appName);
    if (dbHelper == null) {
      WebSqlDatabaseHelper h;
      h = new WebSqlDatabaseHelper(context, path, appName);
      WebDbDefinition defn = h.getWebKitDatabaseInfoHelper();
      if (defn != null) {
        defn.dbFile.getParentFile().mkdirs();
        dbHelper = new DataModelDatabaseHelper(appName, defn.dbFile.getParent(), defn.dbFile.getName());
        dbHelpers.put(appName, dbHelper);
      }
    }
    return dbHelper;
  }

  public SQLiteDatabase getDatabase(Context context, String appName) {
    return getDbHelper(context, appName).getWritableDatabase();
  }
}
