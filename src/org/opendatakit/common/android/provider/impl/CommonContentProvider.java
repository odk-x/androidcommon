/*
 * Copyright (C) 2013 University of Washington
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

import java.util.HashMap;
import java.util.Map;

import org.opendatakit.common.android.database.DataModelDatabaseHelper;
import org.opendatakit.common.android.database.WebDbDefinition;
import org.opendatakit.common.android.database.WebSqlDatabaseHelper;
import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.content.ContentProvider;

/**
 * Common base class for content providers. This holds the access logic to the
 * underlying shared database used by all the content providers.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public abstract class CommonContentProvider extends ContentProvider {

  // array of the underlying database handles used by all the content provider
  // instances
  private static final Map<String, DataModelDatabaseHelper> dbHelpers = new HashMap<String, DataModelDatabaseHelper>();

  /**
   * Shared accessor to get a database handle.
   *
   * @param appName
   * @return an entry in dbHelpers
   */
  public synchronized static DataModelDatabaseHelper getDbHelper(String appName) {

    DataModelDatabaseHelper dbHelper = dbHelpers.get(appName);
    if (dbHelper == null) {
      String path = ODKFileUtils.getWebDbFolder(appName);
      WebSqlDatabaseHelper h;
      h = new WebSqlDatabaseHelper(path);
      WebDbDefinition defn = h.getWebKitDatabaseInfoHelper();
      if (defn != null) {
        defn.dbFile.getParentFile().mkdirs();
        dbHelper = new DataModelDatabaseHelper(defn.dbFile.getParent(), defn.dbFile.getName());
        dbHelpers.put(appName, dbHelper);
      }
    }
    return dbHelper;
  }

}
