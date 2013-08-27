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

package org.opendatakit.common.android.provider;

import android.provider.BaseColumns;

public class TableDefinitionsColumns implements BaseColumns {

  public static final String TABLE_ID = "_table_id";
  public static final String TABLE_KEY = "_table_key";
  public static final String DB_TABLE_NAME = "_db_table_name";
  // DB_TYPE entries must be one of the types defined in TableType.
  public static final String TYPE = "_type";
  public static final String TABLE_ID_ACCESS_CONTROLS = "_table_id_access_controls";
  public static final String SYNC_TAG = "_sync_tag";
  public static final String LAST_SYNC_TIME = "_last_sync_time";
  public static final String SYNC_STATE = "_sync_state";
  public static final String TRANSACTIONING = "_transactioning";

  // This class cannot be instantiated
  private TableDefinitionsColumns() {
  }

  public static String getTableCreateSql(String tableName) {
    //@formatter:off
    String create = "CREATE TABLE IF NOT EXISTS " + tableName + "("
				+ TABLE_ID + " TEXT NOT NULL PRIMARY KEY, "
				+ TABLE_KEY	+ " TEXT NULL UNIQUE, "
				+ DB_TABLE_NAME + " TEXT NOT NULL UNIQUE, "
				+ TYPE + " TEXT NOT NULL, "
				+ TABLE_ID_ACCESS_CONTROLS + " TEXT NULL, "
				+ SYNC_TAG + " TEXT NULL,"
				// TODO last sync time should probably become an int?
				+ LAST_SYNC_TIME + " TEXT NOT NULL, "
				+ SYNC_STATE + " TEXT NOT NULL, "
				+ TRANSACTIONING + " INTEGER NOT NULL" + ")";
    //@formatter:on
    return create;
  }

}
