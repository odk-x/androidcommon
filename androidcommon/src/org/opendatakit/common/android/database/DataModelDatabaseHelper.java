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

package org.opendatakit.common.android.database;

import org.apache.commons.lang3.StringUtils;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.provider.InstanceColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;
import org.opendatakit.common.android.utilities.ODKDatabaseUtils;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

/**
 * This class helps open, create, and upgrade the database file.
 */
public class DataModelDatabaseHelper extends WebKitDatabaseInfoHelper {

  static final String APP_KEY = "org.opendatakit.common";
  static final int APP_VERSION = 1;

  static final String t = "DataModelDatabaseHelper";

  public DataModelDatabaseHelper(String appName, String dbPath, String databaseName) {
    super(appName, dbPath, databaseName, null, APP_KEY, APP_VERSION);
  }

  private void commonTableDefn(SQLiteDatabase db) {
    // db.execSQL(SurveyConfigurationColumns.getTableCreateSql(SURVEY_CONFIGURATION_TABLE_NAME));
    db.execSQL(InstanceColumns.getTableCreateSql(DatabaseConstants.UPLOADS_TABLE_NAME));
    db.execSQL(FormsColumns.getTableCreateSql(DatabaseConstants.FORMS_TABLE_NAME));
    db.execSQL(ColumnDefinitionsColumns.getTableCreateSql(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME));
    db.execSQL(KeyValueStoreColumns.getTableCreateSql(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME));
    db.execSQL(KeyValueStoreColumns.getTableCreateSql(DatabaseConstants.KEY_VALULE_STORE_SYNC_TABLE_NAME));
    db.execSQL(TableDefinitionsColumns.getTableCreateSql(DatabaseConstants.TABLE_DEFS_TABLE_NAME));
  }

  @Override
  public void onCreateAppVersion(SQLiteDatabase db) {
    commonTableDefn(db);
  }

  @Override
  public void onUpgradeAppVersion(SQLiteDatabase db, int oldVersion, int newVersion) {
    // for now, upgrade and creation use the same codepath...
    commonTableDefn(db);
  }

  public static final class IdInstanceNameStruct {
    public final int _id;
    public final String formId;
    public final String tableId;
    public final String instanceName;

    public IdInstanceNameStruct(int _id, String formId, String tableId, String instanceName) {
      this._id = _id;
      this.formId = formId;
      this.tableId = tableId;
      this.instanceName = instanceName;
    }
  }

  /**
   * Accessor to retrieve the database tableId given a formId
   *
   * @param db
   * @param formId
   *          -- either the integer _ID or the textual form_id
   * @return
   */
  public static IdInstanceNameStruct getIds(SQLiteDatabase db, String formId) {
    boolean isNumericId = StringUtils.isNumeric(formId);

    Cursor c = null;
    try {
      c = db.query(DatabaseConstants.FORMS_TABLE_NAME, new String[] { FormsColumns._ID, FormsColumns.FORM_ID,
          FormsColumns.TABLE_ID, FormsColumns.INSTANCE_NAME },
          (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=?",
          new String[] { formId }, null, null, null);

      if (c.moveToFirst()) {
        int idxId = c.getColumnIndex(FormsColumns._ID);
        int idxFormId = c.getColumnIndex(FormsColumns.FORM_ID);
        int idxTableId = c.getColumnIndex(FormsColumns.TABLE_ID);
        int idxInstanceName = c.getColumnIndex(FormsColumns.INSTANCE_NAME);

        return new IdInstanceNameStruct(c.getInt(idxId),
            ODKDatabaseUtils.get().getIndexAsString(c, idxFormId),
            ODKDatabaseUtils.get().getIndexAsString(c, idxTableId),
            ODKDatabaseUtils.get().getIndexAsString(c, idxInstanceName));
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return null;
  }
}