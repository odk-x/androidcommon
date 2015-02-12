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

import org.apache.commons.lang3.StringUtils;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.utilities.ODKDatabaseUtils;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

public final class IdInstanceNameStruct {
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