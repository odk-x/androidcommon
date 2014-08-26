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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.provider.InstanceColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;
import org.opendatakit.common.android.utilities.ODKDatabaseUtils;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * This class helps open, create, and upgrade the database file.
 */
public class DataModelDatabaseHelper extends WebKitDatabaseInfoHelper {

  static final String APP_KEY = "org.opendatakit.common";
  static final int APP_VERSION = 1;

  static final String t = "DataModelDatabaseHelper";

  /**
   * key-value store table
   */

  // tablenames for the various key value stores
  public static final String KEY_VALUE_STORE_ACTIVE_TABLE_NAME = "_key_value_store_active";
  public static final String KEY_VALULE_STORE_SYNC_TABLE_NAME = "_key_value_store_sync";

  /**
   * table definitions table
   */

  // only one of these...
  public static final String TABLE_DEFS_TABLE_NAME = "_table_definitions";
  /**
   * column definitions table
   */

  // only one of these...
  public static final String COLUMN_DEFINITIONS_TABLE_NAME = "_column_definitions";

  /**
   * For ODK Survey (only)
   *
   * Tracks all the forms present in the forms directory.
   */
  public static final String SURVEY_CONFIGURATION_TABLE_NAME = "_survey_configuration";

  /**
   * For ODK Survey (only)
   *
   * Tracks which rows have been sent to the server. TODO: rework to accommodate
   * publishing to multiple formids for a given table row
   */

  public static final String UPLOADS_TABLE_NAME = "_uploads";

  /**
   * For ODK Survey (only)
   *
   * Tracks all the forms present in the forms directory.
   */

  public static final String FORMS_TABLE_NAME = "_formDefs";

  public DataModelDatabaseHelper(String appName, String dbPath, String databaseName) {
    super(appName, dbPath, databaseName, null, APP_KEY, APP_VERSION);
  }

  private void commonTableDefn(SQLiteDatabase db) {
    // db.execSQL(SurveyConfigurationColumns.getTableCreateSql(SURVEY_CONFIGURATION_TABLE_NAME));
    db.execSQL(InstanceColumns.getTableCreateSql(UPLOADS_TABLE_NAME));
    db.execSQL(FormsColumns.getTableCreateSql(FORMS_TABLE_NAME));
    db.execSQL(ColumnDefinitionsColumns.getTableCreateSql(COLUMN_DEFINITIONS_TABLE_NAME));
    db.execSQL(KeyValueStoreColumns.getTableCreateSql(KEY_VALUE_STORE_ACTIVE_TABLE_NAME));
    db.execSQL(KeyValueStoreColumns.getTableCreateSql(KEY_VALULE_STORE_SYNC_TABLE_NAME));
    db.execSQL(TableDefinitionsColumns.getTableCreateSql(TABLE_DEFS_TABLE_NAME));
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

  public static void deleteTableAndData(SQLiteDatabase db, String formId) {
    try {
      IdInstanceNameStruct ids = getIds(db, formId);

      String whereClause = TableDefinitionsColumns.TABLE_ID + " = ?";
      String[] whereArgs = { ids.tableId };

      db.beginTransaction();

      // Drop the table used for the formId
      db.execSQL("DROP TABLE IF EXISTS " + ids.tableId + ";");

      // Delete the table definition for the tableId
      int count = db.delete(TABLE_DEFS_TABLE_NAME, whereClause, whereArgs);

      // Delete the column definitions for this tableId
      db.delete(COLUMN_DEFINITIONS_TABLE_NAME, whereClause, whereArgs);

      // Delete the uploads for the tableId
      String uploadWhereClause = InstanceColumns.DATA_TABLE_TABLE_ID + " = ?";
      db.delete(UPLOADS_TABLE_NAME, uploadWhereClause, whereArgs);

      // Delete the values from the 4 key value stores
      db.delete(KEY_VALUE_STORE_ACTIVE_TABLE_NAME, whereClause, whereArgs);
      db.delete(KEY_VALULE_STORE_SYNC_TABLE_NAME, whereClause, whereArgs);

      db.setTransactionSuccessful();

    } finally {
      db.endTransaction();
    }
  }

  /**
   * Accessor to retrieve the database table name given the tableId
   *
   * @param db
   * @param tableId
   * @return
   */
  public static String getDbTableName(SQLiteDatabase db, String tableId) {
    Cursor c = null;
    try {
      c = db.query(TABLE_DEFS_TABLE_NAME, new String[] { TableDefinitionsColumns.DB_TABLE_NAME },
          TableDefinitionsColumns.TABLE_ID + "=?", new String[] { tableId }, null, null, null);

      if (c.moveToFirst()) {
        int idx = c.getColumnIndex(TableDefinitionsColumns.DB_TABLE_NAME);
        return ODKDatabaseUtils.getIndexAsString(c, idx);
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return null;
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
      c = db.query(FORMS_TABLE_NAME, new String[] { FormsColumns._ID, FormsColumns.FORM_ID,
          FormsColumns.TABLE_ID, FormsColumns.INSTANCE_NAME },
          (isNumericId ? FormsColumns._ID : FormsColumns.FORM_ID) + "=?",
          new String[] { formId }, null, null, null);

      if (c.moveToFirst()) {
        int idxId = c.getColumnIndex(FormsColumns._ID);
        int idxFormId = c.getColumnIndex(FormsColumns.FORM_ID);
        int idxTableId = c.getColumnIndex(FormsColumns.TABLE_ID);
        int idxInstanceName = c.getColumnIndex(FormsColumns.INSTANCE_NAME);

        return new IdInstanceNameStruct(c.getInt(idxId),
            ODKDatabaseUtils.getIndexAsString(c, idxFormId),
            ODKDatabaseUtils.getIndexAsString(c, idxTableId),
            ODKDatabaseUtils.getIndexAsString(c, idxInstanceName));
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return null;
  }

  public static class ColumnDefinition {
    public final String elementKey;
    public final String elementName;
    public final String elementType;
    private boolean isUnitOfRetention = true; // assumed until revised...

    public final ArrayList<ColumnDefinition> children = new ArrayList<ColumnDefinition>();
    public ColumnDefinition parent = null;

    ColumnDefinition(String elementKey, String elementName, String elementType) {
      this.elementKey = elementKey;
      this.elementName = elementName;
      this.elementType = elementType;
    }

    private void setParent(ColumnDefinition parent) {
      this.parent = parent;
    }

    void addChild(ColumnDefinition child) {
      child.setParent(this);
      children.add(child);
    }

    public boolean isUnitOfRetention() {
      return isUnitOfRetention;
    }
    
    void setNotUnitOfRetention() {
      isUnitOfRetention = false;
    }

  };

  private static class ColumnContainer {
    public ColumnDefinition defn = null;
    public ArrayList<String> children = null;
  };

  /**
   * Covert the ColumnDefinition map into a JSON schema.
   *
   * @param defns
   * @return
   */
  public static TreeMap<String, Object> getDataModel(Map<String, ColumnDefinition> defns) {
    TreeMap<String, Object> model = new TreeMap<String, Object>();

    for (ColumnDefinition c : defns.values()) {
      if (c.parent == null) {
        model.put(c.elementName, new TreeMap<String, Object>());
        @SuppressWarnings("unchecked")
        TreeMap<String, Object> jsonSchema = (TreeMap<String, Object>) model.get(c.elementName);
        getDataModelHelper(jsonSchema, c);
      }
    }
    return model;
  }

  private static void getDataModelHelper(TreeMap<String, Object> jsonSchema, ColumnDefinition c) {
    if (c.elementType.equals("string")) {
      jsonSchema.put("type", "string");
      jsonSchema.put("elementKey", c.elementKey);
    } else if (c.elementType.equals("number")) {
      jsonSchema.put("type", "number");
      jsonSchema.put("elementKey", c.elementKey);
    } else if (c.elementType.equals("integer")) {
      jsonSchema.put("type", "integer");
      jsonSchema.put("elementKey", c.elementKey);
    } else if (c.elementType.equals("boolean")) {
      jsonSchema.put("type", "boolean");
      jsonSchema.put("elementKey", c.elementKey);
    } else if (c.elementType.equals("array")) {
      jsonSchema.put("type", "array");
      jsonSchema.put("elementKey", c.elementKey);
      ColumnDefinition ch = c.children.get(0);
      jsonSchema.put("items", new TreeMap<String, Object>());
      @SuppressWarnings("unchecked")
      TreeMap<String, Object> itemSchema = (TreeMap<String, Object>) jsonSchema.get("items");
      getDataModelHelper(itemSchema, ch); // recursion...
    } else {
      jsonSchema.put("type", "object");
      if (!c.elementType.equals("object")) {
        jsonSchema.put("elementType", c.elementType);
      }
      jsonSchema.put("elementKey", c.elementKey);
      jsonSchema.put("properties", new TreeMap<String, Object>());
      @SuppressWarnings("unchecked")
      TreeMap<String, Object> propertiesSchema = (TreeMap<String, Object>) jsonSchema
          .get("properties");
      for (ColumnDefinition ch : c.children) {
        propertiesSchema.put(c.elementName, new TreeMap<String, Object>());
        @SuppressWarnings("unchecked")
        TreeMap<String, Object> itemSchema = (TreeMap<String, Object>) propertiesSchema
            .get(c.elementName);
        getDataModelHelper(itemSchema, ch); // recursion...
      }
    }
  }

  /**
   * Return a map of (elementKey -> ColumnDefinition)
   *
   * @param db
   * @param tableId
   * @return
   * @throws JsonParseException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static Map<String, ColumnDefinition> getColumnDefinitions(SQLiteDatabase db, String tableId)
      throws JsonParseException, JsonMappingException, IOException {
    Map<String, ColumnDefinition> defn = new HashMap<String, ColumnDefinition>();

    Cursor c = null;
    try {
      c = db.query(COLUMN_DEFINITIONS_TABLE_NAME, null, ColumnDefinitionsColumns.TABLE_ID + "=?",
          new String[] { tableId }, null, null, null);

      if (c.moveToFirst()) {
        int idxEK = c.getColumnIndex(ColumnDefinitionsColumns.ELEMENT_KEY);
        int idxEN = c.getColumnIndex(ColumnDefinitionsColumns.ELEMENT_NAME);
        int idxET = c.getColumnIndex(ColumnDefinitionsColumns.ELEMENT_TYPE);
        int idxLIST = c.getColumnIndex(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS);
        HashMap<String, ColumnContainer> ref = new HashMap<String, ColumnContainer>();

        do {
          String elementKey = ODKDatabaseUtils.getIndexAsString(c, idxEK);
          String elementName = ODKDatabaseUtils.getIndexAsString(c, idxEN);
          String elementType = ODKDatabaseUtils.getIndexAsString(c, idxET);
          ArrayList<String> children = ODKDatabaseUtils.getIndexAsType(c, ArrayList.class, idxLIST);
          ColumnContainer ctn = new ColumnContainer();
          ctn.defn = new ColumnDefinition(elementKey, elementName, elementType);
          ctn.children = children;

          ref.put(elementKey, ctn);
        } while (c.moveToNext());

        // OK now connect all the children...

        for (ColumnContainer ctn : ref.values()) {
          if (ctn.children != null) {
            for (String ek : ctn.children) {
              ColumnContainer child = ref.get(ek);
              if (child == null) {
                throw new IllegalArgumentException("Unexpected missing child element: " + ek);
              }
              ctn.defn.addChild(child.defn);
            }
          }
        }

        // and construct the list of entries...
        for (ColumnContainer ctn : ref.values()) {
          defn.put(ctn.defn.elementKey, ctn.defn);
        }
        // and resolve whether a column is a unit of retention
        markUnitOfRetention(defn);
        return defn;
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return null;
  }
  /**
   *  This must match the code in the javascript layer
   *  See databaseUtils.markUnitOfRetention
   *  
   * @param defn
   */
  private static void markUnitOfRetention(Map<String, ColumnDefinition> defn) {
    // for all arrays, mark all descendants of the array as not-retained
    // because they are all folded up into the json representation of the array
    for ( String startKey : defn.keySet() ) {
      ColumnDefinition colDefn = defn.get(startKey);
      if ( !colDefn.isUnitOfRetention() ) {
        // this has already been processed
        continue;
      }
      if ( "array".equals(colDefn.elementType) ) {
        ArrayList<ColumnDefinition> descendantsOfArray = colDefn.children;
        ArrayList<ColumnDefinition> scratchArray = new ArrayList<ColumnDefinition>();
        while ( !descendantsOfArray.isEmpty() ) {
            for ( ColumnDefinition subDefn : descendantsOfArray ) {
              if ( !subDefn.isUnitOfRetention() ) {
                  // this has already been processed
                  continue;
              }
              subDefn.setNotUnitOfRetention();
              scratchArray.addAll(subDefn.children);
            }
            descendantsOfArray = scratchArray;
        }
      }
    }
    // and mark any non-arrays with multiple fields as not retained
    for ( String startKey : defn.keySet() ) {
      ColumnDefinition colDefn = defn.get(startKey);
      if ( !colDefn.isUnitOfRetention() ) {
          // this has already been processed
          continue;
      }
      if ( ! "array".equals(colDefn.elementType) ) {
        if ( !colDefn.children.isEmpty() ) {
          colDefn.setNotUnitOfRetention();
        }
      }
    }
  }
}