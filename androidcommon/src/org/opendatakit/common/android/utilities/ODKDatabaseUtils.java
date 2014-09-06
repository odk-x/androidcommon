package org.opendatakit.common.android.utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColorRule;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.ElementDataType;
import org.opendatakit.common.android.data.ElementType;
import org.opendatakit.common.android.database.DataModelDatabaseHelper;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.provider.InstanceColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;

import android.annotation.SuppressLint;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class ODKDatabaseUtils {

  private static final String t = "ODKDatabaseUtils";

  public static final String DEFAULT_LOCALE = "default";
  public static final String DEFAULT_CREATOR = "anonymous";

  private static final String uriFrag = "uriFragment";
  private static final String contentType = "contentType";

  /*
   * These are the columns that are present in any row in the database.
   * Each row should have these in addition to the user-defined columns.
   * If you add a column here you have to be sure to also add it in the
   * create table statement, which can't be programmatically created easily.
   */
  private static final List<String> ADMIN_COLUMNS;

  /**
   * These are the columns that should be exported
   */
  private static final List<String> EXPORT_COLUMNS;

  static {
    ArrayList<String> adminColumns = new ArrayList<String>();
    adminColumns.add(DataTableColumns.ID);
    adminColumns.add(DataTableColumns.ROW_ETAG);
    adminColumns.add(DataTableColumns.SYNC_STATE); // not exportable
    adminColumns.add(DataTableColumns.CONFLICT_TYPE); // not exportable
    adminColumns.add(DataTableColumns.FILTER_TYPE);
    adminColumns.add(DataTableColumns.FILTER_VALUE);
    adminColumns.add(DataTableColumns.FORM_ID);
    adminColumns.add(DataTableColumns.LOCALE);
    adminColumns.add(DataTableColumns.SAVEPOINT_TYPE);
    adminColumns.add(DataTableColumns.SAVEPOINT_TIMESTAMP);
    adminColumns.add(DataTableColumns.SAVEPOINT_CREATOR);
    ADMIN_COLUMNS = Collections.unmodifiableList(adminColumns);

    ArrayList<String> exportColumns = new ArrayList<String>();
    exportColumns.add(DataTableColumns.ID);
    exportColumns.add(DataTableColumns.ROW_ETAG);
    exportColumns.add(DataTableColumns.FILTER_TYPE);
    exportColumns.add(DataTableColumns.FILTER_VALUE);
    exportColumns.add(DataTableColumns.FORM_ID);
    exportColumns.add(DataTableColumns.LOCALE);
    exportColumns.add(DataTableColumns.SAVEPOINT_TYPE);
    exportColumns.add(DataTableColumns.SAVEPOINT_TIMESTAMP);
    exportColumns.add(DataTableColumns.SAVEPOINT_CREATOR);
    EXPORT_COLUMNS = Collections.unmodifiableList(exportColumns);
  }

  /**
   * Return an unmodifiable list of the admin columns that must be present
   * in every database table.
   * @return
   */
  public static final List<String> getAdminColumns() {
    return ADMIN_COLUMNS;
  }

  public static final List<String> getExportColumns() {
    return EXPORT_COLUMNS;
  }

  private ODKDatabaseUtils() {
  }

  /*
   * Perform raw query against current database
   */
  public static final Cursor rawQuery(SQLiteDatabase db, String sql, String[] selectionArgs) {
    Cursor c = db.rawQuery(sql, selectionArgs);
    return c;
  }

  /*
   * Query the current database with given parameters
   */
  public static final Cursor query(SQLiteDatabase db, boolean distinct, String table,
      String[] columns, String selection, String[] selectionArgs, String groupBy, String having,
      String orderBy, String limit) {
    Cursor c = db.query(distinct, table, columns, selection, selectionArgs, groupBy, having,
        orderBy, limit);
    return c;
  }

  /*
   * Query the current database for all columns
   */
  public static final String[] getAllColumnNames(SQLiteDatabase db, String tableName) {
    Cursor cursor = db.rawQuery("SELECT * FROM " + tableName + " LIMIT 1", null);
    String[] colNames = cursor.getColumnNames();

    return colNames;
  }

  /*
   * Query the current database for all user defined columns
   * 
   * Returns both the grouping columns and the actual persisted
   * columns.
   */
  public static final List<Column> getUserDefinedColumns(
      SQLiteDatabase db, String tableName) {
    List<Column> userDefinedColumns = new ArrayList<Column>();
    String selection = ColumnDefinitionsColumns.TABLE_ID + "=?";
    String[] selectionArgs = { tableName };
    String[] cols = { ColumnDefinitionsColumns.ELEMENT_KEY,
        ColumnDefinitionsColumns.ELEMENT_NAME,
        ColumnDefinitionsColumns.ELEMENT_TYPE,
        ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS };
    Cursor c = null;
    try {
      c = db.query(DataModelDatabaseHelper.COLUMN_DEFINITIONS_TABLE_NAME, cols, selection,
        selectionArgs, null, null, null);

      int elemKeyIndex = c.getColumnIndexOrThrow(ColumnDefinitionsColumns.ELEMENT_KEY);
      int elemNameIndex = c.getColumnIndexOrThrow(ColumnDefinitionsColumns.ELEMENT_NAME);
      int elemTypeIndex = c.getColumnIndexOrThrow(ColumnDefinitionsColumns.ELEMENT_TYPE);
      int listChildrenIndex = c.getColumnIndexOrThrow(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS);
      c.moveToFirst();
      while (!c.isAfterLast()) {
        String elementKey = getIndexAsString(c, elemKeyIndex);
        String elementName = getIndexAsString(c, elemNameIndex);
        String elementType = getIndexAsString(c, elemTypeIndex);
        String listOfChildren = getIndexAsString(c, listChildrenIndex);
        userDefinedColumns.add(new Column(elementKey, elementName, elementType, listOfChildren));
        c.moveToNext();
      }
    } finally {
      c.close();
    }
    return userDefinedColumns;
  }

  /**
   * Verifies that the tableId exists in the database.
   *
   * @param db
   * @param tableId
   * @return true if table is listed in table definitions.
   */
  public static boolean hasTableId(SQLiteDatabase db, String tableId) {
    Cursor c = null;
    try {
      c = db.query(DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, null,
          TableDefinitionsColumns.TABLE_ID + "=?", new String[] { tableId }, null, null, null);

      if (c.moveToFirst()) {
        // we know about the table...
        // tableId is the database table name...
        return true;
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return false;
  }

  public static List<String> getAllTableIds(SQLiteDatabase db) {
    List<String> tableIds = new ArrayList<String>();
    Cursor c = null;
    try {
      c = db.query(DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, 
          new String[] { TableDefinitionsColumns.TABLE_ID },
          null, null, null, null, TableDefinitionsColumns.TABLE_ID + " ASC");

      if (c.moveToFirst()) {
        int idxId = c.getColumnIndex(TableDefinitionsColumns.TABLE_ID);
        do {
          String tableId = c.getString(idxId);
          if ( tableId == null || tableId.length() == 0) {
            c.close();
            throw new IllegalStateException("getAllTableIds: Unexpected tableId found!");
          }
          tableIds.add(tableId);
        } while ( c.moveToNext() );
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return tableIds;
  }
  
  public static void deleteTableAndData(SQLiteDatabase db, String tableId) {
    try {
      String whereClause = TableDefinitionsColumns.TABLE_ID + " = ?";
      String[] whereArgs = { tableId };

      db.beginTransaction();

      // Drop the table used for the formId
      db.execSQL("DROP TABLE IF EXISTS \"" + tableId + "\";");

      // Delete the table definition for the tableId
      int count = db.delete(DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, whereClause, whereArgs);

      // Delete the column definitions for this tableId
      db.delete(DataModelDatabaseHelper.COLUMN_DEFINITIONS_TABLE_NAME, whereClause, whereArgs);

      // Delete the uploads for the tableId
      String uploadWhereClause = InstanceColumns.DATA_TABLE_TABLE_ID + " = ?";
      db.delete(DataModelDatabaseHelper.UPLOADS_TABLE_NAME, uploadWhereClause, whereArgs);

      // Delete the values from the 4 key value stores
      db.delete(DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, whereClause, whereArgs);
      db.delete(DataModelDatabaseHelper.KEY_VALULE_STORE_SYNC_TABLE_NAME, whereClause, whereArgs);

      db.setTransactionSuccessful();

    } finally {
      db.endTransaction();
    }
  }

  /*
   * Get user defined table creation SQL statement
   */
  private static final String getUserDefinedTableCreationStatement(String tableName) {
    /*
     * Resulting string should be the following String createTableCmd =
     * "CREATE TABLE IF NOT EXISTS " + tableName + " (" + DataTableColumns.ID +
     * " TEXT NOT NULL, " + DataTableColumns.ROW_ETAG + " TEXT NULL, " +
     * DataTableColumns.SYNC_STATE + " TEXT NOT NULL, " +
     * DataTableColumns.CONFLICT_TYPE + " INTEGER NULL," +
     * DataTableColumns.FILTER_TYPE + " TEXT NULL," +
     * DataTableColumns.FILTER_VALUE + " TEXT NULL," + DataTableColumns.FORM_ID
     * + " TEXT NULL," + DataTableColumns.LOCALE + " TEXT NULL," +
     * DataTableColumns.SAVEPOINT_TYPE + " TEXT NULL," +
     * DataTableColumns.SAVEPOINT_TIMESTAMP + " TEXT NOT NULL," +
     * DataTableColumns.SAVEPOINT_CREATOR + " TEXT NULL";
     */

    String createTableCmd = "CREATE TABLE IF NOT EXISTS " + tableName + " (";

    List<String> cols = getAdminColumns();

    String endSeq = ", ";
    for (int i = 0 ; i < cols.size(); ++i) {
      if (i == cols.size() - 1) {
        endSeq = "";
      }
      String colName = cols.get(i);
      if (colName.equals(DataTableColumns.ID) || colName.equals(DataTableColumns.SYNC_STATE)
          || colName.equals(DataTableColumns.SAVEPOINT_TIMESTAMP)) {
        createTableCmd = createTableCmd + colName + " TEXT NOT NULL" + endSeq;
      } else if (colName.equals(DataTableColumns.ROW_ETAG)
          || colName.equals(DataTableColumns.FILTER_TYPE)
          || colName.equals(DataTableColumns.FILTER_VALUE)
          || colName.equals(DataTableColumns.FORM_ID) || colName.equals(DataTableColumns.LOCALE)
          || colName.equals(DataTableColumns.SAVEPOINT_TYPE)
          || colName.equals(DataTableColumns.SAVEPOINT_CREATOR)) {
        createTableCmd = createTableCmd + colName + " TEXT NULL" + endSeq;
      } else if (colName.equals(DataTableColumns.CONFLICT_TYPE)) {
        createTableCmd = createTableCmd + colName + " INTEGER NULL" + endSeq;
      }
    }

    return createTableCmd;
  }

  /*
   * Create a user defined database table metadata - table definiton and KVS
   * values
   */
  private static final void createDBTableMetadata(SQLiteDatabase db, String tableId) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    // Add the table id into table definitions
    ContentValues cvTableDef = new ContentValues();
    cvTableDef.put(TableDefinitionsColumns.TABLE_ID, tableId);
    cvTableDef.put(TableDefinitionsColumns.SYNC_TAG, "");
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME, -1);
    cvTableDef.put(TableDefinitionsColumns.SYNC_STATE, SyncState.new_row.name());
    cvTableDef.put(TableDefinitionsColumns.TRANSACTIONING, 0);

    db.replaceOrThrow(DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, null, cvTableDef);

    // Add the tables values into KVS
    ArrayList<ContentValues> cvTableValKVS = new ArrayList<ContentValues>();

    ContentValues cvTableVal = null;

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_COL_ORDER);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, "defaultViewType");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "SPREADSHEET");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_DISPLAY_NAME);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "\"" + tableId + "\"");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_GROUP_BY_COLS);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_INDEX_COL);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_SORT_COL);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_SORT_ORDER);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "TableColorRuleGroup");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, "StatusColumn.ruleList");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    try {
      List<ColorRule> rules = ColorRuleUtil.getDefaultSyncStateColorRules();
      List<TreeMap<String,Object>> jsonableList = new ArrayList<TreeMap<String,Object>>();
      for ( ColorRule rule : rules ) {
        jsonableList.add(rule.getJsonRepresentation());
      }
      String value = ODKFileUtils.mapper.writeValueAsString(jsonableList);
      cvTableVal.put(KeyValueStoreColumns.VALUE, value);
      cvTableValKVS.add(cvTableVal);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }

    // Now add Tables values into KVS
    for (int i = 0; i < cvTableValKVS.size(); i++) {
      db.replaceOrThrow(DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null,
          cvTableValKVS.get(i));
    }
  }
  
  /*
   * Create a user defined database table with a transaction
   */
  public static final void createOrOpenDBTableWithColumns(SQLiteDatabase db, String tableName,
      List<Column> columns) {
    boolean success = false;
    try {
      List<ColumnDefinition> orderedDefs = ColumnDefinition.buildColumnDefinitions(columns);
      db.beginTransaction();
      createDBTableWithColumns(db, tableName, orderedDefs);
      db.setTransactionSuccessful();
      success = true;
    } finally {
      db.endTransaction();
      if (success == false) {

        // Get the names of the columns
        StringBuilder colNames = new StringBuilder();
        if (columns != null) {
          for (Column column : columns) {
            colNames.append(" ").append(column.getElementKey()).append(",");
          }
          if (colNames != null && colNames.length() > 0) {
            colNames.deleteCharAt(colNames.length() - 1);
            Log.e(t, "createOrOpenDBTableWithColumns: Error while adding table " + tableName
                + " with columns:" + colNames.toString());
          }
        } else {
          Log.e(t, "createOrOpenDBTableWithColumns: Error while adding table " + tableName
              + " with columns: null");
        }
      }
    }
  }

  /*
   * Create a user defined database table metadata - table definiton and KVS
   * values
   */
  private static final void createDBTableWithColumns(SQLiteDatabase db, String tableName,
      List<ColumnDefinition> orderedDefs) {
    if (tableName == null || tableName.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    String createTableCmd = getUserDefinedTableCreationStatement(tableName);

    StringBuilder createTableCmdWithCols = new StringBuilder();
    createTableCmdWithCols.append(createTableCmd);

    for (ColumnDefinition column : orderedDefs) {
      if ( !column.isUnitOfRetention() ) {
        continue;
      }
      ElementType elementType = ElementType.parseElementType(column.getElementType(), 
          !column.getChildren().isEmpty());

      ElementDataType dataType = elementType.getDataType();
      String dbType;
      if ( dataType == ElementDataType.array ) {
        dbType = "TEXT";
      } else if ( dataType == ElementDataType.bool ) {
        dbType = "INTEGER";
      } else if ( dataType == ElementDataType.configpath ) {
        dbType = "TEXT";
      } else if ( dataType == ElementDataType.integer ) {
        dbType = "INTEGER";
      } else if ( dataType == ElementDataType.number ) {
        dbType = "REAL";
      } else if ( dataType == ElementDataType.object ) {
        dbType = "TEXT";
      } else if ( dataType == ElementDataType.rowpath ) {
        dbType = "TEXT";
      } else if ( dataType == ElementDataType.string ) {
        dbType = "TEXT";
      } else {
        throw new IllegalStateException("unexpected ElementDataType: " + dataType.name());
      }
      createTableCmdWithCols.append(", ").append(column.getElementKey()).append(" ").append(dbType).append(" NULL");
    }

    createTableCmdWithCols.append(");");

    db.execSQL(createTableCmdWithCols.toString());

    // Create the metadata for the table - table def and KVS
    createDBTableMetadata(db, tableName);

    // Now need to call the function to write out all the column values
    for (ColumnDefinition column : orderedDefs) {
      createNewColumnMetadata(db, tableName, column);
    }

    // Need to address column order
    ContentValues cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_COL_ORDER);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    
    StringBuilder tableDefCol = new StringBuilder();

    boolean needsComma = false;
    for ( ColumnDefinition def : orderedDefs ) {
      if ( !def.isUnitOfRetention() ) {
        continue;
      }
      if ( needsComma ) {
        tableDefCol.append(",");
      }
      needsComma = true;
      tableDefCol.append("\"").append(def.getElementKey()).append("\"");
    }
    
    Log.i(t, "Column order for table " + tableName + " is " + tableDefCol.toString());
    String colOrderVal = "[" + tableDefCol.toString() + "]";
    cvTableVal.put(KeyValueStoreColumns.VALUE, colOrderVal);

    // Now add Tables values into KVS
    db.replaceOrThrow(DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvTableVal);
  }

  /*
   * Create a new column metadata in the database - add column values to KVS and
   * column definitions
   */
  private static final void createNewColumnMetadata(SQLiteDatabase db, String tableName,
      ColumnDefinition column) {
    String colName = column.getElementKey();
    ArrayList<ContentValues> cvColValKVS = new ArrayList<ContentValues>();

    ContentValues cvColVal;
    
    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.array.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_FORMAT);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.string.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, "");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_NAME);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.object.name());
    String colDisplayName = "\"" + colName + "\"";
    cvColVal.put(KeyValueStoreColumns.VALUE, colDisplayName);
    cvColValKVS.add(cvColVal);

    // TODO: change bool to be integer valued in the KVS?
    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_VISIBLE);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.bool.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, column.isUnitOfRetention() ? "true" : "false");
    cvColValKVS.add(cvColVal);
    
    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_JOINS);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.object.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, "");
    cvColValKVS.add(cvColVal);

    // Now add all this data into the database
    for (int i = 0; i < cvColValKVS.size(); i++) {
      db.replaceOrThrow(DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null,
          cvColValKVS.get(i));
    }

    // Create column definition
    ContentValues cvColDefVal = null;

    cvColDefVal = new ContentValues();
    cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, column.getElementName());
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, column.getElementType());
    cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, column.getListChildElementKeys());

    // Now add this data into the database
    db.replaceOrThrow(DataModelDatabaseHelper.COLUMN_DEFINITIONS_TABLE_NAME, null,
          cvColDefVal);
  }

  /*
   * Write data into a user defined database table
   */
  public static final void writeDataIntoExistingDBTable(SQLiteDatabase db, String tableName,
      ContentValues cvValues) {
    
    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableName);
    }

    writeDataAndMetadataIntoExistingDBTable(db, tableName, cvValues, false);

  }

  /*
   * Write data into a user defined database table
   */
  public static final void updateDataInExistingDBTableWithId(SQLiteDatabase db, String tableName,
      ContentValues cvValues, String uuid) {

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableName);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.put(DataTableColumns.ID, uuid);
    cvDataTableVal.putAll(cvValues);

    writeDataAndMetadataIntoExistingDBTable(db, tableName, cvDataTableVal, true);
  }
  
  /*
   * Write data into a user defined database table
   */
  public static final void writeDataIntoExistingDBTableWithId(SQLiteDatabase db, String tableName,
      ContentValues cvValues, String uuid) {

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableName);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.put(DataTableColumns.ID, uuid);
    cvDataTableVal.putAll(cvValues);

    writeDataAndMetadataIntoExistingDBTable(db, tableName, cvDataTableVal, false);
  }

  /*
   * Write data into a user defined database table
   * 
   * TODO: This is broken w.r.t. updates of partial fields
   */
  public static final void writeDataAndMetadataIntoExistingDBTable(SQLiteDatabase db,
      String tableName, ContentValues cvValues, boolean shouldUpdate) {
    String nullString = null;
    String id = null;
    String whereClause = null;
    String [] whereArgs = new String[1];
    boolean update = false;

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableName);
    }

    // manufacture a rowId for this record...
    String rowId = "uuid:" + UUID.randomUUID().toString();
    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.putAll(cvValues);
    
    // Bug fix for not updating a db row if an existing row id is used
    if (cvDataTableVal.containsKey(DataTableColumns.ID)) {
      // Select everything out of the table with given id
      id = cvDataTableVal.getAsString(DataTableColumns.ID);
      whereClause = DataTableColumns.ID + " = ?"; 
      whereArgs[0] = "" + id;
      String sel = "SELECT * FROM " + tableName + " WHERE "+ whereClause;
      String[] selArgs = whereArgs;
      Cursor cursor = rawQuery(db, sel, selArgs);
      
      // There must be only one row in the db for the update to work
      if (shouldUpdate) {
        if (cursor.getCount() == 1) {
          update = true;
        } else if (cursor.getCount() > 1) {
          throw new IllegalArgumentException(t + ": row id " + id + " has more than 1 row in table " + tableName);
        }
      } else {
        if (cursor.getCount() > 0) {
          throw new IllegalArgumentException(t + ": id " + id + " is not unique in table " + tableName);
        }
      }

    }

    // TODO: This is broken w.r.t. updates of partial fields
    // TODO: This is broken w.r.t. updates of partial fields
    // TODO: This is broken w.r.t. updates of partial fields
    // TODO: This is broken w.r.t. updates of partial fields

    if (!cvDataTableVal.containsKey(DataTableColumns.ID)) {
      cvDataTableVal.put(DataTableColumns.ID, rowId);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.ROW_ETAG)) {
      cvDataTableVal.put(DataTableColumns.ROW_ETAG, nullString);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SYNC_STATE) ||
        (cvDataTableVal.get(DataTableColumns.SYNC_STATE) == null)) {
      cvDataTableVal.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.CONFLICT_TYPE)) {
      cvDataTableVal.put(DataTableColumns.CONFLICT_TYPE, nullString);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.FILTER_TYPE) ||
        (cvDataTableVal.get(DataTableColumns.FILTER_TYPE) == null)) {
      cvDataTableVal.put(DataTableColumns.FILTER_TYPE, DataTableColumns.DEFAULT_FILTER_TYPE);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.FILTER_VALUE) ||
        (cvDataTableVal.get(DataTableColumns.FILTER_VALUE) == null)) {
      cvDataTableVal.put(DataTableColumns.FILTER_VALUE, DataTableColumns.DEFAULT_FILTER_VALUE);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.FORM_ID)) {
      cvDataTableVal.put(DataTableColumns.FORM_ID, nullString);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.LOCALE) ||
        (cvDataTableVal.get(DataTableColumns.LOCALE) == null)) {
      cvDataTableVal.put(DataTableColumns.LOCALE, DataTableColumns.DEFAULT_LOCALE);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TYPE) ||
        (cvDataTableVal.get(DataTableColumns.SAVEPOINT_TYPE) == null)) {
      cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP)) {
      cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_CREATOR) ||
        (cvDataTableVal.get(DataTableColumns.SAVEPOINT_CREATOR) == null)) {
      cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR, DataTableColumns.DEFAULT_SAVEPOINT_CREATOR );
    }
    
    if (update) {
      db.update(tableName, cvDataTableVal, whereClause, whereArgs);
    } else {
      db.replaceOrThrow(tableName, null, cvDataTableVal);
    }

  }

  /**
   * Return the data stored in the cursor at the given index and given position
   * (ie the given row which the cursor is currently on) as null OR a String.
   * <p>
   * NB: Currently only checks for Strings, long, int, and double.
   *
   * @param c
   * @param i
   * @return
   */
  @SuppressLint("NewApi")
  public static final String getIndexAsString(Cursor c, int i) {
    // If you add additional return types here be sure to modify the javadoc.
    if (i == -1)
      return null;
    if (c.isNull(i)) {
      return null;
    }
    switch (c.getType(i)) {
    case Cursor.FIELD_TYPE_STRING:
      return c.getString(i);
    case Cursor.FIELD_TYPE_FLOAT:
      return Double.toString(c.getDouble(i));
    case Cursor.FIELD_TYPE_INTEGER:
      return Long.toString(c.getLong(i));
    case Cursor.FIELD_TYPE_NULL:
      return c.getString(i);
    default:
    case Cursor.FIELD_TYPE_BLOB:
      throw new IllegalStateException("Unexpected data type in SQLite table");
    }
  }

  public static final Class<?> getIndexDataType(Cursor c, int i) {
    switch (c.getType(i)) {
    case Cursor.FIELD_TYPE_STRING:
      return String.class;
    case Cursor.FIELD_TYPE_FLOAT:
      return Double.class;
    case Cursor.FIELD_TYPE_INTEGER:
      return Long.class;
    case Cursor.FIELD_TYPE_NULL:
      return String.class;
    default:
    case Cursor.FIELD_TYPE_BLOB:
      throw new IllegalStateException("Unexpected data type in SQLite table");
    }
  }

  /**
   * Return the data stored in the cursor at the given index and given position
   * (ie the given row which the cursor is currently on) as null OR whatever
   * data type it is.
   * <p>This does not actually convert data types from one type
   * to the other. Instead, it safely preserves null values and returns boxed
   * data values. If you specify ArrayList or HashMap, it JSON deserializes
   * the value into one of those.
   *
   * @param c
   * @param clazz
   * @param i
   * @return
   */
  @SuppressLint("NewApi")
  public static final <T> T getIndexAsType(Cursor c, Class<T> clazz, int i) {
    // If you add additional return types here be sure to modify the javadoc.
    try {
      if (i == -1)
        return null;
      if (c.isNull(i)) {
        return null;
      }
      if ( clazz == Long.class ) {
        Long l = c.getLong(i);
        return (T) l;
      } else if ( clazz == Integer.class ) {
        Integer l = c.getInt(i);
        return (T) l;
      } else if ( clazz == Double.class ) {
        Double d = c.getDouble(i);
        return (T) d;
      } else if ( clazz == String.class ) {
        String str = c.getString(i);
        return (T) str;
      } else if ( clazz == Boolean.class ) {
        // stored as integers
        Integer l = c.getInt(i);
        return (T) Boolean.valueOf(l != 0);
      } else if ( clazz == ArrayList.class ) {
        // json deserialization of an array
        String str = c.getString(i);
          return (T) ODKFileUtils.mapper.readValue(str, ArrayList.class);
      } else if ( clazz == HashMap.class ) {
        // json deserialization of an object
        String str = c.getString(i);
        return (T) ODKFileUtils.mapper.readValue(str, HashMap.class);
      } else {
        throw new IllegalStateException("Unexpected data type in SQLite table");
      }
    } catch (ClassCastException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unexpected data type conversion failure " + e.toString() + " in SQLite table ");
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unexpected data type conversion failure " + e.toString() + " on SQLite table");
    } catch (JsonMappingException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unexpected data type conversion failure " + e.toString() + " on SQLite table");
    } catch (IOException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unexpected data type conversion failure " + e.toString() + " on SQLite table");
    }
  }
}
