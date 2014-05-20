package org.opendatakit.common.android.utilities;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Scope;
import org.opendatakit.common.android.database.DataModelDatabaseHelper;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;
import org.opendatakit.common.android.utilities.ODKDatabaseUserDefinedTypes;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

public class ODKDatabaseUtils {

  private static final String t = "ODKDatabaseUtils";
  private static final String uriFrag = "uriFragment";
  private static final String contentType = "contentType";

  private ODKDatabaseUtils() {
  }

  /*
   * Perform raw query against current database
   */
  public static final Cursor rawQuery(SQLiteDatabase db, String sql, String[] selectionArgs)
  {
    Cursor c = db.rawQuery(sql, selectionArgs);
    return c;
  }

  /*
   * Query the current database with given parameters
   */
  public static final Cursor query(SQLiteDatabase db, boolean distinct, String table, String[] columns, String selection, String[] selectionArgs, String groupBy, String having, String orderBy, String limit)
  {
    Cursor c = db.query (distinct, table, columns, selection, selectionArgs, groupBy, having, orderBy, limit);
    return c;
  }

  /*
   * Query the current database for all columns
   */
  public static final String[] getAllColumnNames(SQLiteDatabase db, String tableName)
  {
    Cursor cursor = db.rawQuery("SELECT * FROM " + tableName + " LIMIT 1", null);
    String[] colNames = cursor.getColumnNames();

    return colNames;
  }

  /*
   * Get the database type for the user defined type
   */
  private static final String getDBTypeForUserDefinedType(String userDefinedType)
  {
    String dbType = null;

    if (userDefinedType.equals(ODKDatabaseUserDefinedTypes.STRING) || userDefinedType.equals(ODKDatabaseUserDefinedTypes.MIMEURI) ||
        userDefinedType.equals(ODKDatabaseUserDefinedTypes.DATE) || userDefinedType.equals(ODKDatabaseUserDefinedTypes.DATETIME) ||
        userDefinedType.equals(ODKDatabaseUserDefinedTypes.TIME) || userDefinedType.equals(ODKDatabaseUserDefinedTypes.ARRAY))
    {
      dbType = "TEXT";
    } else if (userDefinedType.equals(ODKDatabaseUserDefinedTypes.BOOLEAN)) {
      dbType = "INTEGER"; // 0 or 1
    } else if (userDefinedType.equals(ODKDatabaseUserDefinedTypes.INTEGER)) {
      dbType = "INTEGER";
    } else if (userDefinedType.equals(ODKDatabaseUserDefinedTypes.NUMBER)) {
      dbType = "REAL";
    } else if (userDefinedType.equals(ODKDatabaseUserDefinedTypes.GEOPOINT)) {
      dbType = "REAL";
    }
    else {
     Log.i(t, "getDBTypeForUserDefinedType: Couldn't convert " + userDefinedType + " to a database type");
    }

    return dbType;
  }

  /*
   * Query the current database for user defined columns
   */
  public static final LinkedHashMap<String, String> getUserDefinedColumnsAndTypes(SQLiteDatabase db, String tableName)
  {
    LinkedHashMap<String,String> userDefinedColumns = new LinkedHashMap<String,String>();
    String selection = "_table_id=? AND _is_unit_of_retention=?";
    String[] selectionArgs = {tableName, "1"};
    String[] cols = {"_element_key", "_element_type"};
    Cursor c = db.query(DataModelDatabaseHelper.COLUMN_DEFINITIONS_TABLE_NAME, cols, selection, selectionArgs, null, null, null);

    int elemKeyIndex = c.getColumnIndexOrThrow("_element_key");
    int elemTypeIndex = c.getColumnIndexOrThrow("_element_type");
    c.moveToFirst();
    while (!c.isAfterLast()) {
      userDefinedColumns.put(c.getString(elemKeyIndex),c.getString(elemTypeIndex));
      c.moveToNext();
    }

    c.close();
    return userDefinedColumns;
  }

  /*
   * Get user defined table creation SQL statement
   */
  private static final String getUserDefinedTableCreationStatement(String tableName) {
    /* Resulting string should be the following
    String createTableCmd = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
    + DataTableColumns.ID + " TEXT NOT NULL, "
    + DataTableColumns.ROW_ETAG + " TEXT NULL, "
    + DataTableColumns.SYNC_STATE + " TEXT NOT NULL, "
    + DataTableColumns.CONFLICT_TYPE + " INTEGER NULL,"
    + DataTableColumns.FILTER_TYPE + " TEXT NULL,"
    + DataTableColumns.FILTER_VALUE + " TEXT NULL,"
    + DataTableColumns.FORM_ID + " TEXT NULL,"
    + DataTableColumns.LOCALE + " TEXT NULL,"
    + DataTableColumns.SAVEPOINT_TYPE + " TEXT NULL,"
    + DataTableColumns.SAVEPOINT_TIMESTAMP + " TEXT NOT NULL,"
    + DataTableColumns.SAVEPOINT_CREATOR + " TEXT NULL";*/

    String createTableCmd = "CREATE TABLE IF NOT EXISTS " + tableName + " (";

    String[] cols = getDefaultUserDefinedTableColumns();

    String endSeq = ", ";
    for (int i = 0; i < cols.length; i++) {
      if (i == cols.length - 1) {
        endSeq = "";
      }
      if (cols[i].equals(DataTableColumns.ID) || cols[i].equals(DataTableColumns.SYNC_STATE) || cols[i].equals(DataTableColumns.SAVEPOINT_TIMESTAMP)) {
        createTableCmd = createTableCmd + cols[i] + " TEXT NOT NULL" + endSeq;
      } else if (cols[i].equals(DataTableColumns.ROW_ETAG) || cols[i].equals(DataTableColumns.FILTER_TYPE) || cols[i].equals(DataTableColumns.FILTER_VALUE) ||
          cols[i].equals(DataTableColumns.FORM_ID) || cols[i].equals(DataTableColumns.LOCALE) || cols[i].equals(DataTableColumns.SAVEPOINT_TYPE) ||
          cols[i].equals(DataTableColumns.SAVEPOINT_CREATOR)) {
        createTableCmd = createTableCmd + cols[i] + " TEXT NULL" + endSeq;
      } else if (cols[i].equals(DataTableColumns.CONFLICT_TYPE)) {
        createTableCmd = createTableCmd + cols[i] + " INTEGER NULL" + endSeq;
      }
    }

    return createTableCmd;
  }

  public static final String[] getDefaultUserDefinedTableColumns() {
    String[] cols = {DataTableColumns.ID, DataTableColumns.ROW_ETAG, DataTableColumns.SYNC_STATE,
        DataTableColumns.CONFLICT_TYPE, DataTableColumns.FILTER_TYPE, DataTableColumns.FILTER_VALUE,
        DataTableColumns.FORM_ID, DataTableColumns.LOCALE, DataTableColumns.SAVEPOINT_TYPE,
        DataTableColumns.SAVEPOINT_TIMESTAMP, DataTableColumns.SAVEPOINT_CREATOR};
    return cols;
  }

  /*
   * Create a user defined database table with a transaction
   */
  public static final void createOrOpenDBTable(SQLiteDatabase db, String tableName)
  {
    boolean success = false;
    try {
      db.beginTransaction();
      createDBTable(db, tableName);
      db.setTransactionSuccessful();
      success = true;
    } finally {
      db.endTransaction();
      if (success == false) {
        Log.e(t, "createOrOpenDBTable: Error while adding table " + tableName);
      }
    }
  }
  
  /*
   * Create a user defined database table metadata - table definiton and KVS values
   */
  private static final void createDBTableMetadata(SQLiteDatabase db, String tableName)
  {
    if (tableName == null || tableName.length() <= 0){
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    // Add the table id into table definitions
    ContentValues cvTableDef = new ContentValues();
    cvTableDef.put(TableDefinitionsColumns.TABLE_ID, tableName);
    cvTableDef.put(TableDefinitionsColumns.DB_TABLE_NAME, tableName);
    cvTableDef.put(TableDefinitionsColumns.SYNC_TAG, "");
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME, -1);
    cvTableDef.put(TableDefinitionsColumns.SYNC_STATE, "inserting");
    cvTableDef.put(TableDefinitionsColumns.TRANSACTIONING, 0);

    db.replaceOrThrow (DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, null, cvTableDef);

    // Add the tables values into KVS
    ArrayList<ContentValues> cvTableValKVS = new ArrayList<ContentValues>();

    ContentValues cvTableVal = null;

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "colOrder");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "defaultViewType");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "SPREADSHEET");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "displayName");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "\"" + tableName + "\"");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "groupByCols");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "indexCol");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "sortCol");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "sortOrder");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "TableColorRuleGroup");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "StatusColumn.ruleList");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "[{\"mValue\":\"rest\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"syncStateRest\",\"mForeground\":-16777216,\"mBackground\":-1},{\"mValue\":\"inserting\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"defaultRule_syncStateInserting\",\"mForeground\":-16777216,\"mBackground\":-16711936},{\"mValue\":\"updating\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"defaultRule_syncStateUpdating\",\"mForeground\":-16777216,\"mBackground\":-935891},{\"mValue\":\"conflicting\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"defaultRule_syncStateConflicting\",\"mForeground\":-16777216,\"mBackground\":-65536},{\"mValue\":\"deleting\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"defaultRule_syncStateDeleting\",\"mForeground\":-16777216,\"mBackground\":-12303292}]");
    cvTableValKVS.add(cvTableVal);

    // Now add Tables values into KVS
    for (int i= 0; i < cvTableValKVS.size(); i++) {
      db.replaceOrThrow (DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvTableValKVS.get(i));
    }
  }

  /*
   * Create a new column in a database table with a transaction
   */
  public static final void createNewColumnIntoExistingDBTable(SQLiteDatabase db, String tableName, String colName, String colType)
  {
    boolean success = false;
    try {
      db.beginTransaction();
      createNewColumn(db, tableName, colName, colType);
      db.setTransactionSuccessful();
      success = true;
    } finally {
      db.endTransaction();
      if (success == false) {
        Log.e(t, "createNewColumnIntoExistingDBTable: Error while creating column " + colName + " in table " + tableName);
      }
    }
  }
  
  /*
   * Create a new column in the database
   */
  private static final void createNewColumn(SQLiteDatabase db, String tableName, String colName, String colType)
  {
    // create metadata for new column
    createNewColumnMetadata(db, tableName, colName, colType);

    // Need to address column order
    ContentValues cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "colOrder");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");

    Map<String, String> userDefinedCols = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    StringBuilder tableDefCol = new StringBuilder();

    if (userDefinedCols.isEmpty()) {
      throw new IllegalArgumentException(t + ": No user defined columns exist in " + tableName);
    }

    Iterator<Entry<String, String>> itr = userDefinedCols.entrySet().iterator();
    tableDefCol.append("\"").append(itr.next().getKey().toString()).append("\"");
    while (itr.hasNext()) {
      tableDefCol.append(", \"").append(itr.next().getKey().toString()).append("\"");
    }

    String colOrderVal = "[" + tableDefCol.toString() + "]";
    cvTableVal.put(KeyValueStoreColumns.VALUE, colOrderVal);

    // Now add Tables values into KVS
    db.replaceOrThrow (DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvTableVal);

    // Have to create a new table with added column
    reformTable(db, tableName, colName, colType);
  }

  /**
   * Construct a temporary database table to save the current table,
   * then creates a new table and copies the data back in.
   *
   */
  private static final void reformTable(SQLiteDatabase db, String tableName, String colName, String colType) {

    String[] colNames = ODKDatabaseUtils.getAllColumnNames(db, tableName);

    if (colNames.length <= 0) {
      throw new IllegalArgumentException(t + ": " + tableName + " does not contain any columns");
    }

    StringBuilder tableDefCol = new StringBuilder();
    tableDefCol.append(colNames[0]);

    for (int i = 1; i < colNames.length; i++) {
      tableDefCol.append(", ").append(colNames[i]);
    }

    String csv = tableDefCol.toString();

    db.execSQL("CREATE TEMPORARY TABLE backup_(" + csv + ")");
    db.execSQL("INSERT INTO backup_(" + csv + ") SELECT " + csv + " FROM " + tableName);
    db.execSQL("DROP TABLE " + tableName);

    // Create Table Code
    Map<String, String> userDefinedCols = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    StringBuilder userDefColStrBld = new StringBuilder();

    for (Map.Entry<String, String> entry : userDefinedCols.entrySet()) {
      String key = entry.getKey();
      String type = entry.getValue();
      userDefColStrBld.append(", ").append(key).append(" ").append(getDBTypeForUserDefinedType(type)).append(" NULL");
    }

    String toExecute = getUserDefinedTableCreationStatement(tableName);

    if (userDefinedCols != null) {
      toExecute = toExecute + userDefColStrBld.toString();
    }
    toExecute = toExecute + ")";

    db.execSQL(toExecute);

    db.execSQL("INSERT INTO " + tableName + "(" + csv + ") SELECT " + csv + " FROM backup_");
    db.execSQL("DROP TABLE backup_");
  }

  /*
   * Create a user defined database table with a transaction
   */
  public static final void createOrOpenDBTableWithColumns(SQLiteDatabase db, String tableName, LinkedHashMap<String,String>columns)
  {
    boolean success = false;
    try {
      db.beginTransaction();
      createDBTableWithColumns(db, tableName, columns);
      db.setTransactionSuccessful();
      success = true;
    } finally {
      db.endTransaction();
      if (success == false) {
        
        // Get the names of the columns
        StringBuilder colNames = new StringBuilder();
        if (columns != null) {
          for (Entry <String, String> entry : columns.entrySet()) {
              colNames.append(" ").append(entry.getKey()).append(",");
          }
          if (colNames != null && colNames.length() > 0) {
            colNames.deleteCharAt(colNames.length() - 1);
            Log.e(t, "createOrOpenDBTableWithColumns: Error while adding table " + tableName + " with columns:" + colNames.toString()); 
          }
        } else {
          Log.e(t, "createOrOpenDBTableWithColumns: Error while adding table " + tableName + " with columns: null");
        }
      }
    }
  }
  

  /*
   * Create a user defined database table metadata - table definiton and KVS values
   */
  private static final void createDBTableWithColumns(SQLiteDatabase db, String tableName, LinkedHashMap<String,String>columns)
  {
    if (tableName == null || tableName.length() <= 0){
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    String createTableCmd = getUserDefinedTableCreationStatement(tableName);

    StringBuilder createTableCmdWithCols = new StringBuilder();
    createTableCmdWithCols.append(createTableCmd);

    for (Map.Entry<String, String> column  : columns.entrySet()) {
      String type = column.getValue();
      String name = column.getKey();
      String dbType = getDBTypeForUserDefinedType(type);

      if (type.equals(ODKDatabaseUserDefinedTypes.GEOPOINT)) {
        createTableCmdWithCols.append(", ").append(name).append("_latitude ").append(dbType).append(" NULL");
        createTableCmdWithCols.append(", ").append(name).append("_longitude ").append(dbType).append(" NULL");
        createTableCmdWithCols.append(", ").append(name).append("_altitude ").append(dbType).append(" NULL");
        createTableCmdWithCols.append(", ").append(name).append("_accuracy ").append(dbType).append(" NULL");
      } else {
        if (dbType != null) {
          createTableCmdWithCols.append(", ").append(name).append(" ").append(dbType).append(" NULL");
        } else {
          Log.i(t, "Didn't add " + name + " unrecognized type " + type + " to the database");
        }
      }
    }

    createTableCmdWithCols.append(");");

    db.execSQL(createTableCmdWithCols.toString());
    
    // Create the metadata for the table - table def and KVS
    createDBTableMetadata(db, tableName);

    // Now need to call the function to write out all the column values
    for (Map.Entry<String, String> column  : columns.entrySet()) {
        String type = column.getValue();
        String name = column.getKey();
        createNewColumnMetadata(db, tableName, name, type);
    }

    // Need to address column order
    ContentValues cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "colOrder");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");

    LinkedHashMap<String, String> userDefinedCols = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    StringBuilder tableDefCol = new StringBuilder();

    if (userDefinedCols.isEmpty()) {
      throw new IllegalArgumentException(t + ": No user defined columns exist in " + tableName);
    }

    // Now need to call the function to write out all the column values
    for (Map.Entry<String, String> column  : userDefinedCols.entrySet()) {
      tableDefCol.append("\"").append(column.getKey().toString()).append("\"").append(",");
    }

    // Get rid of the last character which is an extra comma
    tableDefCol.deleteCharAt(tableDefCol.length() - 1);

    Log.i(t, "Column order for table " +  tableName + " is " + tableDefCol.toString());
    String colOrderVal = "[" + tableDefCol.toString() + "]";
    cvTableVal.put(KeyValueStoreColumns.VALUE, colOrderVal);

    // Now add Tables values into KVS
    db.replaceOrThrow (DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvTableVal);
  }

  /*
   * Create a new column metadata in the database - add column values to KVS and column definitions
   */
  private static final void createNewColumnMetadata(SQLiteDatabase db, String tableName, String colName, String colType)
  {
    ArrayList<ContentValues> cvColValKVS = new ArrayList<ContentValues>();

    ContentValues cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, "Column");
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, "displayVisible");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "boolean");
    cvColVal.put(KeyValueStoreColumns.VALUE, "true");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, "Column");
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, "displayName");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    String colDisplayName = "\"" + colName + "\"";
    cvColVal.put(KeyValueStoreColumns.VALUE, colDisplayName);
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, "Column");
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, "displayChoicesList");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvColVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, "Column");
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, "displayFormat");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvColVal.put(KeyValueStoreColumns.VALUE, "");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, "Column");
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, "joins");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvColVal.put(KeyValueStoreColumns.VALUE, "");
    cvColValKVS.add(cvColVal);

    // Now add all this data into the database
    for (int i= 0; i < cvColValKVS.size(); i++) {
      db.replaceOrThrow (DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvColValKVS.get(i));
    }

    // Create column definition
    ArrayList<ContentValues> cvColDefValArray = new ArrayList<ContentValues>();
    ContentValues cvColDefVal = null;
  
    if (colType.equals(ODKDatabaseUserDefinedTypes.MIMEURI)) {
  
      String colNameUriFrag = colName + "_" + uriFrag;
      String colNameConType = colName + "_" + contentType;
  
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.MIMEURI);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "[\"" + colNameUriFrag + "\",\"" + colNameConType + "\"]");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 1);
      cvColDefValArray.add(cvColDefVal);
  
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colNameUriFrag);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, uriFrag);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.STRING);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "null");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 0);
      cvColDefValArray.add(cvColDefVal);
  
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colNameConType);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, contentType);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.STRING);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "null");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 0);
      cvColDefValArray.add(cvColDefVal);
  
    } else if (colType.equals(ODKDatabaseUserDefinedTypes.ARRAY)) {
      String itemsStr = "items";
      String colNameItem = colName + "_" + itemsStr;
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.ARRAY);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "[\"" + colNameItem + "\"]");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 1);
      cvColDefValArray.add(cvColDefVal);
  
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colNameItem);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, itemsStr);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.STRING);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "null");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 0);
      cvColDefValArray.add(cvColDefVal);
  
    } else if (colType.equals(ODKDatabaseUserDefinedTypes.DATE) ||
        colType.equals(ODKDatabaseUserDefinedTypes.DATETIME) ||
        colType.equals(ODKDatabaseUserDefinedTypes.TIME)) {
   
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, colType);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "[]");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 1);
      cvColDefValArray.add(cvColDefVal);
  
    }  else if (colType.equals(ODKDatabaseUserDefinedTypes.GEOPOINT)) {
      String latStr = "latitude";
      String longStr = "longitude";
      String altStr = "altitude";
      String accStr = "accuracy";
      String colNameLat = colName + "_" + latStr;
      String colNameLong = colName + "_" + longStr;
      String colNameAlt = colName + "_" + altStr;
      String colNameAcc = colName + "_" + accStr;
  
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.GEOPOINT);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "[\"" + colNameLat + "\",\"" + colNameLong + "\",\"" + colNameAlt + "\",\"" + colNameAcc + "\"]");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 0);
      cvColDefValArray.add(cvColDefVal);
  
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colNameLat);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, latStr);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.NUMBER);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "null");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 1);
      cvColDefValArray.add(cvColDefVal);
  
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colNameLong);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, longStr);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.NUMBER);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "null");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 1);
      cvColDefValArray.add(cvColDefVal);
  
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colNameAlt);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, altStr);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.NUMBER);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "null");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 1);
      cvColDefValArray.add(cvColDefVal);
  
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colNameAcc);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, accStr);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, ODKDatabaseUserDefinedTypes.NUMBER);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "null");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 1);
      cvColDefValArray.add(cvColDefVal);
    }
    else {
      cvColDefVal = new ContentValues();
      cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, colName);
      cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, colType);
      cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "null");
      cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 1);
      cvColDefValArray.add(cvColDefVal);
    }

    // Now add all this data into the database
    for (int i = 0; i < cvColDefValArray.size(); i++) {
      db.replaceOrThrow (DataModelDatabaseHelper.COLUMN_DEFINITIONS_TABLE_NAME, null, cvColDefValArray.get(i));
    }
  }
    
  /*
   * Create a user defined database table
   */
  private static final void createDBTable(SQLiteDatabase db, String tableName)
  {
    if (tableName == null || tableName.length() <= 0){
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }
    
    String createTableCmd = getUserDefinedTableCreationStatement(tableName) + ");";

    db.execSQL(createTableCmd);
    
    // Create table metatadata
    createDBTableMetadata(db, tableName);
  }
  
  /*
   * Write data into a user defined database table
   */
  public static final void writeDataIntoExistingDBTable(SQLiteDatabase db, String tableName, ContentValues cvValues)
  {
    Map<String, String> userDefCols = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    
    if (userDefCols.isEmpty())
    {
      throw new IllegalArgumentException(t + ": No user defined columns exist in " + tableName + " - cannot insert data");
    }

    if (cvValues.size() <= 0)
    {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableName);
    }
    
    writeDataAndMetadataIntoExistingDBTable(db, tableName, cvValues);
    
  }
  
  /*
   * Write data into a user defined database table
   */
  public static final void writeDataIntoExistingDBTableWithId(SQLiteDatabase db, String tableName, ContentValues cvValues, String uuid)
  {
    Map<String, String> userDefCols = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    
    if (userDefCols.isEmpty())
    {
      throw new IllegalArgumentException(t + ": No user defined columns exist in " + tableName + " - cannot insert data");
    }

    if (cvValues.size() <= 0)
    {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableName);
    }
    
    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.put(DataTableColumns.ID, uuid);  
    cvDataTableVal.putAll(cvValues);

    writeDataAndMetadataIntoExistingDBTable(db, tableName, cvDataTableVal);
    
  }

  /*
   * Write data into a user defined database table
   */
  public static final void writeDataAndMetadataIntoExistingDBTable(SQLiteDatabase db, String tableName, ContentValues cvValues)
  {
    String nullString = null;

    if (cvValues.size() <= 0)
    {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableName);
    }

    // manufacture a rowId for this record...
    String rowId = "uuid:" + UUID.randomUUID().toString();
    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
    
    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.putAll(cvValues);
    
    if (!cvDataTableVal.containsKey(DataTableColumns.ID)) {
      cvDataTableVal.put(DataTableColumns.ID, rowId);      
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.ROW_ETAG)) {
      cvDataTableVal.put(DataTableColumns.ROW_ETAG, nullString);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SYNC_STATE)) {
      cvDataTableVal.put(DataTableColumns.SYNC_STATE, SyncState.inserting.name());
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.CONFLICT_TYPE)) {
      cvDataTableVal.put(DataTableColumns.CONFLICT_TYPE, nullString);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.FILTER_TYPE)) {
      cvDataTableVal.put(DataTableColumns.FILTER_TYPE, Scope.EMPTY_SCOPE.getType().name());    
    }
    
    if (!cvDataTableVal.containsKey(DataTableColumns.FILTER_VALUE)) {
      cvDataTableVal.put(DataTableColumns.FILTER_VALUE, Scope.EMPTY_SCOPE.getValue());
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.FORM_ID)) {
      cvDataTableVal.put(DataTableColumns.FORM_ID, nullString);    
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.LOCALE)) {
      cvDataTableVal.put(DataTableColumns.LOCALE, Locale.ENGLISH.getLanguage());
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TYPE)) {
      cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP)) {
      cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_CREATOR)) {
      cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);
    }
    
    db.replaceOrThrow (tableName, null, cvDataTableVal);
  }
}
