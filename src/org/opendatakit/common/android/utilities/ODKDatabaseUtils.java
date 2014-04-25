package org.opendatakit.common.android.utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

public class ODKDatabaseUtils {

  private static final String t = "ODKDatabaseUtils";

  private ODKDatabaseUtils() {
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
   * Query the current database for user defined columns
   */
  public static final Map<String, String> getUserDefinedColumnsAndTypes(SQLiteDatabase db, String tableName)
  {
    Map<String,String> userDefinedColumns = new HashMap<String,String>();
    String selection = "_table_id=? AND _is_unit_of_retention=?";
    String[] selectionArgs = {tableName, "1"};
    String[] cols = {"_element_name", "_element_type"};
    Cursor c = db.query(DataModelDatabaseHelper.COLUMN_DEFINITIONS_TABLE_NAME, cols, selection, selectionArgs, null, null, null);

    int elemNameIndex = c.getColumnIndexOrThrow("_element_name");
    int elemTypeIndex = c.getColumnIndexOrThrow("_element_type");
    c.moveToFirst();
    while (!c.isAfterLast()) {
      userDefinedColumns.put(c.getString(elemNameIndex),c.getString(elemTypeIndex));
      c.moveToNext();
    }

    c.close();
    return userDefinedColumns;
  }

  /*
   * Create a user defined database table with a transaction
   */
  public static final void createOrOpenDBTable(SQLiteDatabase db, String tableName)
  {
    try {
      db.beginTransaction();
      createDBTable(db, tableName);
      db.setTransactionSuccessful();
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(t, "Transaction error while adding table " + tableName);
    } finally {
      db.endTransaction();
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

    String createSensorTableCmd = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
         + DataTableColumns.ID + " TEXT NOT NULL, "
         + DataTableColumns.ROW_ETAG + " TEXT NULL, "
         + DataTableColumns.SYNC_STATE + " TEXT NOT NULL, "
         + DataTableColumns.CONFLICT_TYPE + " INTEGER NULL,"
         + DataTableColumns.FILTER_TYPE + " TEXT NULL,"
         + DataTableColumns.FILTER_VALUE + " TEXT NULL,"
         + DataTableColumns.FORM_ID + " TEXT NULL,"
         + DataTableColumns.LOCALE + " TEXT NULL,"
         + DataTableColumns.SAVEPOINT_TYPE + " TEXT NOT NULL,"
         + DataTableColumns.SAVEPOINT_TIMESTAMP + " TEXT NOT NULL,"
         + DataTableColumns.SAVEPOINT_CREATOR + " TEXT NULL);";

      try {
        db.execSQL(createSensorTableCmd);
      } catch (Exception e) {
        Log.e(t, "Error while creating table " + tableName);
        e.printStackTrace();
      }

    // Add the table id into table definitions
    ContentValues cvTableDef = new ContentValues();
    cvTableDef.put(TableDefinitionsColumns.TABLE_ID, tableName);
    cvTableDef.put(TableDefinitionsColumns.DB_TABLE_NAME, tableName);
    cvTableDef.put(TableDefinitionsColumns.SYNC_TAG, "");
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME, -1);
    cvTableDef.put(TableDefinitionsColumns.SYNC_STATE, "inserting");
    cvTableDef.put(TableDefinitionsColumns.TRANSACTIONING, 0);

    try {
      db.replaceOrThrow (DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, null, cvTableDef);
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(t, "Error while trying to add a table definition row for sensor data table");
    }

    // Add the tables values into KVS
    ArrayList<ContentValues> cvTableValKVS = new ArrayList<ContentValues>();

    ContentValues cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "tableType");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "data");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "TableColorRuleGroup");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "StatusColumn.ruleList");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "[{\"mValue\":\"rest\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"syncStateRest\",\"mForeground\":-16777216,\"mBackground\":-1},{\"mValue\":\"inserting\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"defaultRule_syncStateInserting\",\"mForeground\":-16777216,\"mBackground\":-16711936},{\"mValue\":\"updating\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"defaultRule_syncStateUpdating\",\"mForeground\":-16777216,\"mBackground\":-935891},{\"mValue\":\"conflicting\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"defaultRule_syncStateConflicting\",\"mForeground\":-16777216,\"mBackground\":-65536},{\"mValue\":\"deleting\",\"mElementKey\":\"_sync_state\",\"mOperator\":\"EQUAL\",\"mId\":\"defaultRule_syncStateDeleting\",\"mForeground\":-16777216,\"mBackground\":-12303292}]");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "displayName");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, tableName);
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "colOrder");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "primeCols");
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
    cvTableVal.put(KeyValueStoreColumns.KEY, "indexCol");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "currentViewType");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "Spreadsheet");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "summaryDisplayFormat");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "currentQuery");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    // Now add Tables values into KVS
    try {
      for (int i= 0; i < cvTableValKVS.size(); i++) {
        db.replaceOrThrow (DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvTableValKVS.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(t, "Error while trying to write table values into the active key value store " +  tableName);
    }

  }

  /*
   * Write data into a user defined database table
   */
  public static final void writeDataIntoExistingDBTable(SQLiteDatabase db, String tableName, ContentValues cvValues)
  {
    ContentValues cvDataTableVal;
    String nullString = null;

    Map<String, String> userDefCols = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    if (userDefCols.isEmpty())
    {
      throw new IllegalArgumentException(t + ": No user defined columns exist in " + tableName + " - cannot insert data");
    }

    if (cvValues.size() <= 0)
    {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableName);
    }

    // manufacture a rowId for this record...
    String rowId = "uuid:" + UUID.randomUUID().toString();
    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    cvDataTableVal = new ContentValues();
    cvDataTableVal.put(DataTableColumns.ID, rowId);
    cvDataTableVal.put(DataTableColumns.ROW_ETAG, nullString);
    cvDataTableVal.put(DataTableColumns.SYNC_STATE, SyncState.inserting.name());
    cvDataTableVal.put(DataTableColumns.CONFLICT_TYPE, nullString);
    cvDataTableVal.put(DataTableColumns.FILTER_TYPE, Scope.EMPTY_SCOPE.getType().name());
    cvDataTableVal.put(DataTableColumns.FILTER_VALUE, Scope.EMPTY_SCOPE.getValue());
    cvDataTableVal.put(DataTableColumns.FORM_ID, nullString);
    cvDataTableVal.put(DataTableColumns.LOCALE, Locale.ENGLISH.getLanguage());
    cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
    cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);
    cvDataTableVal.putAll(cvValues);

    try {
      db.replaceOrThrow (tableName, null, cvDataTableVal);
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(t, "Error - Could NOT write data into table " + tableName);
    }
  }

  /*
   * Create a new column in a database table with a transaction
   */
  public static final void createNewColumnIntoExistingDBTable(SQLiteDatabase db, String tableName, String colName, String colType)
  {
    try {
      db.beginTransaction();
      createNewColumn(db, tableName, colName, colType);
      db.setTransactionSuccessful();
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(t, "Transaction error while creating column " + colName + " in table " + tableName);
    } finally {
      db.endTransaction();
    }
  }

  /*
   * Create a new column in the database
   */
  private static final void createNewColumn(SQLiteDatabase db, String tableName, String colName, String colType)
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
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    String colDisplayName = "\"" + colName + "\"";
    cvColVal.put(KeyValueStoreColumns.VALUE, colDisplayName);
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, "Column");
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, "displayChoicesList");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
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
    cvColVal.put(KeyValueStoreColumns.KEY, "smsIn");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "boolean");
    cvColVal.put(KeyValueStoreColumns.VALUE, "true");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, "Column");
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, "smsOut");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "boolean");
    cvColVal.put(KeyValueStoreColumns.VALUE, "true");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, "Column");
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, "smsLabel");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvColVal.put(KeyValueStoreColumns.VALUE, "");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvColVal.put(KeyValueStoreColumns.PARTITION, "Column");
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, "footerMode");
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvColVal.put(KeyValueStoreColumns.VALUE, "none");
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
    try {
      for (int i= 0; i < cvColValKVS.size(); i++) {
        db.replaceOrThrow (DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvColValKVS.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(t, "Error while writing column information to active key value store for table " +  tableName);
    }

    // Create column definition
    ContentValues cvColDefVal = new ContentValues();
    cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableName);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, colName);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, colType);
    cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, "null");
    cvColDefVal.put(ColumnDefinitionsColumns.IS_UNIT_OF_RETENTION, 1);

    // Now add all this data into the database
    try {
      db.replaceOrThrow (DataModelDatabaseHelper.COLUMN_DEFINITIONS_TABLE_NAME, null, cvColDefVal);
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(t, "Error while writing to _column_definitions for table " + tableName);
    }

    // Need to address column order
    ContentValues cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableName);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "Table");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, "default");
    cvTableVal.put(KeyValueStoreColumns.KEY, "colOrder");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");

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
    try {
      db.replaceOrThrow (DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvTableVal);
    } catch (Exception e) {
      e.printStackTrace();
      Log.e(t, "Error while writing colOrder to KVS for table " +  tableName);
    }

    // Have to create a new table with added column
    reformTable(db, tableName, colName, colType);
  }

  /**
   * Construct a temporary database table to save the current table,
   * then creates a new table and copies the data back in.
   *
   */
  public static final void reformTable(SQLiteDatabase db, String tableName, String colName, String colType) {

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
      userDefColStrBld.append(", ").append(key).append(" ").append(type.toUpperCase()).append(" NULL");
    }

    String toExecute = "CREATE TABLE " + tableName + "("
      + DataTableColumns.ID + " TEXT NOT NULL, "
      + DataTableColumns.ROW_ETAG + " TEXT NULL, "
      + DataTableColumns.SYNC_STATE + " TEXT NOT NULL, "
      + DataTableColumns.CONFLICT_TYPE + " INTEGER NULL,"
      + DataTableColumns.FILTER_TYPE + " TEXT NULL,"
      + DataTableColumns.FILTER_VALUE + " TEXT NULL,"
      + DataTableColumns.FORM_ID + " TEXT NULL,"
      + DataTableColumns.LOCALE + " TEXT NULL,"
      + DataTableColumns.SAVEPOINT_TYPE + " TEXT NOT NULL,"
      + DataTableColumns.SAVEPOINT_TIMESTAMP + " TEXT NOT NULL,"
      + DataTableColumns.SAVEPOINT_CREATOR + " TEXT NULL";

    if (userDefinedCols != null) {
      toExecute = toExecute + userDefColStrBld.toString();
    }
    toExecute = toExecute + ")";

    db.execSQL(toExecute);

    db.execSQL("INSERT INTO " + tableName + "(" + csv + ") SELECT " + csv + " FROM backup_");
    db.execSQL("DROP TABLE backup_");
  }
}
