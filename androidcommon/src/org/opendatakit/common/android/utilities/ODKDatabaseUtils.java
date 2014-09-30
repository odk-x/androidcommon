package org.opendatakit.common.android.utilities;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColorRule;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.ElementDataType;
import org.opendatakit.common.android.data.ElementType;
import org.opendatakit.common.android.data.KeyValueStoreEntry;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.data.UserTable;
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
    Collections.sort(adminColumns);
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
    Collections.sort(exportColumns);
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

  /**
   * Get a {@link UserTable} for this table based on the given where clause.
   * All columns from the table are returned.
   * <p>
   * It performs SELECT * FROM table whereClause.
   * <p>
   * @param whereClause the whereClause for the selection, beginning with
   * "WHERE". Must include "?" instead of actual values, which are instead
   * passed in the selectionArgs.
   * @param selectionArgs the selection arguments for the where clause.
   * @return
   */
  public static UserTable rawSqlQuery(SQLiteDatabase db, String appName, String tableId, 
      List<String> persistedColumns, String whereClause, String[] selectionArgs,
      String[] groupBy, String having, String orderByElementKey, String orderByDirection) {
    Cursor c = null;
    try {
      StringBuilder s = new StringBuilder();
      s.append("SELECT * FROM \"").append(tableId).append("\" ");
      if ( whereClause != null && whereClause.length() != 0 ) {
        s.append(" WHERE ").append(whereClause);
      }
      if ( groupBy != null && groupBy.length != 0 ) {
        s.append(" GROUP BY ");
        boolean first = true;
        for ( String elementKey : groupBy ) {
          if (!first) {
            s.append(", ");
          }
          first = false;
          s.append(elementKey);
        }
        if ( having != null && having.length() != 0 ) {
          s.append(" HAVING ").append(having);
        }
      }
      if ( orderByElementKey != null && orderByElementKey.length() != 0 ) {
        s.append(" ORDER BY ").append(orderByElementKey);
        if ( orderByDirection != null && orderByDirection.length() != 0 ) {
          s.append(" ").append(orderByDirection);
        } else {
          s.append(" ASC");
        }
      }
      String sqlQuery = s.toString();
      c = db.rawQuery(sqlQuery, selectionArgs);
      UserTable table = new UserTable(c, appName, tableId, 
                persistedColumns, whereClause, selectionArgs,
                groupBy, having, orderByElementKey, orderByDirection);
      return table;
    } finally {
      if ( c != null && !c.isClosed() ) {
        c.close();
      }
    }
  }
  
  public static final UserTable getDataInExistingDBTableWithId(SQLiteDatabase db, 
      String appName, String tableId, List<String> persistedColumns, String rowId ) {
    
    UserTable table = rawSqlQuery(db, appName, tableId, 
        persistedColumns, DataTableColumns.ID + "=?", new String[]{ rowId },
        null, null, DataTableColumns.SAVEPOINT_TIMESTAMP, "DESC");
    
    return table;
  }
  /*
   * Query the current database for all columns
   */
  public static final String[] getAllColumnNames(SQLiteDatabase db, String tableId) {
    Cursor cursor = db.rawQuery("SELECT * FROM " + tableId + " LIMIT 1", null);
    String[] colNames = cursor.getColumnNames();

    return colNames;
  }

  /*
   * Query the current database for all user defined columns
   * 
   * Returns both the grouping columns and the actual persisted
   * columns.
   */
  public static final ArrayList<Column> getUserDefinedColumns(
      SQLiteDatabase db, String tableId) {
    ArrayList<Column> userDefinedColumns = new ArrayList<Column>();
    String selection = ColumnDefinitionsColumns.TABLE_ID + "=?";
    String[] selectionArgs = { tableId };
    String[] cols = { ColumnDefinitionsColumns.ELEMENT_KEY,
        ColumnDefinitionsColumns.ELEMENT_NAME,
        ColumnDefinitionsColumns.ELEMENT_TYPE,
        ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS };
    Cursor c = null;
    try {
      c = db.query(DataModelDatabaseHelper.COLUMN_DEFINITIONS_TABLE_NAME, cols, selection,
        selectionArgs, null, null, ColumnDefinitionsColumns.ELEMENT_KEY + " ASC");

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
      if ( c != null && !c.isClosed() ) {
        c.close();
      }
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

  public static ArrayList<String> getAllTableIds(SQLiteDatabase db) {
    ArrayList<String> tableIds = new ArrayList<String>();
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
  
  public static void deleteTableAndData(SQLiteDatabase db, final String appName, final String tableId) {
    boolean dbWithinTransaction = db.inTransaction();
    try {
      String whereClause = TableDefinitionsColumns.TABLE_ID + " = ?";
      String[] whereArgs = { tableId };

      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }

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

      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }

    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }

    // And delete the files from the SDCard...
    String tableDir = ODKFileUtils.getTablesFolder(appName, tableId);
    try {
      FileUtils.deleteDirectory(new File(tableDir));
    } catch (IOException e1) {
      e1.printStackTrace();
      throw new IllegalStateException("Unable to delete the " + tableDir + " directory", e1);
    }

    String assetsCsvDir = ODKFileUtils.getAssetsFolder(appName) + "/csv";
    try {
      Collection<File> files = FileUtils.listFiles(new File(assetsCsvDir), new IOFileFilter() {

        @Override
        public boolean accept(File file) {
          String[] parts = file.getName().split("\\.");
          return (parts[0].equals(tableId) && parts[parts.length - 1].equals("csv") && (parts.length == 2
              || parts.length == 3 || (parts.length == 4 && parts[parts.length - 2]
              .equals("properties"))));
        }

        @Override
        public boolean accept(File dir, String name) {
          String[] parts = name.split("\\.");
          return (parts[0].equals(tableId) && parts[parts.length - 1].equals("csv") && (parts.length == 2
              || parts.length == 3 || (parts.length == 4 && parts[parts.length - 2]
              .equals("properties"))));
        }
      }, new IOFileFilter() {

        // don't traverse into directories
        @Override
        public boolean accept(File arg0) {
          return false;
        }

        // don't traverse into directories
        @Override
        public boolean accept(File arg0, String arg1) {
          return false;
        }
      });

      FileUtils.deleteDirectory(new File(tableDir));
      for (File f : files) {
        FileUtils.deleteQuietly(f);
      }
    } catch (IOException e1) {
      e1.printStackTrace();
      throw new IllegalStateException("Unable to delete the " + tableDir + " directory", e1);
    }
  }

  /*
   * Get user defined table creation SQL statement
   */
  private static final String getUserDefinedTableCreationStatement(String tableId) {
    /*
     * Resulting string should be the following String createTableCmd =
     * "CREATE TABLE IF NOT EXISTS " + tableId + " (" + DataTableColumns.ID +
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

    String createTableCmd = "CREATE TABLE IF NOT EXISTS " + tableId + " (";

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

  public static final void updateDBTableETags(SQLiteDatabase db, String tableId, 
        String schemaETag, String lastDataETag ) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }
    
    ContentValues cvTableDef = new ContentValues();
    cvTableDef.put(TableDefinitionsColumns.SCHEMA_ETAG, schemaETag);
    cvTableDef.put(TableDefinitionsColumns.LAST_DATA_ETAG, lastDataETag);
    
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }
      db.update(DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, cvTableDef, TableDefinitionsColumns.TABLE_ID + "=?", new String[]{ tableId});
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }
  }

  public static final void updateDBTableLastSyncTime(SQLiteDatabase db, String tableId ) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }
    
    ContentValues cvTableDef = new ContentValues();
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME, 
        TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));
    
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }
      db.update(DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, cvTableDef, TableDefinitionsColumns.TABLE_ID + "=?", new String[]{ tableId});
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }
  }

  public static final TableDefinitionEntry getTableDefinitionEntry(SQLiteDatabase db, String tableId) {

    TableDefinitionEntry e = null;
    Cursor c = null;
    try {
      StringBuilder b = new StringBuilder();
      ArrayList<String> selArgs = new ArrayList<String>();
      b.append(KeyValueStoreColumns.TABLE_ID).append("=?");
      selArgs.add(tableId);

      c = db.query(DataModelDatabaseHelper.TABLE_DEFS_TABLE_NAME, null, 
          b.toString(), selArgs.toArray(new String[selArgs.size()]), null, null, null);
      if ( c.moveToFirst() ) {
        int idxSchemaETag = c.getColumnIndex(TableDefinitionsColumns.SCHEMA_ETAG);
        int idxLastDataETag = c.getColumnIndex(TableDefinitionsColumns.LAST_DATA_ETAG);
        int idxLastSyncTime = c.getColumnIndex(TableDefinitionsColumns.LAST_SYNC_TIME);

        if ( c.getCount() != 1 ) {
          throw new IllegalStateException("Two or more TableDefinitionEntry records found for tableId " + tableId);
        }
        
        e = new TableDefinitionEntry(tableId);
        e.setSchemaETag(c.getString(idxSchemaETag));
        e.setLastDataETag(c.getString(idxLastDataETag));
        e.setLastSyncTime(c.getString(idxLastSyncTime));
      }
    } finally {
      if ( c != null && !c.isClosed() ) {
        c.close();
      }
    }
    return e;
  }
  
  public static final void replaceDBTableMetadata(SQLiteDatabase db, KeyValueStoreEntry entry) {
    ContentValues values = new ContentValues();
    values.put(KeyValueStoreColumns.TABLE_ID, entry.tableId);
    values.put(KeyValueStoreColumns.PARTITION, entry.partition);
    values.put(KeyValueStoreColumns.ASPECT, entry.aspect);
    values.put(KeyValueStoreColumns.VALUE_TYPE, entry.type);
    values.put(KeyValueStoreColumns.VALUE, entry.value);
    
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }
      db.replace(DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, values);
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }
  }
  
  public static final void replaceDBTableMetadata(SQLiteDatabase db, String tableId, List<KeyValueStoreEntry> metadata, boolean clear) {
    
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }
      
      if ( clear ) {
        db.delete( DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, 
                   KeyValueStoreColumns.TABLE_ID + "=?", new String[]{ tableId });
      }
      for ( KeyValueStoreEntry e : metadata ) {
        ContentValues values = new ContentValues();
        if ( !tableId.equals(e.tableId) ) {
          throw new IllegalArgumentException("updateDBTableMetadata: expected all kvs entries to share the same tableId");
        }
        if ( e.value == null || e.value.trim().length() == 0 ) {
          deleteDBTableMetadata(db, e.tableId, e.partition, e.aspect, e.key);
        } else {
          values.put(KeyValueStoreColumns.TABLE_ID, e.tableId);
          values.put(KeyValueStoreColumns.PARTITION, e.partition);
          values.put(KeyValueStoreColumns.ASPECT, e.aspect);
          values.put(KeyValueStoreColumns.KEY, e.key);
          values.put(KeyValueStoreColumns.VALUE_TYPE, e.type);
          values.put(KeyValueStoreColumns.VALUE, e.value);
          db.replace(DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, values);
        }
      }
      
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }
  }

  /**
   * The deletion filter includes all non-null arguments. If all arguments (except the db) 
   * are null, then all properties are removed.
   * 
   * @param db
   * @param tableId
   * @param partition
   * @param aspect
   * @param key
   */
  public static final void 
    deleteDBTableMetadata(SQLiteDatabase db, String tableId, String partition, String aspect, String key) {

    StringBuilder b = new StringBuilder();
    ArrayList<String> selArgs = new ArrayList<String>();
    if ( tableId != null ) {
      b.append(KeyValueStoreColumns.TABLE_ID).append("=?");
      selArgs.add(tableId);
    }
    if ( partition != null ) {
      if ( b.length() != 0 ) {
        b.append(" AND ");
      }
      b.append(KeyValueStoreColumns.PARTITION).append("=?");
      selArgs.add(partition);
    }
    if ( aspect != null ) {
      if ( b.length() != 0 ) {
        b.append(" AND ");
      }
      b.append(KeyValueStoreColumns.ASPECT).append("=?");
      selArgs.add(aspect);
    }
    if ( key != null ) {
      if ( b.length() != 0 ) {
        b.append(" AND ");
      }
      b.append(KeyValueStoreColumns.KEY).append("=?");
      selArgs.add(key);
    }
    
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }

      db.delete(DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, 
          b.toString(), 
          selArgs.toArray(new String[selArgs.size()]));
      
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }
  }
  
  /**
   * Filters results by all non-null field values.
   * 
   * @param db
   * @param tableId
   * @param partition
   * @param aspect
   * @param key
   * @return
   */
  public static final ArrayList<KeyValueStoreEntry> 
    getDBTableMetadata(SQLiteDatabase db, String tableId, String partition, String aspect, String key) {

    ArrayList<KeyValueStoreEntry> entries = new ArrayList<KeyValueStoreEntry>();
    
    Cursor c = null;
    try {
      StringBuilder b = new StringBuilder();
      ArrayList<String> selArgs = new ArrayList<String>();
      if ( tableId != null ) {
        b.append(KeyValueStoreColumns.TABLE_ID).append("=?");
        selArgs.add(tableId);
      }
      if ( partition != null ) {
        if ( b.length() != 0 ) {
          b.append(" AND ");
        }
        b.append(KeyValueStoreColumns.PARTITION).append("=?");
        selArgs.add(partition);
      }
      if ( aspect != null ) {
        if ( b.length() != 0 ) {
          b.append(" AND ");
        }
        b.append(KeyValueStoreColumns.ASPECT).append("=?");
        selArgs.add(aspect);
      }
      if ( key != null ) {
        if ( b.length() != 0 ) {
          b.append(" AND ");
        }
        b.append(KeyValueStoreColumns.KEY).append("=?");
        selArgs.add(key);
      }
      
      c = db.query(DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, 
          b.toString(), selArgs.toArray(new String[selArgs.size()]), null, null, null);
      if ( c.moveToFirst() ) {
        int idxPartition = c.getColumnIndex(KeyValueStoreColumns.PARTITION);
        int idxAspect = c.getColumnIndex(KeyValueStoreColumns.ASPECT);
        int idxKey = c.getColumnIndex(KeyValueStoreColumns.KEY);
        int idxType = c.getColumnIndex(KeyValueStoreColumns.VALUE_TYPE);
        int idxValue = c.getColumnIndex(KeyValueStoreColumns.VALUE);
        
        do {
          KeyValueStoreEntry e = new KeyValueStoreEntry();
          e.partition = c.getString(idxPartition);
          e.aspect = c.getString(idxAspect);
          e.key = c.getString(idxKey);
          e.type = c.getString(idxType);
          e.value = c.getString(idxValue);
          entries.add(e);
        } while ( c.moveToNext() );
      }
    } finally {
      if ( c != null && !c.isClosed() ) {
        c.close();
      }
    }
    return entries;
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
    cvTableDef.putNull(TableDefinitionsColumns.SCHEMA_ETAG);
    cvTableDef.putNull(TableDefinitionsColumns.LAST_DATA_ETAG);
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME, -1);

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
  public static final ArrayList<ColumnDefinition> createOrOpenDBTableWithColumns(SQLiteDatabase db, String tableId,
      List<Column> columns) {
    boolean success = false;
    ArrayList<ColumnDefinition> orderedDefs = ColumnDefinition.buildColumnDefinitions(columns);
    try {
      db.beginTransaction();
      createDBTableWithColumns(db, tableId, orderedDefs);
      db.setTransactionSuccessful();
      success = true;
      return orderedDefs;
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
            Log.e(t, "createOrOpenDBTableWithColumns: Error while adding table " + tableId
                + " with columns:" + colNames.toString());
          }
        } else {
          Log.e(t, "createOrOpenDBTableWithColumns: Error while adding table " + tableId
              + " with columns: null");
        }
      }
    }
  }

  /*
   * Create a user defined database table metadata - table definiton and KVS
   * values
   */
  private static final void createDBTableWithColumns(SQLiteDatabase db, String tableId,
      List<ColumnDefinition> orderedDefs) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    String createTableCmd = getUserDefinedTableCreationStatement(tableId);

    StringBuilder createTableCmdWithCols = new StringBuilder();
    createTableCmdWithCols.append(createTableCmd);

    for (ColumnDefinition column : orderedDefs) {
      if ( !column.isUnitOfRetention() ) {
        continue;
      }
      ElementType elementType = column.getType();

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
    createDBTableMetadata(db, tableId);

    // Now need to call the function to write out all the column values
    for (ColumnDefinition column : orderedDefs) {
      createNewColumnMetadata(db, tableId, column);
    }

    // Need to address column order
    ContentValues cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
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
    
    Log.i(t, "Column order for table " + tableId + " is " + tableDefCol.toString());
    String colOrderVal = "[" + tableDefCol.toString() + "]";
    cvTableVal.put(KeyValueStoreColumns.VALUE, colOrderVal);

    // Now add Tables values into KVS
    db.replaceOrThrow(DataModelDatabaseHelper.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvTableVal);
  }

  /*
   * Create a new column metadata in the database - add column values to KVS and
   * column definitions
   */
  private static final void createNewColumnMetadata(SQLiteDatabase db, String tableId,
      ColumnDefinition column) {
    String colName = column.getElementKey();
    ArrayList<ContentValues> cvColValKVS = new ArrayList<ContentValues>();

    ContentValues cvColVal;
    
    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.array.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_FORMAT);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.string.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, "");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_NAME);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.object.name());
    String colDisplayName = "\"" + colName + "\"";
    cvColVal.put(KeyValueStoreColumns.VALUE, colDisplayName);
    cvColValKVS.add(cvColVal);

    // TODO: change bool to be integer valued in the KVS?
    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_VISIBLE);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.bool.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, column.isUnitOfRetention() ? "true" : "false");
    cvColValKVS.add(cvColVal);
    
    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
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
    cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableId);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, column.getElementName());
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, column.getElementType());
    cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, column.getListChildElementKeys());

    // Now add this data into the database
    db.replaceOrThrow(DataModelDatabaseHelper.COLUMN_DEFINITIONS_TABLE_NAME, null,
          cvColDefVal);
  }

  /**
   * Called when the schema on the server has changed w.r.t. the schema on
   * the device. In this case, we do not know whether the rows on the device
   * match those on the server.
   *
   * Reset all 'in_conflict' rows to their original local state (changed or deleted).
   * Leave all 'deleted' rows in 'deleted' state.
   * Leave all 'changed' rows in 'changed' state.
   * Reset all 'synced' rows to 'new_row' to ensure they are sync'd to the server.
   * Reset all 'synced_pending_files' rows to 'new_row' to ensure they are sync'd to the server.
   */
  public static void changeDataRowsToNewRowState(SQLiteDatabase db, String tableId) {

    StringBuilder b = new StringBuilder();

    // remove server conflicting rows
    b.setLength(0);
    b.append("DELETE FROM \"").append(tableId).append("\" WHERE ").append(DataTableColumns.SYNC_STATE)
    .append(" =? AND ").append(DataTableColumns.CONFLICT_TYPE).append(" IN (?, ?)");

    String sqlConflictingServer = b.toString();
    String argsConflictingServer[] = {
        SyncState.in_conflict.name(),
        Integer.toString(ConflictType.SERVER_DELETED_OLD_VALUES),
        Integer.toString(ConflictType.SERVER_UPDATED_UPDATED_VALUES)
    };

    // update local delete conflicts to deletes
    b.setLength(0);
    b.append("UPDATE \"").append(tableId).append("\" SET ").append(DataTableColumns.SYNC_STATE)
    .append(" =?, ").append(DataTableColumns.CONFLICT_TYPE).append(" = null WHERE ")
    .append(DataTableColumns.CONFLICT_TYPE).append(" = ?");

    String sqlConflictingLocalDeleting = b.toString();
    String argsConflictingLocalDeleting[] = {
        SyncState.deleted.name(),
        Integer.toString(ConflictType.LOCAL_DELETED_OLD_VALUES)
    };

    // update local update conflicts to updates
    String sqlConflictingLocalUpdating = sqlConflictingLocalDeleting;
    String argsConflictingLocalUpdating[] = {
        SyncState.changed.name(),
        Integer.toString(ConflictType.LOCAL_UPDATED_UPDATED_VALUES)
    };

    // reset all 'rest' rows to 'insert'
    b.setLength(0);
    b.append("UPDATE \"").append(tableId).append("\" SET ").append(DataTableColumns.SYNC_STATE)
    .append(" =? WHERE ").append(DataTableColumns.SYNC_STATE).append(" =?");

    String sqlRest = b.toString();
    String argsRest[] = {
        SyncState.new_row.name(),
        SyncState.synced.name()
    };

    String sqlRestPendingFiles = sqlRest;
    String argsRestPendingFiles[] = {
        SyncState.new_row.name(),
        SyncState.synced_pending_files.name()
    };
    
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }

      db.execSQL(sqlConflictingServer, argsConflictingServer);
      db.execSQL(sqlConflictingLocalDeleting, argsConflictingLocalDeleting);
      db.execSQL(sqlConflictingLocalUpdating, argsConflictingLocalUpdating);
      db.execSQL(sqlRest, argsRest);
      db.execSQL(sqlRestPendingFiles, argsRestPendingFiles);
      
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }
  }

  public static final void deleteServerConflictRows(SQLiteDatabase db, String tableId, String rowId) {
    // delete the old server-values in_conflict row if it exists
    String whereClause = String.format("%s = ? AND %s = ? AND %s IN " + "( ?, ? )",
        DataTableColumns.ID, DataTableColumns.SYNC_STATE, DataTableColumns.CONFLICT_TYPE);
    String[] whereArgs = { rowId, SyncState.in_conflict.name(),
        String.valueOf(ConflictType.SERVER_DELETED_OLD_VALUES),
        String.valueOf(ConflictType.SERVER_UPDATED_UPDATED_VALUES) };
        
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }

      db.delete(tableId,  whereClause,  whereArgs);
      
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }
  }

  /**
   * @param rowId
   * @return the sync state of the row (see {@link SyncState}), or null if
   *         the row does not exist.
   */
  public static SyncState getSyncState(SQLiteDatabase db, String appName, String tableId, String rowId) {
    Cursor c = null;
    try {
       c = db.query(tableId, new String[] { DataTableColumns.SYNC_STATE }, DataTableColumns.ID + " = ?",
           new String[] { rowId }, null, null, null);
       if (c.moveToFirst()) {
         int syncStateIndex = c.getColumnIndex(DataTableColumns.SYNC_STATE);
         if ( !c.isNull(syncStateIndex) ) {
           String val = ODKDatabaseUtils.getIndexAsString(c, syncStateIndex);
           return SyncState.valueOf(val);
         }
       }
       return null;
     } finally {
       if ( c != null && !c.isClosed() ) {
          c.close();
       }
     }
  }

  public static final void deleteDataInDBTableWithId(SQLiteDatabase db, String appName, String tableId, String rowId) {
    SyncState syncState = getSyncState(db, appName, tableId, rowId);
       
    boolean dbWithinTransaction = db.inTransaction();
    if (syncState == SyncState.new_row) {
      String[] whereArgs = { rowId };
      String whereClause = DataTableColumns.ID + " = ?";
          
      try {
        if ( !dbWithinTransaction ) {
          db.beginTransaction();
        }
    
        db.delete(tableId, whereClause, whereArgs);
        
        if ( !dbWithinTransaction ) {
          db.setTransactionSuccessful();
        }
      } finally {
        if ( !dbWithinTransaction ) {
          db.endTransaction();
        }
      }

      File instanceFolder = new File(ODKFileUtils.getInstanceFolder(appName, tableId, rowId));
      try {
        FileUtils.deleteDirectory(instanceFolder);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        Log.e(t, "Unable to delete this directory: " + instanceFolder.getAbsolutePath());
      }
    } else if (syncState == SyncState.synced || syncState == SyncState.changed) {
      String[] whereArgs = { rowId };
      ContentValues values = new ContentValues();
      values.put(DataTableColumns.SYNC_STATE, SyncState.deleted.name());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));
      try {
        if ( !dbWithinTransaction ) {
          db.beginTransaction();
        }
    
        db.update(tableId, values, DataTableColumns.ID + " = ?", whereArgs);
        
        if ( !dbWithinTransaction ) {
          db.setTransactionSuccessful();
        }
      } finally {
        if ( !dbWithinTransaction ) {
          db.endTransaction();
        }
      }
    }
  }
  
  public static final void rawDeleteDataInDBTable(SQLiteDatabase db, String tableId, String whereClause, String[] whereArgs) {
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }
  
      db.delete(tableId, whereClause, whereArgs);
      
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }
  }

  public static final void deleteCheckpointDataInDBTableWithId(SQLiteDatabase db, String tableId, String rowId) {
    rawDeleteDataInDBTable(db, tableId, 
        DataTableColumns.ID + "=? AND " + DataTableColumns.SAVEPOINT_TYPE + " IS NULL",
        new String[] { rowId });
  }
  
  public static final void saveAsIncompleteMostRecentCheckpointDataInDBTableWithId(SQLiteDatabase db, String tableId, String rowId) {
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }
  
      db.execSQL("UPDATE \"" + tableId + "\" SET " +
          DataTableColumns.SAVEPOINT_TYPE + "= ? WHERE " +
          DataTableColumns.ID + "=?",
          new String[] { SavepointTypeManipulator.incomplete(), rowId });
      db.delete(tableId, 
          DataTableColumns.ID + "=? AND " + DataTableColumns.SAVEPOINT_TIMESTAMP +
          " NOT IN (SELECT MAX(" + DataTableColumns.SAVEPOINT_TIMESTAMP + ") FROM \"" +
          tableId + "\" WHERE " + DataTableColumns.ID + "=?)",
          new String[] { rowId, rowId });
      
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }
  }
  
  /*
   * Write data into a user defined database table
   */
  public static final void updateDataInExistingDBTableWithId(SQLiteDatabase db, String tableId,
      ArrayList<ColumnDefinition> orderedColumns, ContentValues cvValues, String uuid) {

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.put(DataTableColumns.ID, uuid);
    cvDataTableVal.putAll(cvValues);

    upsertDataAndMetadataIntoExistingDBTable(db, tableId, orderedColumns, cvDataTableVal, true);
  }
  
  /*
   * Write data into a user defined database table
   */
  public static final void insertDataIntoExistingDBTableWithId(SQLiteDatabase db, String tableId,
      ArrayList<ColumnDefinition> orderedColumns, ContentValues cvValues, String uuid) {

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.put(DataTableColumns.ID, uuid);
    cvDataTableVal.putAll(cvValues);

    upsertDataAndMetadataIntoExistingDBTable(db, tableId, orderedColumns, cvDataTableVal, false);
  }

  /*
   * Write data into a user defined database table
   * 
   * TODO: This is broken w.r.t. updates of partial fields
   */
  private static final void upsertDataAndMetadataIntoExistingDBTable(SQLiteDatabase db,
      String tableId, ArrayList<ColumnDefinition> orderedColumns, ContentValues cvValues, boolean shouldUpdate) {
    String rowId = null;
    String whereClause = null;
    String [] whereArgs = new String[1];
    boolean update = false;

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.putAll(cvValues);
    
    // Bug fix for not updating a db row if an existing row id is used
    if (cvDataTableVal.containsKey(DataTableColumns.ID)) {
      // Select everything out of the table with given id
      rowId = cvDataTableVal.getAsString(DataTableColumns.ID);
      if ( rowId == null ) {
        throw new IllegalArgumentException(DataTableColumns.ID + ", if specified, cannot be null");
      }
      whereClause = DataTableColumns.ID + " = ?"; 
      whereArgs[0] = rowId;
      String sel = "SELECT * FROM " + tableId + " WHERE "+ whereClause;
      String[] selArgs = whereArgs;
      Cursor cursor = rawQuery(db, sel, selArgs);
      
      // There must be only one row in the db for the update to work
      if (shouldUpdate) {
        if (cursor.getCount() == 1) {
          update = true;
        } else if (cursor.getCount() > 1) {
          throw new IllegalArgumentException(t + ": row id " + rowId + " has more than 1 row in table " + tableId);
        }
      } else {
        if (cursor.getCount() > 0) {
          throw new IllegalArgumentException(t + ": id " + rowId + " is already present in table " + tableId);
        }
      }

    } else {
      rowId = "uuid:" + UUID.randomUUID().toString();
    }

    // TODO: This is broken w.r.t. updates of partial fields
    // TODO: This is broken w.r.t. updates of partial fields
    // TODO: This is broken w.r.t. updates of partial fields
    // TODO: This is broken w.r.t. updates of partial fields

    if (!cvDataTableVal.containsKey(DataTableColumns.ID)) {
      cvDataTableVal.put(DataTableColumns.ID, rowId);
    }

    if ( update ) {
  
      if (!cvDataTableVal.containsKey(DataTableColumns.SYNC_STATE) ||
          (cvDataTableVal.get(DataTableColumns.SYNC_STATE) == null)) {
        cvDataTableVal.put(DataTableColumns.SYNC_STATE, SyncState.changed.name());
      }
  
      if (cvDataTableVal.containsKey(DataTableColumns.LOCALE) &&
          (cvDataTableVal.get(DataTableColumns.LOCALE) == null)) {
        cvDataTableVal.put(DataTableColumns.LOCALE, DataTableColumns.DEFAULT_LOCALE);
      }
  
      if (cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TYPE) &&
          (cvDataTableVal.get(DataTableColumns.SAVEPOINT_TYPE) == null)) {
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
      }
  
      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP) ||
          cvDataTableVal.get(DataTableColumns.SAVEPOINT_TIMESTAMP) == null) {
        String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
      }
  
      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_CREATOR) ||
          (cvDataTableVal.get(DataTableColumns.SAVEPOINT_CREATOR) == null)) {
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR, DataTableColumns.DEFAULT_SAVEPOINT_CREATOR );
      }
      
    } else {
      if (!cvDataTableVal.containsKey(DataTableColumns.ID)) {
        cvDataTableVal.put(DataTableColumns.ID, rowId);
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.ROW_ETAG) ||
          cvDataTableVal.get(DataTableColumns.ROW_ETAG) == null) {
        cvDataTableVal.put(DataTableColumns.ROW_ETAG, DataTableColumns.DEFAULT_ROW_ETAG);
      }
  
      if (!cvDataTableVal.containsKey(DataTableColumns.SYNC_STATE) ||
          (cvDataTableVal.get(DataTableColumns.SYNC_STATE) == null)) {
        cvDataTableVal.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());
      }
  
      if (!cvDataTableVal.containsKey(DataTableColumns.CONFLICT_TYPE)) {
        cvDataTableVal.putNull(DataTableColumns.CONFLICT_TYPE);
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
        cvDataTableVal.putNull(DataTableColumns.FORM_ID);
      }
  
      if (!cvDataTableVal.containsKey(DataTableColumns.LOCALE) ||
          (cvDataTableVal.get(DataTableColumns.LOCALE) == null)) {
        cvDataTableVal.put(DataTableColumns.LOCALE, DataTableColumns.DEFAULT_LOCALE);
      }
  
      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TYPE) ||
          (cvDataTableVal.get(DataTableColumns.SAVEPOINT_TYPE) == null)) {
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
      }
  
      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP) ||
          cvDataTableVal.get(DataTableColumns.SAVEPOINT_TIMESTAMP) == null) {
        String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
      }
  
      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_CREATOR) ||
          (cvDataTableVal.get(DataTableColumns.SAVEPOINT_CREATOR) == null)) {
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR, DataTableColumns.DEFAULT_SAVEPOINT_CREATOR );
      }
    }
    
    cleanUpValuesMap(orderedColumns, cvDataTableVal);
    
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if ( !dbWithinTransaction ) {
        db.beginTransaction();
      }
  
      if (update) {
        db.update(tableId, cvDataTableVal, whereClause, whereArgs);
      } else {
        db.insertOrThrow(tableId, null, cvDataTableVal);
      }
      
      if ( !dbWithinTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !dbWithinTransaction ) {
        db.endTransaction();
      }
    }

  }

  /**
   * If the caller specified a complex json value for a structured type, flush the value through
   * to the individual columns.
   * 
   * @param orderedColumns
   * @param values
   */
  private static void cleanUpValuesMap(ArrayList<ColumnDefinition> orderedColumns, ContentValues values) {

    Map<String, String> toBeResolved = new HashMap<String,String>();

    for ( String key : values.keySet() ) {
      if ( DataTableColumns.CONFLICT_TYPE.equals(key) ) {
        continue;
      } else if ( DataTableColumns.FILTER_TYPE.equals(key) ) {
        continue;
      } else if ( DataTableColumns.FILTER_TYPE.equals(key) ) {
        continue;
      } else if ( DataTableColumns.FILTER_VALUE.equals(key) ) {
        continue;
      } else if ( DataTableColumns.FORM_ID.equals(key) ) {
        continue;
      } else if ( DataTableColumns.ID.equals(key) ) {
        continue;
      } else if ( DataTableColumns.LOCALE.equals(key) ) {
        continue;
      } else if ( DataTableColumns.ROW_ETAG.equals(key) ) {
        continue;
      } else if ( DataTableColumns.SAVEPOINT_CREATOR.equals(key) ) {
        continue;
      } else if ( DataTableColumns.SAVEPOINT_TIMESTAMP.equals(key) ) {
        continue;
      } else if ( DataTableColumns.SAVEPOINT_TYPE.equals(key) ) {
        continue;
      } else if ( DataTableColumns.SYNC_STATE.equals(key) ) {
        continue;
      } else if ( DataTableColumns._ID.equals(key) ) {
        continue;
      }
      // OK it is one of the data columns
      ColumnDefinition cp = ColumnDefinition.find(orderedColumns, key);
      if ( !cp.isUnitOfRetention() ) {
        toBeResolved.put(key, values.getAsString(key));
      }
    }

    // remove these non-retained values from the values set...
    for ( String key : toBeResolved.keySet() ) {
      values.remove(key);
    }

    while (!toBeResolved.isEmpty() ) {

      Map<String, String> moreToResolve = new HashMap<String, String>();

      for ( Map.Entry<String,String> entry : toBeResolved.entrySet() ) {
        String key = entry.getKey();
        String json = entry.getValue();
        if ( json == null ) {
          // don't need to do anything
          // since the value is null
          continue;
        }
        ColumnDefinition cp = ColumnDefinition.find(orderedColumns, key);
        try {
          Map<String,Object> struct = ODKFileUtils.mapper.readValue(json, Map.class);
          for ( ColumnDefinition child : cp.getChildren() ) {
            String subkey = child.getElementKey();
            ColumnDefinition subcp = ColumnDefinition.find(orderedColumns, subkey);
            if ( subcp.isUnitOfRetention() ) {
              ElementType subtype = subcp.getType();
              ElementDataType type = subtype.getDataType();
              if ( type == ElementDataType.integer ) {
                values.put(subkey, (Integer) struct.get(subcp.getElementName()));
              } else if ( type == ElementDataType.number ) {
                values.put(subkey, (Double) struct.get(subcp.getElementName()));
              } else if ( type == ElementDataType.bool ) {
                values.put(subkey, ((Boolean) struct.get(subcp.getElementName())) ? 1 : 0);
              } else {
                values.put(subkey, (String) struct.get(subcp.getElementName()));
              }
            } else {
              // this must be a javascript structure... re-JSON it and save (for next round).
              moreToResolve.put(subkey, ODKFileUtils.mapper.writeValueAsString(struct.get(subcp.getElementName())));
            }
          }
        } catch (JsonParseException e) {
          e.printStackTrace();
          throw new IllegalStateException("should not be happening");
        } catch (JsonMappingException e) {
          e.printStackTrace();
          throw new IllegalStateException("should not be happening");
        } catch (IOException e) {
          e.printStackTrace();
          throw new IllegalStateException("should not be happening");
        }
      }

      toBeResolved = moreToResolve;
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
