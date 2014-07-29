package org.opendatakit.common.android.utilities.test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.utilities.ODKDatabaseUserDefinedTypes;
import org.opendatakit.common.android.utilities.ODKDatabaseUtils;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.test.AndroidTestCase;
import android.test.RenamingDelegatingContext;
import android.util.Log;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ODKDatabaseUtilsTest extends AndroidTestCase{

  private static final String TAG = "ODKDatabaseUtilsTest";

  private static final String TEST_FILE_PREFIX = "test_";

  private static final String DATABASE_NAME = "test.db";

  private static final int DATABASE_VERSION = 1;

  private static final String testTable = "testTable";

  private static SQLiteDatabase db;

  private static final String colDefTable = "_column_definitions";

  private static final String tableDefTable = "_table_definitions";

  private static final String activeKVSTable = "_key_value_store_active";

  private static final String elemKey = "_element_key";
  private static final String elemName = "_element_name";
  private static final String listChildElemKeys = "_list_child_element_keys";


  private static class DatabaseHelper extends SQLiteOpenHelper {

    public DatabaseHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
      String createColCmd = ColumnDefinitionsColumns.getTableCreateSql(colDefTable);

      try {
        db.execSQL(createColCmd);
      } catch (Exception e) {
        Log.e("test", "Error while creating table " + colDefTable);
        e.printStackTrace();
      }

      String createTableDefCmd = "CREATE TABLE " + tableDefTable + " (" +
        "_table_id TEXT NOT NULL PRIMARY KEY, _db_table_name TEXT NOT NULL UNIQUE," +
        "_sync_tag TEXT NULL,_last_sync_time TEXT NOT NULL, _sync_state TEXT NOT NULL, " +
        "_transactioning INTEGER NOT NULL);";

      try {
        db.execSQL(createTableDefCmd);
      } catch (Exception e) {
        Log.e("test", "Error while creating table " + tableDefTable);
        e.printStackTrace();
      }

      String createKVSCmd = "CREATE TABLE " + activeKVSTable + " ("+
        "_table_id TEXT NOT NULL, _partition TEXT NOT NULL, _aspect TEXT NOT NULL, "+
        "_key TEXT NOT NULL, _type TEXT NOT NULL, _value TEXT NOT NULL, PRIMARY KEY "+
        "( _table_id, _partition, _aspect, _key) );";

      try {
        db.execSQL(createKVSCmd);
      } catch (Exception e) {
        Log.e("test", "Error while creating table " + activeKVSTable);
        e.printStackTrace();
      }
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
      // TODO Auto-generated method stub

    }
  }

  /*
   *  Set up the database for the tests(non-Javadoc)
   * @see android.test.AndroidTestCase#setUp()
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();

    RenamingDelegatingContext context
        = new RenamingDelegatingContext(getContext(), TEST_FILE_PREFIX);

    DatabaseHelper mDbHelper = new DatabaseHelper(context);
    db = mDbHelper.getWritableDatabase();

    File file = context.getDatabasePath(DATABASE_NAME);
    String path = file.getAbsolutePath();

    Log.i("test", "The absolute path of the database is" + path);
  }

  /*
   * Destroy all test data once tests are done(non-Javadoc)
   * @see android.test.AndroidTestCase#tearDown()
   */
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();

    if (db != null) {
      db.close();
    }
  }

  /*
   *  Check that the database is setup
   */
  public void testPreConditions() {
    assertNotNull(db);
  }

  /*
   * Test creation of user defined database table when table doesn't exist
   */
  public void testCreateOrOpenDbTableWhenTableDoesNotExist_ExpectPass() {
    // Create the database table
    String tableName = testTable;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    // Check that the table exists
    Cursor cursor = db.rawQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='" + tableName + "'", null);
    assertNotNull("Cursor is null", cursor);
    assertEquals("Cursor should only have 1 row", cursor.getCount(), 1);
    cursor.moveToFirst();
    assertEquals("Name of user defined table does not match", cursor.getString(0), tableName);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table when table already exists
   */
  public void testCreateOrOpenDbTableWhenTableDoesExist_ExpectPass() {
    // Create the database table
    String tableName = testTable;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    // Check that the table exists
    Cursor cursor = db.rawQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='" + tableName + "'", null);
    assertNotNull("Cursor is null", cursor);
    assertEquals("Cursor should only have 1 row", cursor.getCount(), 1);
    cursor.moveToFirst();
    assertEquals("Name of user defined table does not match", cursor.getString(0), tableName);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database tables when table is null
   */
  public void testCreateOrOpenDbTableWhenTableIsNull_ExpectFail() {
	// Create the database table
    boolean thrown = false;
    String tableName = null;

    try {
      ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);
  }

  /*
   * Test query when there is no data
   */
  public void testQueryWithNoData_ExpectFail() {
    String tableName = testTable;
    boolean thrown = false;

    try {
      ODKDatabaseUtils.query(db, false, tableName, null, null, null, null, null, null, null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);
  }

  /*
   * Test query when there is data
   */
  public void testQueryWithData_ExpectPass() {
    String tableName = testTable;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    // Check that the user defined rows are in the table
    Cursor cursor = ODKDatabaseUtils.query(db, false, tableName, null, null, null, null, null, null, null);
    Cursor refCursor = db.query(false, tableName, null, null, null, null, null, null, null);

    if (cursor != null && refCursor != null) {
      int index = 0;
      while (cursor.moveToNext() && refCursor.moveToNext()) {
        int testType = cursor.getType(index);
        int refType = refCursor.getType(index);
        assertEquals(testType, refType);

        switch (refType) {
          case Cursor.FIELD_TYPE_BLOB:
            byte [] byteArray = cursor.getBlob(index);
            byte [] refByteArray = refCursor.getBlob(index);
            assertEquals(byteArray, refByteArray);
            break;
          case Cursor.FIELD_TYPE_FLOAT:
            float valueFloat = cursor.getFloat(index);
            float refValueFloat = refCursor.getFloat(index);
            assertEquals(valueFloat, refValueFloat);
            break;
          case Cursor.FIELD_TYPE_INTEGER:
            int valueInt = cursor.getInt(index);
            int refValueInt = refCursor.getInt(index);
            assertEquals(valueInt, refValueInt);
            break;
          case Cursor.FIELD_TYPE_STRING:
            String valueStr = cursor.getString(index);
            String refValueStr = refCursor.getString(index);
            assertEquals(valueStr, refValueStr);
            break;
          case Cursor.FIELD_TYPE_NULL:
          default:
            break;
        }
      }
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test raw query when there is data
   */
  public void testRawQueryWithNoData_ExpectFail() {
    String tableName = testTable;
    String query = "SELECT * FROM " + tableName;
    boolean thrown = false;

    try {
      ODKDatabaseUtils.rawQuery(db, query, null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);
  }

  /*
   * Test raw query when there is no data
   */
  public void testRawQueryWithData_ExpectPass() {
    String tableName = testTable;
    String query = "SELECT * FROM " + tableName;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    // Check that the user defined rows are in the table
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, query, null);
    Cursor refCursor = db.rawQuery(query, null);

    if (cursor != null && refCursor != null) {
      int index = 0;
      while (cursor.moveToNext() && refCursor.moveToNext()) {
        int testType = cursor.getType(index);
        int refType = refCursor.getType(index);
        assertEquals(testType, refType);

        switch (refType) {
          case Cursor.FIELD_TYPE_BLOB:
            byte [] byteArray = cursor.getBlob(index);
            byte [] refByteArray = refCursor.getBlob(index);
            assertEquals(byteArray, refByteArray);
            break;
          case Cursor.FIELD_TYPE_FLOAT:
            float valueFloat = cursor.getFloat(index);
            float refValueFloat = refCursor.getFloat(index);
            assertEquals(valueFloat, refValueFloat);
            break;
          case Cursor.FIELD_TYPE_INTEGER:
            int valueInt = cursor.getInt(index);
            int refValueInt = refCursor.getInt(index);
            assertEquals(valueInt, refValueInt);
            break;
          case Cursor.FIELD_TYPE_STRING:
            String valueStr = cursor.getString(index);
            String refValueStr = refCursor.getString(index);
            assertEquals(valueStr, refValueStr);
            break;
          case Cursor.FIELD_TYPE_NULL:
          default:
            break;
        }
      }
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when table does not exist
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnDoesNotExist_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when table does exist
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnDoesExist_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is null
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsNull_ExpectPass(){
    String tableName = testTable;
    boolean thrown = false;

    try {
      ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE IF EXISTS " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is int
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsInt_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE IF EXISTS " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is array
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsArray_ExpectPass(){
        String tableName = testTable;
    String testCol = "testColumn";
    String itemsStr = "items";
    String testColItems = testCol + "_" + itemsStr;
    String testColType = ODKDatabaseUserDefinedTypes.ARRAY;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(1, map.size());
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Select everything out of the table
    String sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String[] selArgs = {"" + testCol};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(listChildElemKeys);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      String testVal = "[\"" + testColItems + "\"]";
      assertEquals(valStr, testVal);
    }

    // Select everything out of the table
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs2 = {testColItems};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs2);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, itemsStr);
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is array
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsBoolean_ExpectPass(){
        String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.BOOLEAN;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is string
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsString_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.STRING;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is date
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsDate_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.DATE;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is datetime
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsDateTime_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.DATETIME;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is time
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsTime_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.TIME;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is geopoint
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsGeopoint_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String lat = "latitude";
    String lng = "longitude";
    String alt = "altitude";
    String acc = "accuracy";
    String testColLat = testCol + "_" + lat;
    String testColLng = testCol + "_" + lng;
    String testColAlt = testCol + "_" + alt;
    String testColAcc = testCol + "_" + acc;
    String testColType = ODKDatabaseUserDefinedTypes.GEOPOINT;
    String testColResType = ODKDatabaseUserDefinedTypes.NUMBER;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 4);
    assertTrue(map.containsKey(testColLat));
    assertTrue(map.containsKey(testColLng));
    assertTrue(map.containsKey(testColAlt));
    assertTrue(map.containsKey(testColAcc));

    for (Entry<String, String> entry : map.entrySet()) {
      String value = entry.getValue();
      assertTrue(value.equals(testColResType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of user defined database table with column when column is mimeUri
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsMimeUri_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String uriFrag = "uriFragment";
    String conType = "contentType";
    String testColUriFrag = testCol + "_" + uriFrag;
    String testColContType = testCol + "_" + conType;
    String testColType = ODKDatabaseUserDefinedTypes.MIMEURI;
    LinkedHashMap <String, String> col = new LinkedHashMap <String, String>();
    col.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, col);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(2, map.size());
    assertTrue(map.containsKey(testColUriFrag));
    assertTrue(map.containsKey(testColContType));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(value.equals("string"));
    }

    // Select everything out of the table for element key
    String sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String[] selArgs = {"" + testCol};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(listChildElemKeys);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      String testVal = "[\"" + testColUriFrag + "\",\"" + testColContType + "\"]";
      assertEquals(valStr, testVal);
    }

    // Select everything out of the table for uriFragment
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs2 = {testColUriFrag};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs2);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, uriFrag);
    }

    // Select everything out of the table for contentType
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs3 = {testColContType};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs3);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, conType);
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test getting all column names when columns exist
   */
  public void testGetAllColumnNamesWhenColumnsExist_ExpectPass() {
    String tableName = testTable;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    String[] colNames = ODKDatabaseUtils.getAllColumnNames(db, tableName);
    boolean colLength = (colNames.length > 0);
    assertTrue(colLength);

    String[] defCols = ODKDatabaseUtils.getDefaultUserDefinedTableColumns();
    assertEquals(colNames.length, defCols.length);

    Arrays.sort(colNames);
    Arrays.sort(defCols);

    for (int i = 0; i < colNames.length; i++) {
      assertEquals(colNames[i], defCols[i]);
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test getting all column names when table does not exist
   */
  public void testGetAllColumnNamesWhenTableDoesNotExist_ExpectFail() {
    String tableName = testTable;
    boolean thrown = false;

    try {
      ODKDatabaseUtils.getAllColumnNames(db, tableName);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);
  }

  /*
   * Test getting user defined column names when columns exist
   */
  public void testGetUserDefinedColumnNamesWhenColumnsExist_ExpectPass() {
    String tableName = testTable;
    LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
    columns.put("testCol", ODKDatabaseUserDefinedTypes.STRING);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);
    LinkedHashMap<String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);

    assertEquals(map, columns);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test getting user defined column names when column does not exist
   */
  public void testGetUserDefinedColumnNamesWhenColumnDoesNotExist_ExpectPass() {
    String tableName = testTable;
    LinkedHashMap<String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertTrue(map.isEmpty());
  }

  /*
   * Test getting user defined column names when table does not exist
   */
  public void testGetUserDefinedColumnNamesWhenTableDoesNotExist_ExpectPass() {
    String tableName = testTable;
    LinkedHashMap<String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertTrue(map.isEmpty());
  }

  /*
   * Test creation of new column when column does not already exist
   */
  public void testCreateNewColumnWhenColumnDoesNotExist_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column does exist
   */
  public void testCreateNewColumnWhenColumnDoesExist_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is null
   */
  public void testCreateNewColumnWhenColumnIsNull_ExpectFail() {
    boolean thrown = false;
    String tableName = testTable;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    try {
      ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, null, null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is int
   */
  public void testCreateNewColumnWhenColumnIsInt_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is array
   */
  public void testCreateNewColumnWhenColumnIsArray_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String itemsStr = "items";
    String testColItems = testCol + "_" + itemsStr;
    String testColType = ODKDatabaseUserDefinedTypes.ARRAY;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(1, map.size());
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Select everything out of the table
    String sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String[] selArgs = {"" + testCol};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(listChildElemKeys);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      String testVal = "[\"" + testColItems + "\"]";
      assertEquals(valStr, testVal);
    }

    // Select everything out of the table
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs2 = {testColItems};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs2);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, itemsStr);
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is boolean
   */
  public void testCreateNewColumnWhenColumnIsBoolean_ExpectPass(){
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.BOOLEAN;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is string
   */
  public void testCreateNewColumnWhenColumnIsString_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.STRING;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is date
   */
  public void testCreateNewColumnWhenColumnIsDate_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.DATE;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is date time
   */
  public void testCreateNewColumnWhenColumnIsDateTime_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.DATETIME;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is time
   */
  public void testCreateNewColumnWhenColumnIsTime_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.TIME;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 1);
    assertTrue(map.containsKey(testCol));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(key.equals(testCol));
      assertTrue(value.equals(testColType));
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is geopoint
   */
  public void testCreateNewColumnWhenColumnIsGeopoint_ExpectPass() {
    String tableName = testTable + "_Geopoint";
    String testCol = "testColumn";
    String lat = "latitude";
    String lng = "longitude";
    String alt = "altitude";
    String acc = "accuracy";
    String testColLat = testCol + "_" + lat;
    String testColLng = testCol + "_" + lng;
    String testColAlt = testCol + "_" + alt;
    String testColAcc = testCol + "_" + acc;
    String testColType = ODKDatabaseUserDefinedTypes.GEOPOINT;
    String testColResType = ODKDatabaseUserDefinedTypes.NUMBER;
    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(map.size(), 4);
    assertTrue(map.containsKey(testColLat));
    assertTrue(map.containsKey(testColLng));
    assertTrue(map.containsKey(testColAlt));
    assertTrue(map.containsKey(testColAcc));

    for (Entry<String, String> entry : map.entrySet()) {
      String value = entry.getValue();
      assertTrue(value.equals(testColResType));
    }

    // Select everything out of the table for element key
    String sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String[] selArgs = {"" + testCol};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(listChildElemKeys);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      String testVal = "[\"" + testColLat + "\",\"" + testColLng + "\",\"" + testColAlt + "\",\"" + testColAcc + "\"]";
      assertEquals(valStr, testVal);
    }

    // Select everything out of the table for lat
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs2 = {testColLat};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs2);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, lat);
    }

    // Select everything out of the table for long
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs3 = {testColLng};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs3);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, lng);
    }

    // Select everything out of the table for alt
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs4 = {testColAlt};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs4);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, alt);
    }

    // Select everything out of the table for acc
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs5 = {testColAcc};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs5);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, acc);
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test creation of new column when column is mimeUri
   */
  public void testCreateNewColumnWhenColumnIsMimeUri_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String uriFrag = "uriFragment";
    String conType = "contentType";
    String testColUriFrag = testCol + "_" + uriFrag;
    String testColContType = testCol + "_" + conType;
    String testColType = ODKDatabaseUserDefinedTypes.MIMEURI;

    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);
    ODKDatabaseUtils.createNewColumnIntoExistingDBTable(db, tableName, testCol, testColType);

    LinkedHashMap <String, String> map = ODKDatabaseUtils.getUserDefinedColumnsAndTypes(db, tableName);
    assertEquals(2, map.size());
    assertTrue(map.containsKey(testColUriFrag));
    assertTrue(map.containsKey(testColContType));

    for (Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      assertTrue(value.equals("string"));
    }

    // Select everything out of the table for element key
    String sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String[] selArgs = {"" + testCol};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(listChildElemKeys);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      String testVal = "[\"" + testColUriFrag + "\",\"" + testColContType + "\"]";
      assertEquals(valStr, testVal);
    }

    // Select everything out of the table for uriFragment
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs2 = {testColUriFrag};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs2);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, uriFrag);
    }

    // Select everything out of the table for contentType
    sel = "SELECT * FROM " + colDefTable + " WHERE " + elemKey + " = ?";
    String [] selArgs3 = {testColContType};
    cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs3);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, conType);
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with all null values
   */
  public void testWriteDataIntoExisitingDbTableWithAllNullValues_ExpectFail() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    boolean thrown = false;
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();

    columns.put(testCol, testColType);
    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    try {
      ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with valid values
   */
  public void testWriteDataIntoExisitingDbTableWithValidValue_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    int testVal = 5;
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with valid values and a certain id
   */
  public void testWriteDataIntoExisitingDbTableWithId_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    int testVal = 5;
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, 5);

    String uuid = UUID.randomUUID().toString();
    ODKDatabaseUtils.writeDataIntoExistingDBTableWithId(db, tableName, cvValues, uuid);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data and metadata into the existing db table with valid values
   */
  public void testWriteDataAndMetadataIntoExistingDBTableWithValidValue_ExpectPass() {
    String tableName = testTable;
    String nullString = null;

    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    String uuid = UUID.randomUUID().toString();
    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ContentValues cvValues = new ContentValues();
    cvValues.put(DataTableColumns.ID, uuid);
    cvValues.put(DataTableColumns.ROW_ETAG, nullString);
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.inserting.name());
    cvValues.put(DataTableColumns.CONFLICT_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_VALUE, nullString);
    cvValues.put(DataTableColumns.FORM_ID, nullString);
    cvValues.put(DataTableColumns.LOCALE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);


    ODKDatabaseUtils.writeDataAndMetadataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ DataTableColumns.ID + " = ?";
    String[] selArgs = {uuid};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TIMESTAMP);
      String ts = cursor.getString(ind);
      assertEquals(ts, timeStamp);

      ind = cursor.getColumnIndex(DataTableColumns.SYNC_STATE);
      String ss = cursor.getString(ind);
      assertEquals(ss, SyncState.inserting.name());
    }

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing writing metadata into an existing table when the rowID is null
   */
  public void testWriteDataAndMetadataIntoExistingDBTableWhenIDIsNull_ExpectFail() {
    String tableName = testTable;
    String nullString = null;
    boolean thrown = false;

    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ContentValues cvValues = new ContentValues();
    cvValues.put(DataTableColumns.ID, nullString);
    cvValues.put(DataTableColumns.ROW_ETAG, nullString);
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.inserting.name());
    cvValues.put(DataTableColumns.CONFLICT_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_VALUE, nullString);
    cvValues.put(DataTableColumns.FORM_ID, nullString);
    cvValues.put(DataTableColumns.LOCALE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);

    try {
      ODKDatabaseUtils.writeDataAndMetadataIntoExistingDBTable(db, tableName, cvValues);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing metadata into the existing db table when sync state is null
   */
  public void testWriteDataAndMetadataIntoExistingDBTableWhenSyncStateIsNull_ExpectFail() {
    String tableName = testTable;
    String nullString = null;
    boolean thrown = false;

    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    String uuid = UUID.randomUUID().toString();
    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ContentValues cvValues = new ContentValues();
    cvValues.put(DataTableColumns.ID, uuid);
    cvValues.put(DataTableColumns.ROW_ETAG, nullString);
    cvValues.put(DataTableColumns.SYNC_STATE, nullString);
    cvValues.put(DataTableColumns.CONFLICT_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_VALUE, nullString);
    cvValues.put(DataTableColumns.FORM_ID, nullString);
    cvValues.put(DataTableColumns.LOCALE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);

    try {
      ODKDatabaseUtils.writeDataAndMetadataIntoExistingDBTable(db, tableName, cvValues);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing metadata into the existing db table when sync state is null
   */
  public void testWriteDataAndMetadataIntoExistingDBTableWhenTimeStampIsNull_ExpectFail() {
    String tableName = testTable;
    String nullString = null;
    boolean thrown = false;

    ODKDatabaseUtils.createOrOpenDBTable(db, tableName);

    String uuid = UUID.randomUUID().toString();

    ContentValues cvValues = new ContentValues();
    cvValues.put(DataTableColumns.ID, uuid);
    cvValues.put(DataTableColumns.ROW_ETAG, nullString);
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.inserting.name());
    cvValues.put(DataTableColumns.CONFLICT_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_VALUE, nullString);
    cvValues.put(DataTableColumns.FORM_ID, nullString);
    cvValues.put(DataTableColumns.LOCALE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);

    try {
      ODKDatabaseUtils.writeDataAndMetadataIntoExistingDBTable(db, tableName, cvValues);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with array value
   */
  public void testWriteDataIntoExisitingDbTableWithArray_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.ARRAY;
    String testVal = "item";
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }


  /*
   * Test writing the data into the existing db table with boolean value
   */
  public void testWriteDataIntoExisitingDbTableWithBoolean_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.BOOLEAN;
    int testVal = 1;
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with valid values
   */
  public void testWriteDataIntoExisitingDbTableWithDate_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.DATE;
    String testVal = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with datetime
   */
  public void testWriteDataIntoExisitingDbTableWithDatetime_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.DATETIME;
    String testVal = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with geopoint
   */
  public void testWriteDataIntoExisitingDbTableWithGeopoint_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColLat = "testColumn_latitude";
    String testColLong = "testColumn_longitude";
    String testColAlt = "testColumn_altitude";
    String testColAcc = "testColumn_accuracy";
    double pos_lat = 5.55;
    double pos_long = 6.6;
    double pos_alt = 7.77;
    double pos_acc = 8.88;
    String testColType = ODKDatabaseUserDefinedTypes.GEOPOINT;
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testColLat, pos_lat);
    cvValues.put(testColLong, pos_long);
    cvValues.put(testColAlt, pos_alt);
    cvValues.put(testColAcc, pos_acc);

    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testColLat + " = ?";
    String[] selArgs = {"" + pos_lat};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    double valLat = 0;
    double valLong = 0;
    double valAlt = 0;
    double valAcc = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testColLat);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      valLat = cursor.getDouble(ind);

      ind = cursor.getColumnIndex(testColLong);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      valLong = cursor.getDouble(ind);

      ind = cursor.getColumnIndex(testColAlt);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      valAlt = cursor.getDouble(ind);

      ind = cursor.getColumnIndex(testColAcc);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      valAcc = cursor.getDouble(ind);
    }

    assertEquals(valLat, pos_lat);
    assertEquals(valLong, pos_long);
    assertEquals(valAlt, pos_alt);
    assertEquals(valAcc, pos_acc);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with integer
   */
  public void testWriteDataIntoExisitingDbTableWithInteger_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.INTEGER;
    int testVal = 5;
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with mimeUri
   */
  public void testWriteDataIntoExisitingDbTableWithMimeUri_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.MIMEURI;
    String uuid = UUID.randomUUID().toString();
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, String> mapObject = new HashMap<String, String>();
    mapObject.put("uriFragment", "tables/example/instances/" + uuid + "/" + testCol + "-" + uuid + ".jpg");
    mapObject.put("contentType", "image/jpg");
    String picJsonStr = null;
    try {
      picJsonStr = objectMapper.writeValueAsString(mapObject);
    } catch (Exception e) {
      fail("testWriteDataIntoExisitingDbTableWithMimeUri_ExpectPass: exception while trying to convert mimeUri to string");
      e.printStackTrace();
    }

    String testVal = null;
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, picJsonStr);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with number
   */
  public void testWriteDataIntoExisitingDbTableWithNumber_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.NUMBER;
    double testVal = 5.5;
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    double val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      val = cursor.getDouble(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with string
   */
  public void testWriteDataIntoExisitingDbTableWithString_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.STRING;
    String testVal = "test";
    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  /*
   * Test writing the data into the existing db table with time
   */
  public void testWriteDataIntoExisitingDbTableWithTime_ExpectPass() {
    String tableName = testTable;
    String testCol = "testColumn";
    String testColType = ODKDatabaseUserDefinedTypes.TIME;
    String interMed = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
    int pos = interMed.indexOf('T');
    String testVal = null;

    if (pos > -1) {
      testVal = interMed.substring(pos+1);
    } else {
      fail("The conversion of the date time string to time is incorrect");
      Log.i(TAG, "Time string is " + interMed);
    }

    Log.i(TAG, "Time string is " + testVal);

    LinkedHashMap<String, String> columns = new LinkedHashMap<String,String>();
    columns.put(testCol, testColType);

    ODKDatabaseUtils.createOrOpenDBTableWithColumns(db, tableName, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseUtils.writeDataIntoExistingDBTable(db, tableName, cvValues);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableName + " WHERE "+ testCol + " = ?";
    String[] selArgs = {"" + testVal};
    Cursor cursor = ODKDatabaseUtils.rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableName);
  }

  public void testFailureForCheckins() {
    assertTrue(true);
  }

}
