package org.opendatakit.common.android.provider;

import android.provider.BaseColumns;

public class KeyValueStoreColumns implements BaseColumns {

	// Names of the columns in the key value store.
	public static final String TABLE_ID = "table_id";
	public static final String PARTITION = "partition";
	public static final String ASPECT = "aspect";
	public static final String KEY = "key";
	public static final String VALUE_TYPE = "type";
	public static final String VALUE = "value";


	  /**
	   * The table creation SQL statement for a KeyValueStore table.
	   *
	   * @param tableName -- the name of the KVS table to be created
	   * @return well-formed SQL create-table statement.
	   */
	  public static String getTableCreateSql(String tableName) {
	    return "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
	        TABLE_ID + " TEXT NOT NULL, " +
	        PARTITION + " TEXT NOT NULL, " +
	        ASPECT + " TEXT NOT NULL, " +
	        KEY + " TEXT NOT NULL, " +
	        VALUE_TYPE + " TEXT NOT NULL, " +
	        VALUE + " TEXT NOT NULL, " +
	        "PRIMARY KEY ( " + TABLE_ID + ", " + PARTITION + ", " + ASPECT + ", " + KEY + ") )";
	  }
  }