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

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.provider.InstanceColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;
import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

/**
 * This class helps open, create, and upgrade the database file.
 */
public class DataModelDatabaseHelper extends WebKitDatabaseInfoHelper {

	static final String APP_KEY = "org.opendatakit.common";
	static final int APP_VERSION = 1;

	/**
	 * key-value store table
	 */

	// tablenames for the various key value stores
	public static final String KEY_VALUE_STORE_DEFAULT_TABLE_NAME = "key_value_store_default";
	public static final String KEY_VALUE_STORE_ACTIVE_TABLE_NAME = "key_value_store_active";
	public static final String KEY_VALUE_STORE_SERVER_TABLE_NAME = "key_value_store_server";
	public static final String KEY_VALULE_STORE_SYNC_TABLE_NAME = "key_value_store_sync";

	/**
	 * table definitions table
	 */

	// only one of these...
	private static final String TABLE_DEFS_TABLE_NAME = "table_definitions";
	/**
	 * column definitions table
	 */

	// only one of these...
	private static final String COLUMN_DEFINITIONS_TABLE_NAME = "column_definitions";
	/**
	 * For ODK Survey (only)
	 *
	 * Tracks which rows have been sent to the server.
	 * TODO: rework to accommodate publishing to multiple formids for a given table row
	 */

	public static final String UPLOADS_TABLE_NAME = "uploads";

	/**
    * For ODK Survey (only)
    *
    * Tracks all the forms present in the forms directory.
    */

	public static final String FORMS_TABLE_NAME = "formDefs";


	public DataModelDatabaseHelper(String dbPath, String databaseName) {
		super(dbPath, databaseName, null, APP_KEY, APP_VERSION);
	}

	private void commonTableDefn(SQLiteDatabase db) {
		db.execSQL(InstanceColumns.getTableCreateSql(UPLOADS_TABLE_NAME));
		db.execSQL(FormsColumns.getTableCreateSql(FORMS_TABLE_NAME));
      db.execSQL(ColumnDefinitionsColumns.getTableCreateSql(COLUMN_DEFINITIONS_TABLE_NAME));
      db.execSQL(KeyValueStoreColumns.getTableCreateSql(KEY_VALUE_STORE_DEFAULT_TABLE_NAME));
      db.execSQL(KeyValueStoreColumns.getTableCreateSql(KEY_VALUE_STORE_ACTIVE_TABLE_NAME));
      db.execSQL(KeyValueStoreColumns.getTableCreateSql(KEY_VALUE_STORE_SERVER_TABLE_NAME));
      db.execSQL(KeyValueStoreColumns.getTableCreateSql(KEY_VALULE_STORE_SYNC_TABLE_NAME));
      db.execSQL(TableDefinitionsColumns.getTableCreateSql(TABLE_DEFS_TABLE_NAME));
	}

	@Override
	public void onCreateAppVersion(SQLiteDatabase db) {
		commonTableDefn(db);
	}

	@Override
	public void onUpgradeAppVersion(SQLiteDatabase db, int oldVersion,
			int newVersion) {
		// for now, upgrade and creation use the same codepath...
		commonTableDefn(db);
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
			c = db.query(TABLE_DEFS_TABLE_NAME,
					new String[] { TableDefinitionsColumns.DB_TABLE_NAME },
					TableDefinitionsColumns.TABLE_ID + "=?", new String[] { tableId }, null,
					null, null);

			if (c.moveToFirst()) {
				int idx = c.getColumnIndex(TableDefinitionsColumns.DB_TABLE_NAME);
				return c.getString(idx);
			}
		} finally {
			if (c != null && !c.isClosed()) {
				c.close();
			}
		}
		return null;
	}

	public static class Join {
		public final String tableId;
		public final String elementKey;

		Join(String tableId, String elementKey) {
			this.tableId = tableId;
			this.elementKey = elementKey;
		}
	};

	public static class ColumnDefinition {
		public final String elementKey;
		public final String elementName;
		public final String elementType;
		public final boolean isPersisted;

		public final ArrayList<ColumnDefinition> children = new ArrayList<ColumnDefinition>();
		public final ArrayList<Join> joins = new ArrayList<Join>();
		public ColumnDefinition parent = null;

		ColumnDefinition(String elementKey, String elementName,
				String elementType, boolean isPersisted) {
			this.elementKey = elementKey;
			this.elementName = elementName;
			this.elementType = elementType;
			this.isPersisted = isPersisted;
		}

		private void setParent(ColumnDefinition parent) {
			this.parent = parent;
		}

		void addJoin(Join j) {
			joins.add(j);
		}

		void addChild(ColumnDefinition child) {
			child.setParent(this);
			children.add(child);
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
	public static TreeMap<String, Object> getDataModel(
			Map<String, ColumnDefinition> defns) {
		TreeMap<String, Object> model = new TreeMap<String, Object>();

		for (ColumnDefinition c : defns.values()) {
			if (c.parent == null) {
				model.put(c.elementName, new TreeMap<String, Object>());
				@SuppressWarnings("unchecked")
				TreeMap<String, Object> jsonSchema = (TreeMap<String, Object>) model
						.get(c.elementName);
				getDataModelHelper(jsonSchema, c);
			}
		}
		return model;
	}

	private static void getDataModelHelper(TreeMap<String, Object> jsonSchema,
			ColumnDefinition c) {
		if (c.elementType.equals("string")) {
			jsonSchema.put("type", "string");
			jsonSchema.put("elementKey", c.elementKey);
			jsonSchema.put("isPersisted", c.isPersisted);
		} else if (c.elementType.equals("number")) {
			jsonSchema.put("type", "number");
			jsonSchema.put("elementKey", c.elementKey);
			jsonSchema.put("isPersisted", c.isPersisted);
		} else if (c.elementType.equals("integer")) {
			jsonSchema.put("type", "integer");
			jsonSchema.put("elementKey", c.elementKey);
			jsonSchema.put("isPersisted", c.isPersisted);
		} else if (c.elementType.equals("boolean")) {
			jsonSchema.put("type", "boolean");
			jsonSchema.put("elementKey", c.elementKey);
			jsonSchema.put("isPersisted", c.isPersisted);
		} else if (c.elementType.equals("array")) {
			jsonSchema.put("type", "array");
			jsonSchema.put("elementKey", c.elementKey);
			jsonSchema.put("isPersisted", c.isPersisted);
			ColumnDefinition ch = c.children.get(0);
			jsonSchema.put("items", new TreeMap<String, Object>());
			@SuppressWarnings("unchecked")
			TreeMap<String, Object> itemSchema = (TreeMap<String, Object>) jsonSchema
					.get("items");
			getDataModelHelper(itemSchema, ch); // recursion...
		} else {
			jsonSchema.put("type", "object");
			if (!c.elementType.equals("object")) {
				jsonSchema.put("elementType", c.elementType);
			}
			jsonSchema.put("elementKey", c.elementKey);
			jsonSchema.put("isPersisted", c.isPersisted);
			jsonSchema.put("properties", new TreeMap<String, Object>());
			@SuppressWarnings("unchecked")
			TreeMap<String, Object> propertiesSchema = (TreeMap<String, Object>) jsonSchema
					.get("properties");
			for (ColumnDefinition ch : c.children) {
				propertiesSchema.put(c.elementName,
						new TreeMap<String, Object>());
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
	public static Map<String, ColumnDefinition> getColumnDefinitions(
			SQLiteDatabase db, String tableId) throws JsonParseException,
			JsonMappingException, IOException {
		Map<String, ColumnDefinition> defn = new HashMap<String, ColumnDefinition>();

		Cursor c = null;
		try {
			c = db.query(COLUMN_DEFINITIONS_TABLE_NAME, null,
					ColumnDefinitionsColumns.TABLE_ID + "=?",
					new String[] { tableId }, null, null, null);

			if (c.moveToFirst()) {
				int idxEK = c.getColumnIndex(ColumnDefinitionsColumns.ELEMENT_KEY);
				int idxEN = c.getColumnIndex(ColumnDefinitionsColumns.ELEMENT_NAME);
				int idxET = c.getColumnIndex(ColumnDefinitionsColumns.ELEMENT_TYPE);
				int idxIP = c.getColumnIndex(ColumnDefinitionsColumns.IS_PERSISTED);
				int idxLIST = c
						.getColumnIndex(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS);
				int idxJOINS = c.getColumnIndex(ColumnDefinitionsColumns.JOINS);
				HashMap<String, ColumnContainer> ref = new HashMap<String, ColumnContainer>();

				do {
					String elementKey = c.getString(idxEK);
					String elementName = c.getString(idxEN);
					String elementType = c.getString(idxET);
					boolean isPersisted = (c.getInt(idxIP) != 0);
					String childrenString = c.isNull(idxLIST) ? null : c
							.getString(idxLIST);
					String joinsString = c.isNull(idxJOINS) ? null : c
							.getString(idxJOINS);
					ColumnContainer ctn = new ColumnContainer();
					ctn.defn = new ColumnDefinition(elementKey, elementName,
							elementType, isPersisted);

					if (childrenString != null) {
						@SuppressWarnings("unchecked")
						ArrayList<String> l = ODKFileUtils.mapper.readValue(
								childrenString, ArrayList.class);
						ctn.children = l;
					}

					if (joinsString != null) {
						@SuppressWarnings("unchecked")
						ArrayList<Object> joins = ODKFileUtils.mapper
								.readValue(joinsString, ArrayList.class);
						for (Object o : joins) {
							@SuppressWarnings("unchecked")
							Map<String, Object> m = (Map<String, Object>) o;
							String tId = (String) m.get("table_id");
							String tEK = (String) m.get("element_key");

							Join j = new Join(tId, tEK);
							ctn.defn.addJoin(j);
						}
					}

					ref.put(elementKey, ctn);
				} while (c.moveToNext());

				// OK now connect all the children...

				for (ColumnContainer ctn : ref.values()) {
					if (ctn.children != null) {
						for (String ek : ctn.children) {
							ColumnContainer child = ref.get(ek);
							if (child == null) {
								throw new IllegalArgumentException(
										"Unexpected missing child element: "
												+ ek);
							}
							ctn.defn.addChild(child.defn);
						}
					}
				}

				// and construct the list of entries...
				for (ColumnContainer ctn : ref.values()) {
					defn.put(ctn.defn.elementKey, ctn.defn);
				}
				return defn;
			}
		} finally {
			if (c != null && !c.isClosed()) {
				c.close();
			}
		}
		return null;
	}
}