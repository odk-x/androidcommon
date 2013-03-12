/*
 * Copyright (C) 2007 The Android Open Source Project
 * Copyright (C) 2011-2013 University of Washington
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

package org.opendatakit.common.android.provider.impl;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.opendatakit.common.android.R;
import org.opendatakit.common.android.database.DataModelDatabaseHelper;
import org.opendatakit.common.android.database.DataModelDatabaseHelper.IdStruct;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.provider.InstanceColumns;
import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;

/**
 * TODO: convert to true app-scoped instance provider
 */
public abstract class InstanceProviderImpl extends CommonContentProvider {

	// private static final String t = "InstancesProviderImpl";

	private static final String DATA_TABLE_ID_COLUMN = InstanceColumns.DATA_TABLE_INSTANCE_ID;
	private static final String DATA_TABLE_TIMESTAMP_COLUMN = DataTableColumns.TIMESTAMP;
	private static final String DATA_TABLE_INSTANCE_NAME_COLUMN = DataTableColumns.INSTANCE_NAME;
	private static final String DATA_TABLE_SAVED_COLUMN = DataTableColumns.SAVED;
   private static final String DATA_TABLE_FORM_ID_COLUMN = DataTableColumns.FORM_ID;
	private static final String SAVED_AS_COMPLETE = "COMPLETE";

	private static HashMap<String, String> sInstancesProjectionMap;

	public abstract String getInstanceAuthority();

	@Override
	public boolean onCreate() {

	  return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		List<String> segments = uri.getPathSegments();

		if (segments.size() < 2 || segments.size() > 3) {
			throw new IllegalArgumentException(
					"Unknown URI (too many segments!) " + uri);
		}

		String appName = segments.get(0);
		String uriFormId = segments.get(1);
		// _ID in UPLOADS_TABLE_NAME
		String instanceId = (segments.size() == 3 ? segments.get(2) : null);

		SQLiteDatabase db = getDbHelper(appName).getReadableDatabase();

		IdStruct ids = DataModelDatabaseHelper.getIds(db, uriFormId);

		if (ids == null) {
		  throw new IllegalArgumentException(
		      "Unknown URI (no matching formId) " + uri);
		}
		String dbTableName = DataModelDatabaseHelper.getDbTableName(db, ids.tableId);
		if (dbTableName == null) {
			throw new IllegalArgumentException(
					"Unknown URI (missing data table for formId) " + uri);
		}

		dbTableName = "\"" + dbTableName + "\"";

		// ARGH! we must ensure that we have records in our UPLOADS_TABLE_NAME
		// for every distinct instance in the data table.
		StringBuilder b = new StringBuilder();
		b.append("INSERT INTO ").append(DataModelDatabaseHelper.UPLOADS_TABLE_NAME).append("(")
				.append(InstanceColumns.DATA_TABLE_INSTANCE_ID).append(",")
            .append(InstanceColumns.DATA_TABLE_TABLE_ID).append(",")
            .append(InstanceColumns.XML_PUBLISH_FORM_ID)
				.append(") ")
				.append("SELECT ")
				.append(InstanceColumns.DATA_TABLE_INSTANCE_ID).append(",")
				.append(InstanceColumns.DATA_TABLE_TABLE_ID).append(",")
            .append(InstanceColumns.XML_PUBLISH_FORM_ID).append(",")
				.append(" FROM (").append("SELECT DISTINCT ")
				.append(DATA_TABLE_ID_COLUMN)
				   .append(" as ").append(InstanceColumns.DATA_TABLE_INSTANCE_ID).append(",")
            .append("? as ").append(InstanceColumns.DATA_TABLE_TABLE_ID).append(",")
            .append("? as ").append(InstanceColumns.XML_PUBLISH_FORM_ID).append(",")
				.append(" FROM ").append(dbTableName)
				.append(" EXCEPT SELECT DISTINCT ")
				.append(InstanceColumns.DATA_TABLE_INSTANCE_ID).append(",")
            .append(InstanceColumns.DATA_TABLE_TABLE_ID).append(",")
            .append(InstanceColumns.XML_PUBLISH_FORM_ID)
				.append(" FROM ").append(DataModelDatabaseHelper.UPLOADS_TABLE_NAME).append(")");

		String[] args = { ids.tableId, ids.formId };
		db.execSQL(b.toString(), args);

		// We can now join through and access the data table rows

		b.setLength(0);
		b.append("SELECT ");
		b.append(DataModelDatabaseHelper.UPLOADS_TABLE_NAME).append(".").append(InstanceColumns._ID)
		 .append(DataModelDatabaseHelper.UPLOADS_TABLE_NAME).append(".").append(InstanceColumns.XML_PUBLISH_FORM_ID)
				.append(",").append(dbTableName).append(".").append("*")
				.append(",");
		// b.append(dbTableName).append(".").append(InstanceColumns._ID).append(",");
		b.append("CASE WHEN ").append(DATA_TABLE_TIMESTAMP_COLUMN)
				.append(" IS NULL THEN null").append(" WHEN ")
				.append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
				.append(" IS NULL THEN null").append(" WHEN ")
				.append(DATA_TABLE_TIMESTAMP_COLUMN).append(" > ")
				.append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
				.append(" THEN null").append(" ELSE ")
				.append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
				.append(" END as ")
				.append(InstanceColumns.XML_PUBLISH_TIMESTAMP).append(",");
		b.append("CASE WHEN ").append(DATA_TABLE_TIMESTAMP_COLUMN)
				.append(" IS NULL THEN null").append(" WHEN ")
				.append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
				.append(" IS NULL THEN null").append(" WHEN ")
				.append(DATA_TABLE_TIMESTAMP_COLUMN).append(" > ")
				.append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
				.append(" THEN null").append(" ELSE ")
				.append(InstanceColumns.XML_PUBLISH_STATUS)
				.append(" END as ")
				.append(InstanceColumns.XML_PUBLISH_STATUS).append(",");
		b.append("CASE WHEN ").append(DATA_TABLE_TIMESTAMP_COLUMN)
				.append(" IS NULL THEN null").append(" WHEN ")
				.append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
				.append(" IS NULL THEN null").append(" WHEN ")
				.append(DATA_TABLE_TIMESTAMP_COLUMN).append(" > ")
				.append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
				.append(" THEN null").append(" ELSE ")
				.append(InstanceColumns.DISPLAY_SUBTEXT)
				.append(" END as ")
				.append(InstanceColumns.DISPLAY_SUBTEXT).append(",");
		b.append(DATA_TABLE_INSTANCE_NAME_COLUMN).append(" as ")
				.append(InstanceColumns.DISPLAY_NAME);
		b.append(" FROM ");
		b.append("( SELECT * FROM ").append(dbTableName).append(" GROUP BY ")
				.append(DATA_TABLE_ID_COLUMN).append(" HAVING ")
				.append(DATA_TABLE_TIMESTAMP_COLUMN).append(" = MAX(")
				.append(DATA_TABLE_TIMESTAMP_COLUMN).append(")")
				.append(") as ")
				.append(dbTableName);
		b.append(" JOIN ").append(DataModelDatabaseHelper.UPLOADS_TABLE_NAME).append(" ON ")
				.append(dbTableName).append(".").append(DATA_TABLE_ID_COLUMN)
				.append("=").append(DataModelDatabaseHelper.UPLOADS_TABLE_NAME).append(".")
				.append(InstanceColumns.DATA_TABLE_INSTANCE_ID)
				.append(" AND ")
				.append("? =").append(DataModelDatabaseHelper.UPLOADS_TABLE_NAME).append(".")
				.append(InstanceColumns.DATA_TABLE_TABLE_ID)
            .append(" AND ")
            .append("? =").append(DataModelDatabaseHelper.UPLOADS_TABLE_NAME).append(".")
            .append(InstanceColumns.XML_PUBLISH_FORM_ID);
		b.append(" WHERE ")
       .append(DATA_TABLE_SAVED_COLUMN).append("=?")
       ;

		String filterArgs[];
		if ( instanceId != null ) {
		  b.append(" AND ").append(DataModelDatabaseHelper.UPLOADS_TABLE_NAME).append(".")
          .append(InstanceColumns._ID).append("=?");
		    String tempArgs[] = { ids.tableId, ids.formId, InstanceColumns.STATUS_COMPLETE, instanceId };
		    filterArgs = tempArgs;
	   } else {
	     String tempArgs[] = { ids.tableId, ids.formId, InstanceColumns.STATUS_COMPLETE };
        filterArgs = tempArgs;
	   }

		if ( selection != null ) {
        b.append(" AND (").append(selection).append(")");
		}

		if (selectionArgs != null) {
			String[] tempArgs = new String[filterArgs.length+selectionArgs.length];
         for (int i = 0; i < filterArgs.length; ++i) {
           tempArgs[i] = filterArgs[i];
        }
			for (int i = 0; i < selectionArgs.length; ++i) {
			  tempArgs[filterArgs.length+i] = selectionArgs[i];
			}
			filterArgs = tempArgs;
		}

		if (sortOrder != null) {
			b.append(" ORDER BY ").append(sortOrder);
		}
		Cursor c = db.rawQuery(b.toString(), filterArgs);
		// Tell the cursor what uri to watch, so it knows when its source data
		// changes
		c.setNotificationUri(getContext().getContentResolver(), uri);
		return c;
	}

	@Override
	public String getType(Uri uri) {
		// don't see the point of trying to implement this call...
		return null;
		// switch (sUriMatcher.match(uri)) {
		// case INSTANCES:
		// return InstanceColumns.CONTENT_TYPE;
		//
		// case INSTANCE_ID:
		// return InstanceColumns.CONTENT_ITEM_TYPE;
		//
		// default:
		// throw new IllegalArgumentException("Unknown URI " + uri);
		// }
	}

	@Override
	public Uri insert(Uri uri, ContentValues initialValues) {
		throw new IllegalArgumentException("Insert not implemented!");
	}

	private String getDisplaySubtext(String xmlPublishStatus,
			Date xmlPublishDate) {
		if (xmlPublishDate == null) {
			return getContext().getString(R.string.not_yet_sent);
		} else if (InstanceColumns.STATUS_SUBMITTED
				.equalsIgnoreCase(xmlPublishStatus)) {
			return new SimpleDateFormat(getContext().getString(
					R.string.sent_on_date_at_time), Locale.getDefault())
					.format(xmlPublishDate);
		} else if (InstanceColumns.STATUS_SUBMISSION_FAILED
				.equalsIgnoreCase(xmlPublishStatus)) {
			return new SimpleDateFormat(getContext().getString(
					R.string.sending_failed_on_date_at_time),
					Locale.getDefault()).format(xmlPublishDate);
		} else {
			throw new IllegalStateException("Unrecognized xmlPublishStatus: "
					+ xmlPublishStatus);
		}
	}

	/**
	 * This method removes the entry from the content provider, and also removes
	 * any associated files. files: form.xml, [formmd5].formdef, formname
	 * {directory}
	 */
	@Override
	public int delete(Uri uri, String where, String[] whereArgs) {
     List<String> segments = uri.getPathSegments();

     if (segments.size() < 2 || segments.size() > 3) {
        throw new IllegalArgumentException(
              "Unknown URI (too many segments!) " + uri);
     }

     String appName = segments.get(0);
     String tableId = segments.get(1);
     // _ID in UPLOADS_TABLE_NAME
     String instanceId = (segments.size() == 3 ? segments.get(2) : null);

     SQLiteDatabase db = getDbHelper(appName).getWritableDatabase();

		String dbTableName = DataModelDatabaseHelper
				.getDbTableName(db, tableId);
		if (dbTableName == null) {
			return 0; // not known...
		}

		dbTableName = "\"" + dbTableName + "\"";

		if (segments.size() == 2) {
			where = "(" + where + ") AND ("
					+ InstanceColumns.DATA_TABLE_INSTANCE_ID + "=? )";
			if (whereArgs != null) {
				String[] args = new String[whereArgs.length + 1];
				for (int i = 0; i < whereArgs.length; ++i) {
					args[i] = whereArgs[i];
				}
				args[whereArgs.length] = instanceId;
				whereArgs = args;
			} else {
				whereArgs = new String[] { instanceId };
			}
		}

		List<String> ids = new ArrayList<String>();
		Cursor del = null;
		try {
			del = this.query(uri, null, where, whereArgs, null);
			del.moveToPosition(-1);
			while (del.moveToNext()) {
				String iId = del
						.getString(del
								.getColumnIndex(InstanceColumns.DATA_TABLE_INSTANCE_ID));
				ids.add(iId);
				String path = ODKFileUtils.getInstanceFolder(appName, tableId, iId);
				File f = new File(path);
				if (f.exists()) {
					if (f.isDirectory()) {
						FileUtils.deleteDirectory(f);
					} else {
						f.delete();
					}
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(
					"Unable to delete instance directory: " + e.toString());
		} finally {
			if (del != null) {
				del.close();
			}
		}

		for (String id : ids) {
			db.delete(DataModelDatabaseHelper.UPLOADS_TABLE_NAME,
					InstanceColumns.DATA_TABLE_INSTANCE_ID + "=?",
					new String[] { id });
			db.delete(dbTableName, DATA_TABLE_ID_COLUMN + "=?",
					new String[] { id });
		}
		getContext().getContentResolver().notifyChange(uri, null);
		return ids.size();
	}

	@Override
	public int update(Uri uri, ContentValues values, String where,
			String[] whereArgs) {
     List<String> segments = uri.getPathSegments();

     if (segments.size() != 3) {
        throw new IllegalArgumentException(
              "Unknown URI (does not specify instance!) " + uri);
     }

     String appName = segments.get(0);
     String tableId = segments.get(1);
     // _ID in UPLOADS_TABLE_NAME
     String instanceId = segments.get(2);
     // TODO: should we do something to ensure instanceId is the one updated?

     SQLiteDatabase db = getDbHelper(appName).getWritableDatabase();

		String dbTableName = DataModelDatabaseHelper
				.getDbTableName(db, tableId);
		if (dbTableName == null) {
			throw new IllegalArgumentException(
					"Unknown URI (no matching tableId) " + uri);
		}

		dbTableName = "\"" + dbTableName + "\"";

		// run the query to get all the ids...
		List<String> ids = new ArrayList<String>();
		Cursor ref = null;
		try {
			// use this provider's query interface to get the set of ids that
			// match (if any)
			ref = this.query(uri, null, where, whereArgs, null);
			ref.moveToPosition(-1);
			while (ref.moveToNext()) {
				String iId = ref
						.getString(ref
								.getColumnIndex(InstanceColumns.DATA_TABLE_INSTANCE_ID));
				ids.add(iId);
			}
		} finally {
			if (ref != null) {
				ref.close();
			}
		}

		// update the values string...
		if (values.containsKey(InstanceColumns.XML_PUBLISH_STATUS)) {
			Date xmlPublishDate = new Date();
			values.put(InstanceColumns.XML_PUBLISH_TIMESTAMP,
					xmlPublishDate.getTime());
			String xmlPublishStatus = values
					.getAsString(InstanceColumns.XML_PUBLISH_STATUS);
			if (values.containsKey(InstanceColumns.DISPLAY_SUBTEXT) == false) {
				String text = getDisplaySubtext(xmlPublishStatus,
						xmlPublishDate);
				values.put(InstanceColumns.DISPLAY_SUBTEXT, text);
			}
		}

		int count = 0;
		for (String id : ids) {
			values.put(InstanceColumns.DATA_TABLE_INSTANCE_ID, id);
			count += (db.replace(DataModelDatabaseHelper.UPLOADS_TABLE_NAME, null, values) != -1) ? 1
					: 0;
		}
		getContext().getContentResolver().notifyChange(uri, null);
		return count;
	}

	static {

		sInstancesProjectionMap = new HashMap<String, String>();
		sInstancesProjectionMap.put(InstanceColumns._ID, InstanceColumns._ID);
		sInstancesProjectionMap.put(InstanceColumns.DATA_TABLE_INSTANCE_ID,
				InstanceColumns.DATA_TABLE_INSTANCE_ID);
		sInstancesProjectionMap.put(InstanceColumns.XML_PUBLISH_TIMESTAMP,
				InstanceColumns.XML_PUBLISH_TIMESTAMP);
		sInstancesProjectionMap.put(InstanceColumns.XML_PUBLISH_STATUS,
				InstanceColumns.XML_PUBLISH_STATUS);
	}

}
