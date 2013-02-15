package org.opendatakit.common.android.provider;

import android.provider.BaseColumns;

/**
 * ODK Survey (only)
 *
 * Tracks the upload status of each row in each data table.
 * TODO: revise to support publishing into multiple formid streams
 */
public final class InstanceColumns implements BaseColumns {
	// saved status from row in data table:
	public static final String STATUS_INCOMPLETE = "INCOMPLETE";
	public static final String STATUS_COMPLETE = "COMPLETE";
	// xmlPublishStatus from instances db:
	public static final String STATUS_SUBMITTED = "submitted";
	public static final String STATUS_SUBMISSION_FAILED = "submissionFailed";
	// This class cannot be instantiated
	private InstanceColumns() {
	}

	public static final String CONTENT_TYPE = "vnd.android.cursor.dir/vnd.opendatakit.instance";
	public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/vnd.opendatakit.instance";

	// These are the only things needed for an insert
	// _ID is the index on the table maintained for ODK Survey purposes
	// DATA_TABLE_INSTANCE_ID ****MUST MATCH**** value used in javascript
	public static final String DATA_TABLE_INSTANCE_ID = "id"; // join on
																// data
																// table...
	public static final String XML_PUBLISH_TIMESTAMP = "xmlPublishTimestamp";
	public static final String XML_PUBLISH_STATUS = "xmlPublishStatus";
	public static final String DISPLAY_NAME = "displayName";
	public static final String DISPLAY_SUBTEXT = "displaySubtext";


   /**
    * Get the create sql for the forms table (ODK Survey only).
    *
    * @return
    */
   public static String getTableCreateSql(String tableName) {
     //@formatter:off
       return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
           + _ID + " integer primary key, "
           + DATA_TABLE_INSTANCE_ID + " text unique, "
           + XML_PUBLISH_TIMESTAMP + " integer, "
           + XML_PUBLISH_STATUS + " text, "
           + DISPLAY_SUBTEXT + " text)";
     //@formatter:on
   }

}