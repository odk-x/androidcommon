package org.opendatakit.common.android.provider;

import android.provider.BaseColumns;

/**
 * ODK Survey (only)
 *
 * Tracks what forms are available in the ODK Survey forms directory.
 */
public final class FormsColumns implements BaseColumns {
	// This class cannot be instantiated
	private FormsColumns() {
	}

	public static final String CONTENT_TYPE = "vnd.android.cursor.dir/vnd.opendatakit.form";
	public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/vnd.opendatakit.form";

	// These are the only things needed for an insert
	public static final String TABLE_ID = "tableId"; // for Tables linkage
	public static final String FORM_ID = "formId";
	public static final String FORM_VERSION = "formVersion"; // can be null
	public static final String DISPLAY_NAME = "displayName";
	public static final String DESCRIPTION = "description"; // can be null
	public static final String FORM_FILE_PATH = "formFilePath"; // ODK2:
																// within
																// the media
																// directory
	public static final String FORM_MEDIA_PATH = "formMediaPath"; // directory
																	// containing
																	// formDef.json
	public static final String FORM_PATH = "formPath"; // relative path for
														// WebKit
	public static final String DEFAULT_FORM_LOCALE = "defaultFormLocale";
	public static final String XML_SUBMISSION_URL = "xmlSubmissionUrl"; // ODK1
																		// support
																		// -
																		// can
																		// be
																		// null
	public static final String XML_BASE64_RSA_PUBLIC_KEY = "xmlBase64RsaPublicKey"; // ODK1
																					// support
																					// -
																					// can
																					// be
																					// null
	public static final String XML_ROOT_ELEMENT_NAME = "xmlRootElementName"; // ODK1
																				// support
																				// -
																				// can
																				// be
																				// null
	public static final String XML_DEVICE_ID_PROPERTY_NAME = "xmlDeviceIdPropertyName";
	public static final String XML_USER_ID_PROPERTY_NAME = "xmlUserIdPropertyName";

	// these are generated for you (but you can insert something else if you
	// want)
	public static final String DISPLAY_SUBTEXT = "displaySubtext";
	public static final String MD5_HASH = "md5Hash";
	public static final String DATE = "date"; // last modification date

	// NOTE: this omits _ID (the primary key)
	public static final String[] formsDataColumnNames = { DISPLAY_NAME,
			DISPLAY_SUBTEXT, DESCRIPTION, TABLE_ID, FORM_ID, FORM_VERSION,
			FORM_FILE_PATH, FORM_MEDIA_PATH, FORM_PATH, MD5_HASH, DATE,
			DEFAULT_FORM_LOCALE, XML_SUBMISSION_URL,
			XML_BASE64_RSA_PUBLIC_KEY, XML_DEVICE_ID_PROPERTY_NAME,
			XML_USER_ID_PROPERTY_NAME, XML_ROOT_ELEMENT_NAME };

	  /**
	   * Get the create sql for the forms table (ODK Survey only).
	   *
	   * @return
	   */
	  public static String getTableCreateSql(String tableName) {
	    //@formatter:off
	      return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
	            + _ID + " integer primary key, "
	            + DISPLAY_NAME + " text not null, "
	            + DISPLAY_SUBTEXT + " text not null, "
	            + DESCRIPTION + " text, "
	            + TABLE_ID + " text not null, "
	            + FORM_ID + " text not null, "
	            + FORM_VERSION + " text, "
	            + FORM_FILE_PATH + " text null, "
	            + FORM_MEDIA_PATH + " text not null, "
	            + FORM_PATH + " text not null, "
	            + MD5_HASH + " text not null, "
	            + DATE + " integer not null, " // milliseconds
	            + DEFAULT_FORM_LOCALE + " text, "
	            + XML_SUBMISSION_URL + " text, "
	            + XML_BASE64_RSA_PUBLIC_KEY + " text, "
	            + XML_ROOT_ELEMENT_NAME + " text, "
	            + XML_DEVICE_ID_PROPERTY_NAME + " text, "
	            + XML_USER_ID_PROPERTY_NAME + " text )";
       //@formatter:on
	  }

}