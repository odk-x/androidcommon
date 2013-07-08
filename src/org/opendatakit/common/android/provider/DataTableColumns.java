/*
 * Copyright (C) 2013 University of Washington
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

package org.opendatakit.common.android.provider;

import org.opendatakit.aggregate.odktables.rest.TableConstants;

import android.provider.BaseColumns;

/**
 * Metadata Columns added to the user-defined data tables.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class DataTableColumns implements BaseColumns {

  /**
   * For simplicity, share the exact names with the REST interface to the server.
   */

  // tablename is chosen by user...
  public static final String ID = TableConstants.ID;
  public static final String ROW_ID = TableConstants.ID;
  public static final String URI_USER = TableConstants.URI_USER;
  public static final String SYNC_TAG = TableConstants.SYNC_TAG;
  public static final String SYNC_STATE = TableConstants.SYNC_STATE;
  public static final String TRANSACTIONING = TableConstants.TRANSACTIONING;

  /**
   * (timestamp, saved, form_id) are the tuple written and managed by ODK Survey
   * when a record is updated. ODK Tables needs to update these appropriately
   * when a cell is directly edited based upon whether or not the table is
   * 'form-managed' or not.
   *
   * timestamp and last_mod_time are the same field. last_mod_time is simply
   * a well-formatted text representation of the timestamp value.
   */
  public static final String TIMESTAMP = TableConstants.TIMESTAMP;
  public static final String SAVED = TableConstants.SAVED;
  public static final String FORM_ID = TableConstants.FORM_ID;
  /*
   * For ODKTables generated rows (as opposed to ODK Collect), the thought is
   * that this instance name would just be the iso86 pretty print date of
   * creation.
   */
  public static final String INSTANCE_NAME = TableConstants.INSTANCE_NAME;
  public static final String LOCALE = TableConstants.LOCALE;

  // These are the default values that will be set to the database in case
  // there is nothing included. This has been a problem when downloading a
  // table from the server.
  public static final String DEFAULT_INSTANCE_NAME = "";
  public static final String DEFAULT_LOCALE= "";
  public static final String DEFAULT_URI_USER = "";
  public static final String DEFAULT_SYNC_TAG = "";

  // This class cannot be instantiated
  private DataTableColumns() {
  }

}
