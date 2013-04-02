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

import android.provider.BaseColumns;

/**
 * Metadata Columns added to the user-defined data tables.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class DataTableColumns implements BaseColumns {

  // tablename is chosen by user...
  public static final String ID = "id";
  public static final String ROW_ID = "id";
  public static final String URI_USER = "uri_user";
  public static final String LAST_MODIFIED_TIME = "last_mod_time";
  public static final String SYNC_TAG = "sync_tag";
  public static final String SYNC_STATE = "sync_state";
  public static final String TRANSACTIONING = "transactioning";

  /**
   * (timestamp, saved, form_id) are the tuple written and managed by ODK Survey
   * when a record is updated. ODK Tables needs to update these appropriately
   * when a cell is directly edited based upon whether or not the table is
   * 'form-managed' or not.
   */
  public static final String TIMESTAMP = "timestamp";
  public static final String SAVED = "saved";
  public static final String FORM_ID = "form_id";
  /*
   * For ODKTables generated rows (as opposed to ODK Collect), the thought is
   * that this instance name would just be the iso86 pretty print date of
   * creation.
   */
  public static final String INSTANCE_NAME = "instance_name";
  public static final String LOCALE = "locale";

  // This class cannot be instantiated
  private DataTableColumns() {
  }

}
