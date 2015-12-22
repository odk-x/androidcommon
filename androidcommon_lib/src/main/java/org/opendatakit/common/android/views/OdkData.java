/*
 * Copyright (C) 2015 University of Washington
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

package org.opendatakit.common.android.views;

import android.app.Activity;
import android.os.Bundle;
import org.opendatakit.IntentConsts;
import org.opendatakit.common.android.provider.DataTableColumns;

public class OdkData {

   public static final String descOrder = "DESC";

   public static class IntentKeys {

      public static final String ACTION_TABLE_ID = "actionTableId";
      /** tables that have conflict rows */
      public static final String CONFLICT_TABLES = "conflictTables";
      /** tables that have checkpoint rows */
      public static final String CHECKPOINT_TABLES = "checkpointTables";

      /** for conflict resolution screens */
      public static final String TABLE_ID = "tableId";
      /** common for all activities */
      public static final String APP_NAME = "appName";
      /**
       * Tells what time of view it should be
       * displaying.
       */
      public static final String TABLE_DISPLAY_VIEW_TYPE = "tableDisplayViewType";
      public static final String FILE_NAME = "filename";
      public static final String ROW_ID = "rowId";
      /** The name of the graph view that should be displayed. */
      public static final String GRAPH_NAME = "graphName";
      public static final String ELEMENT_KEY = "elementKey";
      public static final String COLOR_RULE_TYPE = "colorRuleType";
      /**
       * The  that should be displayed when launching a
       */
      public static final String TABLE_PREFERENCE_FRAGMENT_TYPE = "tablePreferenceFragmentType";
      /**
       * Key to the where clause if this list view is to be opened with a more
       * complex query than permissible by the simple query object.
       */
      public static final String SQL_WHERE = "sqlWhereClause";
      /**
       * An array of strings for restricting the rows displayed in the table.
       */
      public static final String SQL_SELECTION_ARGS = "sqlSelectionArgs";
      /**
       * An array of strings giving the group by columns. What was formerly
       * 'overview' mode is a non-null groupBy list.
       */
      public static final String SQL_GROUP_BY_ARGS = "sqlGroupByArgs";
      /**
       * The having clause, if present
       */
      public static final String SQL_HAVING = "sqlHavingClause";
      /**
       * The order by column. NOTE: restricted to a single column
       */
      public static final String SQL_ORDER_BY_ELEMENT_KEY = "sqlOrderByElementKey";
      /**
       * The order by direction (ASC or DESC)
       */
      public static final String SQL_ORDER_BY_DIRECTION = "sqlOrderByDirection";
   }

   protected Activity mActivity;

   private static final String TAG = OdkData.class.getSimpleName();

   private final ICallbackFragment mFragment;

   private ExecutorContext context;

   public OdkData(ICallbackFragment fragment, Activity activity) {
      mFragment = fragment;
      mActivity = activity;
      // change to support multiple data objects within a single webpage
      context = ExecutorContext.getContext(mFragment);
   }

   public synchronized void refreshContext() {
      if ( !context.isAlive() ) {
         context = ExecutorContext.getContext(mFragment);
      }
   }

   public synchronized void shutdownContext() {
      context.releaseResources("Shutting down context");
   }

   private void queueRequest(ExecutorRequest request) {
      context.queueRequest(request);
   }

   public OdkDataIf getJavascriptInterfaceWithWeakReference() {
      return new OdkDataIf(this);
   }

   /**
    * Access the result of a request
    *
    * @return null if there is no result, otherwise the responseJSON of the last action
    */
   public String getResponseJSON() {
      return mFragment.getResponseJSON();
   }

   /**
    * Get the data for the view once the user is ready for it.
    * When the user chooses to launch a detail, list, or map view
    * they will have to call this via the JS API with success and
    * failure callback functions to manipulate the data for their views
    *
    */
   public void getViewData(String callbackJSON) {
      Bundle bundle = this.mActivity.getIntent().getExtras();

      String tableId = bundle.getString(IntentKeys.TABLE_ID);
      if (tableId == null || tableId.isEmpty()) {
         throw new IllegalArgumentException("Tables view launched without tableId specified");
      }

      // This was changed to use
      String rowId = bundle.getString(IntentConsts.INTENT_KEY_INSTANCE_ID);
      String whereClause = bundle.getString(IntentKeys.SQL_WHERE);
      String[] selArgs = bundle.getStringArray(IntentKeys.SQL_SELECTION_ARGS);
      String[] groupBy = bundle.getStringArray(IntentKeys.SQL_GROUP_BY_ARGS);
      String havingClause = bundle.getString(IntentKeys.SQL_HAVING);
      String orderByElemKey = bundle.getString(IntentKeys.SQL_ORDER_BY_ELEMENT_KEY);
      String orderByDir = bundle.getString(IntentKeys.SQL_ORDER_BY_DIRECTION);

      if (rowId != null && !rowId.isEmpty()) {
         query(tableId, DataTableColumns.ID + "=?", new String[]{rowId}, null, null,
             DataTableColumns.SAVEPOINT_TIMESTAMP, descOrder, true, callbackJSON);
      } else {
         query(tableId, whereClause, selArgs, groupBy, havingClause, orderByElemKey, orderByDir,
             true, callbackJSON);
      }
   }

   /**
    * Query the database using sql.
    *
    * @param tableId                 The table being queried. This is a user-defined table.
    * @param whereClause             The where clause for the query
    * @param sqlBindParams           The array of bind parameter values (including any in the having clause)
    * @param groupBy                 The array of columns to group by
    * @param having                  The having clause
    * @param orderByElementKey       The column to order by
    * @param orderByDirection        'ASC' or 'DESC' ordering
    * @param includeKeyValueStoreMap true if the keyValueStoreMap should be returned
    * @param callbackJSON            The JSON object used by the JS layer to recover the callback function
    *                                that can process the response
    */
   public void query(String tableId, String whereClause, String[] sqlBindParams, String[] groupBy,
       String having, String orderByElementKey, String orderByDirection,
       boolean includeKeyValueStoreMap, String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(tableId, whereClause, sqlBindParams, groupBy,
          having, orderByElementKey, orderByDirection, includeKeyValueStoreMap, callbackJSON);

      queueRequest(request);
   }

   /**
    * Raw SQL query
    *
    * @param sqlCommand           The Select statement to issue. It can reference any table in the database, including system tables.
    * @param sqlBindParams        The array of bind parameter values (including any in the having clause)
    * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
    *                             that can process the response
    * @return see description in class header
    */
   public void rawQuery(String sqlCommand, String[] sqlBindParams, String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(sqlCommand, sqlBindParams, callbackJSON);

      queueRequest(request);
   }

  /**
   * Get all rows that match the given rowId.
   * This can be zero, one or more. It is more than one if there
   * is a sync conflict or if there are edit checkpoints.
   *
   * @param tableId              The table being updated
   * @param rowId                The rowId of the row being added.
   * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
   *                             that can process the response
   */
  public void getRows(String tableId, String rowId, String callbackJSON) {
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_GET_ROWS, tableId,
        null, rowId, callbackJSON);

    queueRequest(request);
  }

   /**
    * Get the most recent checkpoint or last state for a row in the table.
    * Throws an exception if this row is in conflict.
    * Returns an empty rowset if the rowId is not present in the table.
    *
    * @param tableId              The table being updated
    * @param rowId                The rowId of the row being added.
    * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
    *                             that can process the response
    */
   public void getMostRecentRow(String tableId, String rowId, String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_GET_MOST_RECENT_ROW, tableId,
          null, rowId, callbackJSON);

      queueRequest(request);
   }

   /**
    * Update a row in the table
    *
    * @param tableId              The table being updated
    * @param stringifiedJSON      key-value map of values to store or update. If missing, the value remains unchanged.
    * @param rowId                The rowId of the row being changed.
    * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
    *                             that can process the response
    * @return see description in class header
    */
   public void updateRow(String tableId, String stringifiedJSON, String rowId, String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_UPDATE_ROW,
          tableId, stringifiedJSON, rowId, callbackJSON);

      queueRequest(request);
   }

   /**
    * Delete a row from the table
    *
    * @param tableId              The table being updated
    * @param stringifiedJSON      key-value map of values to store or update. If missing, the value remains unchanged.
    * @param rowId                The rowId of the row being deleted.
    * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
    *                             that can process the response
    */
   public void deleteRow(String tableId, String stringifiedJSON, String rowId, String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_DELETE_ROW,
          tableId, stringifiedJSON, rowId, callbackJSON);

      queueRequest(request);
   }

   /**
    * Add a row in the table
    *
    * @param tableId              The table being updated
    * @param stringifiedJSON      key-value map of values to store or update. If missing, the value remains unchanged.
    * @param rowId                The rowId of the row being added.
    * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
    *                             that can process the response
    */
   public void addRow(String tableId, String stringifiedJSON, String rowId, String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_ADD_ROW, tableId,
          stringifiedJSON, rowId, callbackJSON);

      queueRequest(request);
   }

   /**
    * Update the row, marking the updates as a checkpoint save.
    *
    * @param tableId              The table being updated
    * @param stringifiedJSON      key-value map of values to store or update. If missing, the value remains unchanged.
    * @param rowId                The rowId of the row being added.
    * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
    *                             that can process the response
    */
   public void addCheckpoint(String tableId, String stringifiedJSON, String rowId,
       String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_ADD_CHECKPOINT,
          tableId, stringifiedJSON, rowId, callbackJSON);

      queueRequest(request);
   }

   /**
    * Save checkpoint as incomplete. In the process, it applies any changes indicated by the stringifiedJSON.
    *
    * @param tableId              The table being updated
    * @param stringifiedJSON      key-value map of values to store or update. If missing, the value remains unchanged.
    * @param rowId                The rowId of the row being saved-as-incomplete.
    * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
    *                             that can process the response
    */
   public void saveCheckpointAsIncomplete(String tableId, String stringifiedJSON, String rowId,
       String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(
          ExecutorRequestType.USER_TABLE_SAVE_CHECKPOINT_AS_INCOMPLETE, tableId,
          stringifiedJSON, rowId, callbackJSON);

      queueRequest(request);
   }

   /**
    * Save checkpoint as complete.
    *
    * @param tableId              The table being updated
    * @param stringifiedJSON      key-value map of values to store or update. If missing, the value remains unchanged.
    * @param rowId                The rowId of the row being marked-as-complete.
    * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
    *                             that can process the response
    */
   public void saveCheckpointAsComplete(String tableId, String stringifiedJSON, String rowId, String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(
          ExecutorRequestType.USER_TABLE_SAVE_CHECKPOINT_AS_COMPLETE, tableId, stringifiedJSON,
          rowId, callbackJSON);

      queueRequest(request);
   }

  /**
   * Delete all checkpoint.  Checkpoints accumulate; this removes all of them.
   *
   * @param tableId              The table being updated
   * @param rowId                The rowId of the row being saved-as-incomplete.
   * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
   *                             that can process the response
   */
  public void deleteAllCheckpoints(String tableId, String rowId, String callbackJSON) {
    ExecutorRequest request = new ExecutorRequest(
        ExecutorRequestType.USER_TABLE_DELETE_ALL_CHECKPOINTS, tableId, null, rowId, callbackJSON);

    queueRequest(request);
  }

   /**
    * Delete last checkpoint.  Checkpoints accumulate; this removes the most recent one, leaving earlier ones.
    *
    * @param tableId              The table being updated
    * @param rowId                The rowId of the row being saved-as-incomplete.
    * @param callbackJSON         The JSON object used by the JS layer to recover the callback function
    *                             that can process the response
    */
   public void deleteLastCheckpoint(String tableId, String rowId, String callbackJSON) {
      ExecutorRequest request = new ExecutorRequest(
          ExecutorRequestType.USER_TABLE_DELETE_LAST_CHECKPOINT, tableId, null, rowId, callbackJSON);

      queueRequest(request);
   }

}
