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

package org.opendatakit.views;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.opendatakit.activities.IOdkDataActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.utilities.ODKFileUtils;

import java.lang.ref.WeakReference;
import java.util.HashMap;

public class OdkData {

  public static final String descOrder = "DESC";

  public static class IntentKeys {

    public static final String ACTION_TABLE_ID = "actionTableId";
    /**
     * tables that have conflict rows
     */
    public static final String CONFLICT_TABLES = "conflictTables";
    /**
     * tables that have checkpoint rows
     */
    public static final String CHECKPOINT_TABLES = "checkpointTables";

    /**
     * for conflict resolution screens
     */
    public static final String TABLE_ID = IntentConsts.INTENT_KEY_TABLE_ID;
    /**
     * common for all activities
     */
    public static final String APP_NAME = IntentConsts.INTENT_KEY_APP_NAME;
    /**
     * Tells what time of view it should be
     * displaying.
     */
    public static final String TABLE_DISPLAY_VIEW_TYPE = "tableDisplayViewType";
    public static final String FILE_NAME = "filename";
    public static final String ROW_ID = "rowId";
    /**
     * The name of the graph view that should be displayed.
     */
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
     * A JSON serialization of an array of objects for restricting the rows displayed in the table.
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

  private WeakReference<IOdkWebView> mWebView;
  private IOdkDataActivity mActivity;

  private static final String TAG = OdkData.class.getSimpleName();

  private ExecutorContext context;

  public OdkData(IOdkDataActivity activity, IOdkWebView webView) {
    mActivity = activity;
    mWebView = new WeakReference<IOdkWebView>(webView);
    // change to support multiple data objects within a single webpage
    context = ExecutorContext.getContext(mActivity);
  }

  public boolean isInactive() {
    return (mWebView.get() == null) || (mWebView.get().isInactive());
  }

  public synchronized void refreshContext() {
    if (!context.isAlive()) {
      context = ExecutorContext.getContext(mActivity);
    }
  }

  public synchronized void shutdownContext() {
    context.shutdownWorker();
  }

  private void logDebug(String loggingString) {
    WebLogger.getLogger(this.mActivity.getAppName()).d("odkData", loggingString);
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
    return mActivity.getResponseJSON(getFragmentID());
  }

  /**
   * Get the data for the view once the user is ready for it.
   * When the user chooses to launch a detail, list, or map view
   * they will have to call this via the JS API with success and
   * failure callback functions to manipulate the data for their views
   */
  public void getViewData(String callbackJSON, Integer limit, Integer offset) {
    logDebug("getViewData");

    ViewDataQueryParams queryParams = this.mActivity.getViewQueryParams(getFragmentID());

    if (queryParams == null) {
      return;
    }

    ExecutorRequest request;
    if (queryParams.isSingleRowQuery()) {
      BindArgs bindArgs = new BindArgs(new Object[] { queryParams.rowId });
      request = new ExecutorRequest(queryParams.tableId, DataTableColumns.ID + "=?", bindArgs,
          null, null, DataTableColumns.SAVEPOINT_TIMESTAMP, descOrder, limit, offset, true,
          callbackJSON, getFragmentID());
    } else {
      request = new ExecutorRequest(queryParams.tableId, queryParams.whereClause,
          queryParams.selectionArgs,
          queryParams.groupBy, queryParams.having, queryParams.orderByElemKey,
          queryParams.orderByDir, limit, offset, true, callbackJSON, getFragmentID());
    }
    queueRequest(request);
  }

  private String getFragmentID() {
    IOdkWebView webView = mWebView.get();
    if (webView == null) {
      // should not occur unless we are tearing down the view
      WebLogger.getLogger(mActivity.getAppName()).w(TAG, "null webView");
      return null;
    }

    return webView.getContainerFragmentID();
  }

  /**
   * Get all the roles granted to this user by the server.
   *
   * @param callbackJSON
   */
  public void getRoles(String callbackJSON) {
    logDebug("getRoles");
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.GET_ROLES_LIST,
        callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Get the default group of the current user as assigned to this user by the server.
   *
   * @param callbackJSON
   */
  public void getDefaultGroup(String callbackJSON) {
    logDebug("getDefaultGroup");
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.GET_DEFAULT_GROUP,
        callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Get all the users on the server and their roles.
   *
   * @param callbackJSON
   */
  public void getUsers(String callbackJSON) {
    logDebug("getUsers");
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.GET_USERS_LIST,
        callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Get all the tableIds in the system.
   *
   * @param callbackJSON
   */
  public void getAllTableIds(String callbackJSON) {
    logDebug("getAllTableIds");
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.GET_ALL_TABLE_IDS,
        callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Query the database using sql.
   *
   * @param tableId                 The table being queried. This is a user-defined table.
   * @param whereClause             The where clause for the query
   * @param sqlBindParamsJSON JSON.stringify of array of bind parameter values (including any in
   *                          the having clause)
   * @param groupBy                 The array of columns to group by
   * @param having                  The having clause
   * @param orderByElementKey       The column to order by
   * @param orderByDirection        'ASC' or 'DESC' ordering
   * @param limit         The maximum number of rows to return (null returns all)
   * @param offset        The offset into the result set of the first row to return (null ok)
   * @param includeKeyValueStoreMap true if the keyValueStoreMap should be returned
   * @param callbackJSON            The JSON object used by the JS layer to recover the callback function
   *                                that can process the response
   */
  public void query(String tableId, String whereClause, String sqlBindParamsJSON, String[] groupBy,
      String having, String orderByElementKey, String orderByDirection,
      Integer limit, Integer offset, boolean includeKeyValueStoreMap, String callbackJSON) {
    logDebug("query: " + tableId + " whereClause: " + whereClause);
    BindArgs bindArgs = new BindArgs(sqlBindParamsJSON);
    ExecutorRequest request = new ExecutorRequest(tableId, whereClause, bindArgs, groupBy,
        having, orderByElementKey, orderByDirection, limit, offset, includeKeyValueStoreMap,
        callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Arbitrary SQL query
   *
   * @param tableId       The tableId whose metadata should be returned. If a result
   *                      column matches the column name in this tableId, then the data
   *                      type interpretations for that column will be applied to the result
   *                      column (e.g., integer, number, array, object conversions).
   * @param sqlCommand    The Select statement to issue. It can reference any table in the database,
   *                      including system tables.
   * @param sqlBindParamsJSON JSON.stringify of array of bind parameter values (including any in
   *                          the having clause)
   * @param limit         The maximum number of rows to return (null returns all)
   * @param offset        The offset into the result set of the first row to return (null ok)
   * @param callbackJSON  The JSON object used by the JS layer to recover the callback function
   *                      that can process the response
   * @return see description in class header
   */
  public void arbitraryQuery(String tableId, String sqlCommand, String sqlBindParamsJSON,
      Integer limit, Integer offset, String callbackJSON) {
    logDebug("arbitraryQuery: " + tableId + " sqlCommand: " + sqlCommand);
    BindArgs bindArgs = new BindArgs(sqlBindParamsJSON);
    ExecutorRequest request = new ExecutorRequest(tableId, sqlCommand, bindArgs,
        limit, offset, callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Get all rows that match the given rowId.
   * This can be zero, one or more. It is more than one if there
   * is a sync conflict or if there are edit checkpoints.
   *
   * @param tableId      The table being updated
   * @param rowId        The rowId of the row being added.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  public void getRows(String tableId, String rowId, String callbackJSON) {
    logDebug("getRows: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_GET_ROWS, tableId,
        null, rowId, callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Get the most recent checkpoint or last state for a row in the table.
   * Throws an exception if this row is in conflict.
   * Returns an empty rowset if the rowId is not present in the table.
   *
   * @param tableId      The table being updated
   * @param rowId        The rowId of the row being added.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  public void getMostRecentRow(String tableId, String rowId, String callbackJSON) {
    logDebug("getMostRecentRow: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(
        ExecutorRequestType.USER_TABLE_GET_MOST_RECENT_ROW, tableId, null, rowId, callbackJSON,
        getFragmentID());

    queueRequest(request);
  }

  /**
   * Update a row in the table
   *
   * @param tableId         The table being updated
   * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
   * @param rowId           The rowId of the row being changed.
   * @param callbackJSON    The JSON object used by the JS layer to recover the callback function
   *                        that can process the response
   * @return see description in class header
   */
  public void updateRow(String tableId, String stringifiedJSON, String rowId, String
      callbackJSON) {
    logDebug("updateRow: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_UPDATE_ROW,
        tableId, stringifiedJSON, rowId, callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Update a row in the table with the given filter type and value.
   *
   * @param tableId
   * @param defaultAccess
   * @param owner
   * @param rowId
   * @param callbackJSON
    */
  public void changeAccessFilterOfRow(String tableId, String defaultAccess, String
      owner, String groupReadOnly, String groupModify, String groupPrivileged, String
      rowId, String callbackJSON) {

    logDebug("changeAccessFilter: " + tableId + " _id: " + rowId);
    HashMap<String,String> valueMap = new HashMap<String,String>();
    valueMap.put(DataTableColumns.DEFAULT_ACCESS, defaultAccess);
    valueMap.put(DataTableColumns.ROW_OWNER, owner);
    valueMap.put(DataTableColumns.GROUP_READ_ONLY, groupReadOnly);
    valueMap.put(DataTableColumns.GROUP_MODIFY, groupModify);
    valueMap.put(DataTableColumns.GROUP_PRIVILEGED, groupPrivileged);

    String stringifiedJSON = null;
    try {
      stringifiedJSON = ODKFileUtils.mapper.writeValueAsString(valueMap);
    } catch (JsonProcessingException e) {
      WebLogger.getLogger(mActivity.getAppName()).printStackTrace(e);
      return;
    }
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType
        .USER_TABLE_CHANGE_ACCESS_FILTER_ROW,
        tableId, stringifiedJSON, rowId, callbackJSON, getFragmentID());

    queueRequest(request);

  }
  /**
   * Delete a row from the table
   *
   * @param tableId         The table being updated
   * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
   * @param rowId           The rowId of the row being deleted.
   * @param callbackJSON    The JSON object used by the JS layer to recover the callback function
   *                        that can process the response
   */
  public void deleteRow(String tableId, String stringifiedJSON, String rowId, String callbackJSON) {
    logDebug("deleteRow: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_DELETE_ROW,
        tableId, stringifiedJSON, rowId, callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Add a row in the table
   *
   * @param tableId         The table being updated
   * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
   * @param rowId           The rowId of the row being added.
   * @param callbackJSON    The JSON object used by the JS layer to recover the callback function
   *                        that can process the response
   */
  public void addRow(String tableId, String stringifiedJSON, String rowId, String callbackJSON) {
    logDebug("addRow: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_ADD_ROW, tableId,
        stringifiedJSON, rowId, callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Update the row, marking the updates as a checkpoint save.
   *
   * @param tableId         The table being updated
   * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
   * @param rowId           The rowId of the row being added.
   * @param callbackJSON    The JSON object used by the JS layer to recover the callback function
   *                        that can process the response
   */
  public void addCheckpoint(String tableId, String stringifiedJSON, String rowId,
      String callbackJSON) {
    logDebug("addCheckpoint: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(ExecutorRequestType.USER_TABLE_ADD_CHECKPOINT,
        tableId, stringifiedJSON, rowId, callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Save checkpoint as incomplete. In the process, it applies any changes indicated by the stringifiedJSON.
   *
   * @param tableId         The table being updated
   * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
   * @param rowId           The rowId of the row being saved-as-incomplete.
   * @param callbackJSON    The JSON object used by the JS layer to recover the callback function
   *                        that can process the response
   */
  public void saveCheckpointAsIncomplete(String tableId, String stringifiedJSON, String rowId,
      String callbackJSON) {
    logDebug("saveCheckpointAsIncomplete: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(
        ExecutorRequestType.USER_TABLE_SAVE_CHECKPOINT_AS_INCOMPLETE, tableId, stringifiedJSON,
        rowId, callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Save checkpoint as complete.
   *
   * @param tableId         The table being updated
   * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
   * @param rowId           The rowId of the row being marked-as-complete.
   * @param callbackJSON    The JSON object used by the JS layer to recover the callback function
   *                        that can process the response
   */
  public void saveCheckpointAsComplete(String tableId, String stringifiedJSON, String rowId,
      String callbackJSON) {
    logDebug("saveCheckpointAsComplete: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(
        ExecutorRequestType.USER_TABLE_SAVE_CHECKPOINT_AS_COMPLETE, tableId, stringifiedJSON, rowId,
        callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Delete all checkpoint.  Checkpoints accumulate; this removes all of them.
   *
   * @param tableId      The table being updated
   * @param rowId        The rowId of the row being saved-as-incomplete.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  public void deleteAllCheckpoints(String tableId, String rowId, String callbackJSON) {
    logDebug("deleteAllCheckpoints: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(
        ExecutorRequestType.USER_TABLE_DELETE_ALL_CHECKPOINTS, tableId, null, rowId,
        callbackJSON, getFragmentID());

    queueRequest(request);
  }

  /**
   * Delete last checkpoint.  Checkpoints accumulate; this removes the most recent one, leaving earlier ones.
   *
   * @param tableId      The table being updated
   * @param rowId        The rowId of the row being saved-as-incomplete.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  public void deleteLastCheckpoint(String tableId, String rowId, String callbackJSON) {
    logDebug("deleteLastCheckpoint: " + tableId + " _id: " + rowId);
    ExecutorRequest request = new ExecutorRequest(
        ExecutorRequestType.USER_TABLE_DELETE_LAST_CHECKPOINT, tableId, null, rowId,
        callbackJSON, getFragmentID());

    queueRequest(request);
  }

}
