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

import java.lang.ref.WeakReference;

/**
 * @author mitchellsundt@gmail.com
 *         <p>
 *         Database interface from JS to Java.
 *         </p><p>
 *         General notes on APIs:
 *         </p><p>
 *         callbackJSON - this is a JSON serialization of a description that the caller can use to recreate
 *         or retrieve the callback that will handle the response.
 *         </p><h2>
 *         COMMON RETURN VALUE:
 *         </h2><p>
 *         All of these functions asynchronously return a stringifiedJSON object.
 *         </p><pre>
 *             {
 *                 callbackJSON: callbackJSON-that-was-passed-in,
 *                 errorMsg: "message if there was an error",
 *                 data: [ [ row1elementValue1, row1elementValue2, ... ],
 *                         [row2elementValue1,  row2elementValue2, ...], ...]
 *                 metadata: {
 *                     tableId: tableId,
 *                     schemaETag: schemaETag for this table,
 *                     lastDataETag: the server's dataETag for the last successful
 *                                   row sync of the table,
 *                     lastSyncTime: the timestamp of the last successful row sync,
 *                     elementKeyMap: { 'elementKey': innerIdx, ... },
 *                     dataTableModel: dataTableModel of table -- identical to that in a formDef
 *                                    specification except there are no session variables since
 *                                    this is a reconstruction from the database content.
 *                     keyValueStoreList: [ { partition: pv,
 *                                            aspect: av,
 *                                            key: kv,
 *                                            type: tv,
 *                                            value: vv }, ... ]
 *                     ...
 *                     // other tool-specific fields. E.g., for ODK Tables, the custom
 *                     // color values for the rows and columns.
 *                 }
 *             }
 *         </pre><p>
 *         If there was an error, the errorMsg field will be present. If an error occured,
 *         the data and metadata will generally not be present.
 *         </p><p>
 *         The data object is an array of data rows, which are themselves hetrogeneous
 *         field-value arrays containing the values for all the fields (data and instance metadata)
 *         in a row.
 *         </p><p>
 *         The order of the fields in the field-value arrays is specified in the
 *         </p><pre>
 *                    metadata.elementKeyMap
 *         </pre><p>
 *         If the request was a deleteRow or deleteAllCheckpoints, the data array can either
 *         be empty or can contain a row with sync_state = 'deleted' to indicate that the deletion
 *         needs to be sync'd with the server.
 *         </p><p>
 *         For all other row modification requests, the returned data array will contain the
 *         most recent data field-values for that row.  I.e., the database entry where
 *         </p><pre>
 *                     _savepoint_timestamp = max(_savepoint_timestamp)
 *         </pre><p>
 *         The metadata object fields are populated if the odkDataIf action specified a tableId.
 *         </p><h3>
 *         INTERPRETATION of stringifiedJSON arguments to the odkDataIf APIs
 *         </h3><p>
 *         The stringifiedJSON argument is a JSON serialization of an elementKey-to-value map.
 *         As such, all keys in these maps are required to be the elementKeys (database column
 *         names) of the underlying table; any complex object must be resolved to the separate
 *         values that are stored into individual columns in the database table before invoking
 *         these APIs.  The values are "marshalled representations" of the content of the
 *         fields, as described in the following section.</p><p>
 *         The dataTableModel can be used to resolve arbitrary portions of a complex Javascript
 *         object into their underlying elementKey units. I.e.,
 *         if you update a geopoint, it is expected that you would specify an stringifiedJSON
 *         input string that was a JSON serialization of an update map with the 4 entries
 *         for the latitude, longitude, altitude and accuracy of the geopoint you are updating
 *         since those will be mapped to separate columns in the database table. It is
 *         incorrect to pass an update map with a single entry corresponding to the composite
 *         value for the complex 'geopoint' object.</p><p>
 *         Similarly, the data rows returned by the odkDataIf API will return the values for the
 *         individual elementKeys (database column names). You can use the dataTableModel to
 *         reconstruct the complex Javascript object from those values.
 *         </p><h3>
 *           DATA ROW AND KEY-VALUE-STORE VALUE REPRESENTATION
 *         </h3><p>
 *         All vv key-value-store field values are transmitted across the interfaces in a
 *         "marshalled representation" as follows: boolean, integer, number, rowpath, configpath
 *         and string values are transmitted as their native values (i.e., booleans are
 *         true/false, integers are 1, 2, ..., numbers are decimal numbers). Arrays and objects
 *         (the only other allowed value types) are transmitted as the JSON stringify of
 *         their "marshalled representation" described below.</p><p>
 *         All rowNelementValueM field values are transmitted across the
 *         interfaces in a "marshalled representation" as follows: boolean, integer,
 *         number, rowpath, configpath and string values are transmitted as their native values
 *         (i.e., booleans are true/false, integers are 1, 2,..., numbers are decimal numbers).
 *         All other values (arrays, user defined types and objects) are marshalled. Arrays and
 *         objects are then JSON stringify'd before transitting the interface.</p><p>
 *         The "marshalled representation" for an array is the array after traversing all
 *         elements of the array and generating their "marshalled representation"s.</p><p>
 *         User-defined types are either user-defined object types or primitive types. If the
 *         list of children is empty, a user-defined type is assumed to be a string unless there
 *         is an explicit data type. rowpath, configpath, date, dateTime and time could be viewed
 *         as predefined user-defined string types; these are all named types with no explicit data
 *         type and no children, so they default to be string data.</p><p>
 *         To explicitly declare a data type for an element type, users may specify a different type
 *         by following the user-named object type with a colon and a primitive data type
 *         (integer, number, string). e.g.,<pre>
 *                       elementType = "feet:number"
 *         </pre><p>
 *         Would define a 'feet' object type that is stored as a number, rather than a string.
 *         Additionally, there are optional data type attributes that can be specified within
 *         parentheses at the end of the element type. This would be how you would specify a
 *         string field storing 1024 characters (storage length specifications are only relevant
 *         on the server):<pre>
 *                       elementType = "myfieldtype:string(1024)"
 *         <em>or, because string is implied:</em>
 *                       elementType = "myfieldType(1024)"
 *         </pre><p>
 *         It is an error to attempt to declare a native data type when the field also has children.
 *         If there are children, the object type is expected to be 'object' but if there is only
 *         one child, it can alternatively be specified as 'array' (e.g., "workItems:array"), and
 *         the child element then defines the data type stored in that array. You can define
 *         fields that store arrays of integers or booleans through this mechanism.
 *         </p>
 *         When needed, the "marshalled representation" of an object is as a Javascript
 *         object, with key-value pairs, with the keys being the element names within the object
 *         definition and the values being the "marshalled representation" of the data
 *         values that those fields contain contain.
 *         </p><p>
 *         User code (either in an ODK Survey prompt or in and ODK Tables webpage) is expected to
 *         parse the content and act accordingly.</p><p>
 *         The supplied date, dateTime, time and timeInterval data types are represented as
 *         string values in the data table. If these are manipulated using Date() objects,
 *         the values within those Date() objects need to be converted into a string formatted
 *         as defined by opendatakit before being passed through this interface. Conversion
 *         routines are provided for this by the odkCommon object.</p><p>
 *         The upshot of all of this is that the Javascript layer needs to make sure that any
 *         types are appropriately converted into their "marshalled representations" as described
 *         above.</p>
 */
public class OdkDataIf {

  public static final String TAG = "OdkDataIf";

  private WeakReference<OdkData> weakData;

  OdkDataIf(OdkData odkData) {
    weakData = new WeakReference<OdkData>(odkData);
  }

  private boolean isInactive() {
    return (weakData.get() == null) || (weakData.get().isInactive());
  }

  /**
   * Get the data for the view once the user is ready for it.
   * When the user chooses to launch a detail, list, or map view
   * they will have to call this via the JS API with success and
   * failure callback functions to manipulate the data for their views
   */
  @android.webkit.JavascriptInterface public void getViewData(String callbackJSON, String limit,
      String offset) {
    if (isInactive())
      return;

    Integer integerLimit = (limit != null ? Integer.valueOf(limit) : null);
    Integer integerOffset = (offset != null ? Integer.valueOf(offset) : null);

    weakData.get().getViewData(callbackJSON, integerLimit, integerOffset);
  }

  /**
   * Access the result of a request
   *
   * @return null if there is no result, otherwise the responseJSON of the last action
   */
  @android.webkit.JavascriptInterface public String getResponseJSON() {
    if (isInactive())
      return null;
    return weakData.get().getResponseJSON();
  }

  /**
   * Get all the roles and groups assigned to this user by the server.
   *
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                                that can process the response
   */
  @android.webkit.JavascriptInterface public void getRoles(String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().getRoles(callbackJSON);
  }

  /**
   * Get the default group of the current user as assigned to this user by the server.
   *
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                                that can process the response
   */
  @android.webkit.JavascriptInterface public void getDefaultGroup(String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().getDefaultGroup(callbackJSON);
  }

  /**
   * Get all the users on the server and their assigned roles.
   *
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                                that can process the response
   */
  @android.webkit.JavascriptInterface public void getUsers(String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().getUsers(callbackJSON);
  }

  /**
   * Get all the tableIds in the system.
   *
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                                that can process the response
   */
  @android.webkit.JavascriptInterface public void getAllTableIds(String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().getAllTableIds(callbackJSON);
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
   * @param limit   The maximum number of rows to return (null for unlimited)
   * @param offset  The offset into the result set for the first row to return (null ok)
   * @param includeKeyValueStoreMap true if the keyValueStoreMap should be returned
   * @param callbackJSON            The JSON object used by the JS layer to recover the callback function
   *                                that can process the response
   */
  @android.webkit.JavascriptInterface public void query(String tableId, String whereClause,
      String sqlBindParamsJSON, String[] groupBy, String having, String orderByElementKey,
      String orderByDirection, String limit, String offset, boolean includeKeyValueStoreMap,
      String callbackJSON)
      {
    if (isInactive())
      return;

    Integer integerLimit = (limit != null ? Integer.valueOf(limit) : null);
    Integer integerOffset = (offset != null ? Integer.valueOf(offset) : null);

    weakData.get().query(tableId, whereClause, sqlBindParamsJSON, groupBy, having, orderByElementKey,
        orderByDirection, integerLimit, integerOffset, includeKeyValueStoreMap, callbackJSON);
  }

  /**
   * Arbitrary SQL query
   *
   * @param tableId       The tableId whose metadata should be returned. If a result
   *                      column matches the column name in this tableId, then the data
   *                      type interpretations for that column will be applied to the result
   *                      column (e.g., integer, number, array, object conversions).
   * @param sqlCommand    The Select statement to issue. It can reference any table in the database, including system tables.
   * @param sqlBindParamsJSON JSON.stringify of array of bind parameter values (including any in
   *                          the having clause)
   * @param limit   The maximum number of rows to return (null for unlimited)
   * @param offset  The offset into the result set for the first row to return (null ok)
   * @param callbackJSON  The JSON object used by the JS layer to recover the callback function
   *                      that can process the response
   */
  @android.webkit.JavascriptInterface public void arbitraryQuery(String tableId, String sqlCommand,
      String sqlBindParamsJSON, String limit, String offset, String callbackJSON) {
    if (isInactive())
      return;
    Integer integerLimit = limit != null ? Integer.valueOf(limit) : null;
    Integer integerOffset = offset != null ? Integer.valueOf(offset) : null;

    weakData.get().arbitraryQuery(tableId, sqlCommand, sqlBindParamsJSON, integerLimit, integerOffset, callbackJSON);
  }

  /**
   * Get all rows that match the given rowId.
   * This can be zero, one or more. It is more than one if there
   * is a sync conflict or if there are edit checkpoints.
   *
   * @param tableId      The table being updated
   * @param rowId        The rowId of the row being changed.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  @android.webkit.JavascriptInterface public void getRows(String tableId, String rowId,
      String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().getRows(tableId, rowId, callbackJSON);
  }

  /**
   * Get the most recent checkpoint or data for a row in the table
   * Throws an exception if the row is in conflict.
   *
   * @param tableId      The table being updated
   * @param rowId        The rowId of the row being changed.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  @android.webkit.JavascriptInterface public void getMostRecentRow(String tableId, String rowId,
      String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().getMostRecentRow(tableId, rowId, callbackJSON);
  }

  /**
   * Update a row in the table
   *
   * @param tableId         The table being updated
   * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
   * @param rowId           The rowId of the row being changed.
   * @param callbackJSON    The JSON object used by the JS layer to recover the callback function
   *                        that can process the response
   */
  @android.webkit.JavascriptInterface public void updateRow(String tableId, String stringifiedJSON,
      String rowId, String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().updateRow(tableId, stringifiedJSON, rowId, callbackJSON);
  }

  /**
   * Update a row in the table with the given filter type and value.
   *
   * @param tableId         The table being updated
   * @param defaultAccess      cannot be null. One of DEFAULT, MODIFY, READ_ONLY, HIDDEN
   * @param owner     can be null. userid of owner of the row.
   * @param rowId           The rowId of the row being changed.
   * @param callbackJSON    The JSON object used by the JS layer to recover the callback function
   *                        that can process the response
   */
  @android.webkit.JavascriptInterface public void changeAccessFilterOfRow(String tableId,
      String defaultAccess, String owner, String groupReadOnly, String groupModify, String groupPrivileged,
      String rowId, String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().changeAccessFilterOfRow(tableId, defaultAccess, owner, groupReadOnly, groupModify,
            groupPrivileged, rowId, callbackJSON);
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
  @android.webkit.JavascriptInterface public void deleteRow(String tableId, String stringifiedJSON,
      String rowId, String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().deleteRow(tableId, stringifiedJSON, rowId, callbackJSON);
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
  @android.webkit.JavascriptInterface public void addRow(String tableId, String stringifiedJSON,
      String rowId, String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().addRow(tableId, stringifiedJSON, rowId, callbackJSON);
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
  @android.webkit.JavascriptInterface public void addCheckpoint(String tableId,
      String stringifiedJSON, String rowId, String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().addCheckpoint(tableId, stringifiedJSON, rowId, callbackJSON);
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
  @android.webkit.JavascriptInterface public void saveCheckpointAsIncomplete(String tableId,
      String stringifiedJSON, String rowId, String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().saveCheckpointAsIncomplete(tableId, stringifiedJSON, rowId, callbackJSON);
  }

  /**
   * Save checkpoint as complete. In the process, it applies any changes indicated by the stringifiedJSON.
   *
   * @param tableId         The table being updated
   * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
   * @param rowId           The rowId of the row being marked-as-complete.
   * @param callbackJSON    The JSON object used by the JS layer to recover the callback function
   *                        that can process the response
   */
  @android.webkit.JavascriptInterface public void saveCheckpointAsComplete(String tableId,
      String stringifiedJSON, String rowId, String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().saveCheckpointAsComplete(tableId, stringifiedJSON, rowId, callbackJSON);
  }

  /**
   * Delete all checkpoints.  Checkpoints accumulate; this removes all of them.
   *
   * @param tableId      The table being updated
   * @param rowId        The rowId of the row being saved-as-incomplete.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  @android.webkit.JavascriptInterface public void deleteAllCheckpoints(String tableId, String rowId,
      String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().deleteAllCheckpoints(tableId, rowId, callbackJSON);
  }

  /**
   * Delete last checkpoint.  Checkpoints accumulate; this removes the most recent one, leaving earlier ones.
   *
   * @param tableId      The table being updated
   * @param rowId        The rowId of the row being saved-as-incomplete.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  @android.webkit.JavascriptInterface public void deleteLastCheckpoint(String tableId, String rowId,
      String callbackJSON) {
    if (isInactive())
      return;
    weakData.get().deleteLastCheckpoint(tableId, rowId, callbackJSON);
  }
}
