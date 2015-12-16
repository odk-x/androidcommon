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

import android.os.RemoteException;

import java.lang.ref.WeakReference;

/**
 * @author mitchellsundt@gmail.com
 *
 * Database interface from JS to Java.
 *
 * General notes on APIs:
 *
 * callbackJSON - this is a JSON serialization of a description that the caller can use to recreate
 * or retrieve the callback that will handle the response.
 *
 * COMMON RETURN VALUE:
 *
 * All of these functions asynchronously return a stringifiedJSON object.
 *
 * <pre>
 *     {
 *         callbackJSON: callbackJSON-that-was-passed-in,
 *         errorMsg: "message if there was an error",
 *         data: [ [ row1elementKey1, row1elementKey2, ... ], [row2elementKey1, row2elementKey1, ...], ...]
 *         metadata: {
 *             rowIdMap: { 'rowId' : outerIdx, ...},
 *             elementKeyMap: { 'elementKey': innerIdx, ... },
 *             ...
 *         }
 *     }
 * </pre>
 *
 * If there was an error, the errorMsg field will be present.
 *
 * If the request was a deleteRow or deleteLastCheckpoint, then the data and metadata
 * objects will be omitted. Otherwise, the data and metadta objects will be present.
 *
 * The data object is an array of data rows, which are themselves hetrogeneous row field arrays.
 *
 * The metadata object contains:
 * <ul><li>rowIdMap -- mapping rowId to an index in the data array</li>
 * <li>elementKeyMap -- mapping elementKey to an index in the row field array</li>
 * <li>orderedColumns -- if not rawQuery; the ordered column defns for this tableId,
 * This is a map of elementName to the extended JS schema struct for that element.</li>
 * <li>keyValueStoreList -- if not rawQuery; optional KeyValueStore content for this tableId.
 * This is a list of objects [ { dimension: , aspect: , key:, type:, value:}, ...] with integer, boolean and
 * number types converted to JS representation. Arrays and objects are left as strings for JS decoding.</li>
 * <li>other tool-specific optional fields such as computed row and column colors or extended content</li></ul>
 *
 * The rawQuery result just has the rowIdMap and elementKeyMap.
 */
public class OdkDataIf {

    public static final String TAG = "OdkDataIf";

    private WeakReference<OdkData> weakData;

    OdkDataIf(OdkData odkData) {
        weakData = new WeakReference<OdkData>(odkData);
    }

   /**
    * Get the data for the view once the user is ready for it.
    * When the user chooses to launch a detail, list, or map view
    * they will have to call this via the JS API with success and
    * failure callback functions to manipulate the data for their views
    *
    */
  @android.webkit.JavascriptInterface
  public void getViewData(String callbackJSON) {
     weakData.get().getViewData(callbackJSON);
  }

   /**
    * Access the result of a request
    *
    * @return null if there is no result, otherwise the responseJSON of the last action
    */
   @android.webkit.JavascriptInterface
   public String getResponseJSON() {
      return weakData.get().getResponseJSON();
   }

    /**
     * Query the database using sql.
     *
     * @param tableId  The table being queried. This is a user-defined table.
     * @param whereClause The where clause for the query
     * @param sqlBindParams The array of bind parameter values (including any in the having clause)
     * @param groupBy The array of columns to group by
     * @param having The having clause
     * @param orderByElementKey The column to order by
     * @param orderByDirection 'ASC' or 'DESC' ordering
     * @param includeKeyValueStoreMap true if the keyValueStoreMap should be returned
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
    */
    @android.webkit.JavascriptInterface
    public void query(String tableId, String whereClause, String[] sqlBindParams,
                      String[] groupBy, String having, String orderByElementKey, String orderByDirection,
                      boolean includeKeyValueStoreMap,
                      String callbackJSON)
            throws RemoteException {
        weakData.get().query(tableId, whereClause, sqlBindParams, groupBy, having,
            orderByElementKey, orderByDirection, includeKeyValueStoreMap, callbackJSON);
    }


    /**
     * Raw SQL query
     *
     * @param sqlCommand The Select statement to issue. It can reference any table in the database, including system tables.
     * @param sqlBindParams The array of bind parameter values (including any in the having clause)
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    @android.webkit.JavascriptInterface
    public void rawQuery(String sqlCommand, String[] sqlBindParams,
                           String callbackJSON)
            throws RemoteException {
        weakData.get().rawQuery(sqlCommand, sqlBindParams, callbackJSON);
    }

  /**
   * Get all rows that match the given rowId.
   * This can be zero, one or more. It is more than one if there
   * is a sync conflict or if there are edit checkpoints.
   *
   * @param tableId  The table being updated
   * @param rowId The rowId of the row being changed.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  @android.webkit.JavascriptInterface
  public void getRows(String tableId, String rowId, String callbackJSON)
      throws RemoteException {
    weakData.get().getRows(tableId, rowId, callbackJSON);
  }


  /**
     * Get the most recent checkpoint or data for a row in the table
     * Throws an exception if the row is in conflict.
     *
     * @param tableId  The table being updated
     * @param rowId The rowId of the row being changed.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    @android.webkit.JavascriptInterface
    public void getMostRecentRow(String tableId, String rowId, String callbackJSON)
      throws RemoteException {
         weakData.get().getMostRecentRow(tableId, rowId, callbackJSON);
    }


    /**
     * Update a row in the table
     *
     * @param tableId  The table being updated
     * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
     * @param rowId The rowId of the row being changed.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    @android.webkit.JavascriptInterface
    public void updateRow(String tableId, String stringifiedJSON, String rowId,
                            String callbackJSON)
            throws RemoteException {
        weakData.get().updateRow(tableId, stringifiedJSON, rowId, callbackJSON);
    }


    /**
     * Delete a row from the table
     *
     * @param tableId  The table being updated
     * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
     * @param rowId The rowId of the row being deleted.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    @android.webkit.JavascriptInterface
    public void deleteRow(String tableId, String stringifiedJSON, String rowId,
                            String callbackJSON)
            throws RemoteException {
        weakData.get().deleteRow(tableId, stringifiedJSON, rowId, callbackJSON);
    }


    /**
     * Add a row in the table
     *
     * @param tableId  The table being updated
     * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
     * @param rowId The rowId of the row being added.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    @android.webkit.JavascriptInterface
    public void addRow(String tableId, String stringifiedJSON, String rowId,
                       String callbackJSON)
            throws RemoteException {
        weakData.get().addRow(tableId, stringifiedJSON, rowId, callbackJSON);
    }



    /**
     * Update the row, marking the updates as a checkpoint save.
     *
     * @param tableId  The table being updated
     * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
     * @param rowId The rowId of the row being added.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    @android.webkit.JavascriptInterface
    public void addCheckpoint (String tableId, String stringifiedJSON, String rowId,
                                 String callbackJSON)
            throws RemoteException {
        weakData.get().addCheckpoint(tableId, stringifiedJSON, rowId, callbackJSON);
    }


    /**
     * Save checkpoint as incomplete. In the process, it applies any changes indicated by the stringifiedJSON.
     *
     * @param tableId  The table being updated
     * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
     * @param rowId The rowId of the row being saved-as-incomplete.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    @android.webkit.JavascriptInterface
    public void saveCheckpointAsIncomplete (String tableId, String stringifiedJSON, String rowId,
                                              String callbackJSON)
            throws RemoteException {
        weakData.get().saveCheckpointAsIncomplete(tableId, stringifiedJSON, rowId, callbackJSON);
    }


    /**
     * Save checkpoint as complete. In the process, it applies any changes indicated by the stringifiedJSON.
     *
     * @param tableId  The table being updated
     * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
     * @param rowId The rowId of the row being marked-as-complete.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    @android.webkit.JavascriptInterface
    public void saveCheckpointAsComplete (String tableId, String stringifiedJSON, String rowId,
                                            String callbackJSON)
            throws RemoteException {
        weakData.get().saveCheckpointAsComplete(tableId, stringifiedJSON, rowId, callbackJSON);
    }


  /**
   * Delete all checkpoints.  Checkpoints accumulate; this removes all of them.
   *
   * @param tableId  The table being updated
   * @param rowId The rowId of the row being saved-as-incomplete.
   * @param callbackJSON The JSON object used by the JS layer to recover the callback function
   *                     that can process the response
   */
  @android.webkit.JavascriptInterface
  public void deleteAllCheckpoints (String tableId, String rowId,
      String callbackJSON)
      throws RemoteException {
    weakData.get().deleteAllCheckpoints(tableId, rowId, callbackJSON);
  }

    /**
     * Delete last checkpoint.  Checkpoints accumulate; this removes the most recent one, leaving earlier ones.
     *
     * @param tableId  The table being updated
     * @param rowId The rowId of the row being saved-as-incomplete.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    @android.webkit.JavascriptInterface
    public void deleteLastCheckpoint (String tableId, String rowId,
                                              String callbackJSON)
            throws RemoteException {
        weakData.get().deleteLastCheckpoint(tableId, rowId, callbackJSON);
    }
}
