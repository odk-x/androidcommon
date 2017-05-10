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

/**
 * @author mitchellsundt@gmail.com
 */
public class ExecutorRequest {


    public final ExecutorRequestType executorRequestType;

    // To clear an older context
    public final ExecutorContext oldContext;

    // For raw query interaction
    public final String sqlCommand;

    // shared between raw query and user table query interactions
    public final Object[] sqlBindParams;

    // For user table interactions
    public final String tableId;

    // For user table query interaction
    public final String whereClause;
    public final String[] groupBy;
    public final String having;
    public final String orderByElementKey;
    public final String orderByDirection;
    public final Integer limit;
    public final Integer offset;
    public final boolean includeKeyValueStoreMap;

    // For user table modification interactions
    public final String stringifiedJSON;
    public final String rowId;

    // For checkpoint delete interations
    public final boolean deleteAllCheckpoints;

    // For commit interaction
    public final boolean commitTransaction;

    // For most interactions
    public final String callbackJSON;

    // Find its way back to the correct caller
    public final String callerID;

    public ExecutorRequest(ExecutorContext oldContext) {
        this.executorRequestType = ExecutorRequestType.UPDATE_EXECUTOR_CONTEXT;
        this.oldContext = oldContext;

        // unused:
        this.sqlCommand = null;
        this.sqlBindParams = null;
        this.callbackJSON = null;
        this.tableId = null;
        this.whereClause = null;
        this.groupBy = null;
        this.having = null;
        this.orderByElementKey = null;
        this.orderByDirection = null;
        this.limit = null;
        this.offset = null;
        this.includeKeyValueStoreMap = false;
        this.stringifiedJSON = null;
        this.rowId = null;
        this.deleteAllCheckpoints = false;
        this.commitTransaction = false;
        this.callerID = null;
    }

    /**
     * Arbitrary SQL query
     *
     * @param tableId              The tableId whose metadata should be returned. If a result
     *                             column matches the column name in this tableId, then the data
     *                             type interpretations for that column will be applied to the result
     *                             column (e.g., integer, number, array, object conversions).
     * @param sqlCommand The Select statement to issue. It can reference any table in the database, including system tables.
     * @param sqlBindParams The array of bind parameter values (including any in the having clause)
     * @param limit null to return everything. Otherwise, max number or rows to return
     * @param offset if limit is not null, specify the offset into the result set to return.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    public ExecutorRequest(String tableId, String sqlCommand, Object[] sqlBindParams,
                Integer limit, Integer offset, String callbackJSON, String callerID) {
        this.executorRequestType = ExecutorRequestType.ARBITRARY_QUERY;
        this.tableId = tableId;
        this.sqlCommand = sqlCommand;
        this.sqlBindParams = sqlBindParams;
        this.limit = limit;
        this.offset = offset;
        this.callbackJSON = callbackJSON;
        this.callerID = callerID;

        // unused:
        this.oldContext = null;
        this.whereClause = null;
        this.groupBy = null;
        this.having = null;
        this.orderByElementKey = null;
        this.orderByDirection = null;
        this.includeKeyValueStoreMap = false;
        this.stringifiedJSON = null;
        this.rowId = null;
        this.deleteAllCheckpoints = false;
        this.commitTransaction = false;
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
     * @param limit null to return everything. Otherwise, max number or rows to return
     * @param offset if limit is not null, specify the offset into the result set to return.
     * @param includeKeyValueStoreMap true if the keyValueStoreMap should be returned
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    public ExecutorRequest(String tableId, String whereClause, Object[] sqlBindParams,
                           String[] groupBy, String having, String orderByElementKey, String orderByDirection,
                           Integer limit, Integer offset, boolean includeKeyValueStoreMap,
                           String callbackJSON, String callerID) {
        this.executorRequestType = ExecutorRequestType.USER_TABLE_QUERY;
        this. tableId = tableId;
        this.whereClause = whereClause;
        this.sqlBindParams = sqlBindParams;
        this.groupBy = groupBy;
        this.having = having;
        this.orderByElementKey = orderByElementKey;
        this.orderByDirection = orderByDirection;
        this.limit = limit;
        this.offset = offset;
        this.includeKeyValueStoreMap = includeKeyValueStoreMap;
        this.callbackJSON = callbackJSON;
        this.callerID = callerID;

        // unused:
        this.oldContext = null;
        this.sqlCommand = null;
        this.stringifiedJSON = null;
        this.rowId = null;
        this.deleteAllCheckpoints = false;
        this.commitTransaction = false;
    }

    /**
     * Add or modify a row in the table, or save-as-incomplete or save-as-complete
     *
     * @param executorRequestType The type of request. One of:
     *                    <ul><li>USER_TABLE_UPDATE_ROW</li>
     *                    <li>USER_TABLE_DELETE_ROW</li>
     *                    <li>USER_TABLE_GET_MOST_RECENT_ROW</li>
     *                    <li>USER_TABLE_ADD_ROW</li>
     *                    <li>USER_TABLE_ADD_CHECKPOINT</li>
     *                    <li>USER_TABLE_SAVE_CHECKPOINT_AS_INCOMPLETE</li>
     *                    <li>USER_TABLE_SAVE_CHECKPOINT_AS_COMPLETE</li>
     *                    <li>USER_TABLE_DELETE_ALL_CHECKPOINTS</li>
     *                    <li>USER_TABLE_DELETE_LAST_CHECKPOINT</li></ul>
     * @param tableId  The table being updated
     * @param stringifiedJSON key-value map of values to store or update. If missing, the value remains unchanged.
     *                        This field is ignored when performing
     *                        USER_TABLE_DELETE_LAST_CHECKPOINT or USER_TABLE_GET_MOST_RECENT_ROW
     * @param rowId The rowId of the row being deleted.
     * @param callbackJSON The JSON object used by the JS layer to recover the callback function
     *                     that can process the response
     */
    public ExecutorRequest(ExecutorRequestType executorRequestType, String tableId, String stringifiedJSON, String rowId,
        String callbackJSON, String callerID) {
        this.executorRequestType = executorRequestType;
        this.tableId = tableId;
        this.stringifiedJSON = stringifiedJSON;
        this.rowId = rowId;
        this.callbackJSON = callbackJSON;
        this.callerID = callerID;

        // unused:
        this.oldContext = null;
        this.sqlCommand = null;
        this.whereClause = null;
        this.sqlBindParams = null;
        this.groupBy = null;
        this.having = null;
        this.orderByElementKey = null;
        this.orderByDirection = null;
        this.limit = null;
        this.offset = null;
        this.includeKeyValueStoreMap = false;
        this.deleteAllCheckpoints = false;
        this.commitTransaction = false;
    }

    public ExecutorRequest(ExecutorRequestType executorRequestType, String callbackJSON,
        String callerID) {
        this.executorRequestType = executorRequestType;
        this.tableId = null;
        this.stringifiedJSON = null;
        this.rowId = null;
        this.callbackJSON = callbackJSON;
        this.callerID = callerID;

        // unused:
        this.oldContext = null;
        this.sqlCommand = null;
        this.whereClause = null;
        this.sqlBindParams = null;
        this.groupBy = null;
        this.having = null;
        this.orderByElementKey = null;
        this.orderByDirection = null;
        this.limit = null;
        this.offset = null;
        this.includeKeyValueStoreMap = false;
        this.deleteAllCheckpoints = false;
        this.commitTransaction = false;
    }
}
