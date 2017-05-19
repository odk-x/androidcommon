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

import android.content.ContentValues;

import com.fasterxml.jackson.core.type.TypeReference;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TableDefinitionEntry;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.data.utilities.ColumnUtil;
import org.opendatakit.utilities.DataHelper;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.database.service.UserDbInterface;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.data.Row;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.queries.ResumableQuery;
import org.sqlite.database.sqlite.SQLiteException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * @author mitchellsundt@gmail.com
 */
public abstract class ExecutorProcessor implements Runnable {
  private static final String TAG = "ExecutorProcessor";

  // Changed this to protected so that extended
  // ExecutorProcessors can make use of this
  protected static final List<String> ADMIN_COLUMNS;

  static {
    // everything is a STRING except for
    // CONFLICT_TYPE which is an INTEGER
    // see OdkDatabaseImplUtils.getUserDefinedTableCreationStatement()
    ArrayList<String> adminColumns = new ArrayList<String>();
    adminColumns.add(DataTableColumns.ID);
    adminColumns.add(DataTableColumns.ROW_ETAG);
    adminColumns.add(DataTableColumns.SYNC_STATE); // not exportable
    adminColumns.add(DataTableColumns.CONFLICT_TYPE); // not exportable
    adminColumns.add(DataTableColumns.DEFAULT_ACCESS);
    adminColumns.add(DataTableColumns.ROW_OWNER);
    adminColumns.add(DataTableColumns.FORM_ID);
    adminColumns.add(DataTableColumns.LOCALE);
    adminColumns.add(DataTableColumns.SAVEPOINT_TYPE);
    adminColumns.add(DataTableColumns.SAVEPOINT_TIMESTAMP);
    adminColumns.add(DataTableColumns.SAVEPOINT_CREATOR);
    Collections.sort(adminColumns);
    ADMIN_COLUMNS = Collections.unmodifiableList(adminColumns);
  }

  private ExecutorContext context;

  private ExecutorRequest request;
  private UserDbInterface dbInterface;
  private String transId;
  private DbHandle dbHandle;

  protected ExecutorProcessor(ExecutorContext context) {
    this.context = context;
  }

  @Override public void run() {
    this.request = context.peekRequest();
    if (request == null) {
      // no work to do...
      return;
    }

    dbInterface = context.getDatabase();
    if (dbInterface == null) {
      // no database to do the work...
      return;
    }

    try {
      // we have a request and a viable database interface...
      dbHandle = dbInterface.openDatabase(context.getAppName());
      if (dbHandle == null) {
        context.reportError(request.callbackJSON, request.callerID, null,
            IllegalStateException.class.getName() + ": Unable to open database connection");
        context.popRequest(true);
        return;
      }

      transId = UUID.randomUUID().toString();
      context.registerActiveConnection(transId, dbHandle);

      switch (request.executorRequestType) {
      case UPDATE_EXECUTOR_CONTEXT:
        updateExecutorContext();
        break;
      case GET_ROLES_LIST:
        getRolesList();
        break;
      case GET_DEFAULT_GROUP:
        getDefaultGroup();
        break;
      case GET_USERS_LIST:
        getUsersList();
        break;
      case GET_ALL_TABLE_IDS:
        getAllTableIds();
        break;
      case ARBITRARY_QUERY:
        arbitraryQuery();
        break;
      case USER_TABLE_QUERY:
        userTableQuery();
        break;
      case USER_TABLE_GET_ROWS:
        getRows();
        break;
      case USER_TABLE_GET_MOST_RECENT_ROW:
        getMostRecentRow();
        break;
      case USER_TABLE_UPDATE_ROW:
        updateRow();
        break;
      case USER_TABLE_CHANGE_ACCESS_FILTER_ROW:
        changeAccessFilterRow();
        break;
      case USER_TABLE_DELETE_ROW:
        deleteRow();
        break;
      case USER_TABLE_ADD_ROW:
        addRow();
        break;
      case USER_TABLE_ADD_CHECKPOINT:
        addCheckpoint();
        break;
      case USER_TABLE_SAVE_CHECKPOINT_AS_INCOMPLETE:
        saveCheckpointAsIncomplete();
        break;
      case USER_TABLE_SAVE_CHECKPOINT_AS_COMPLETE:
        saveCheckpointAsComplete();
        break;
      case USER_TABLE_DELETE_ALL_CHECKPOINTS:
        deleteAllCheckpoints();
        break;
      case USER_TABLE_DELETE_LAST_CHECKPOINT:
        deleteLastCheckpoint();
        break;
      default:
        reportErrorAndCleanUp(IllegalStateException.class.getName() +
            ": ExecutorProcessor has not implemented this request type!");
      }
    } catch (ActionNotAuthorizedException ex) {
      reportErrorAndCleanUp(ActionNotAuthorizedException.class.getName() +
          ": Not Authorized - " + ex.getMessage());
    } catch (ServicesAvailabilityException e) {
      reportErrorAndCleanUp(ServicesAvailabilityException.class.getName() +
          ": " + e.getMessage());
    } catch (SQLiteException e) {
      reportErrorAndCleanUp(SQLiteException.class.getName() +
          ": " + e.getMessage());
    } catch (Throwable t) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() +
          ": ExecutorProcessor unexpected exception " + t.toString());
    }
  }

  /**
   * Handle the open/close transaction treatment for the database and report an error.
   *
   * @param errorMessage
   */
  private void reportErrorAndCleanUp(String errorMessage) {
    try {
      dbInterface.closeDatabase(context.getAppName(), dbHandle);
    } catch (Exception e) {
      // ignore this -- favor first reported error
      WebLogger.getLogger(context.getAppName()).printStackTrace(e);
      WebLogger.getLogger(context.getAppName()).w(TAG, "error while releasing database conneciton");
    } finally {
      context.removeActiveConnection(transId);
      context.reportError(request.callbackJSON, request.callerID, null, errorMessage);
      context.popRequest(true);
    }
  }

  /**
   * Handle the open/close transaction treatment for the database and report a success.
   *
   * @param data
   * @param metadata
   */
  private void reportSuccessAndCleanUp(ArrayList<List<Object>> data, Map<String, Object> metadata) {
    boolean successful = false;
    String exceptionString = null;
    try {
      dbInterface.closeDatabase(context.getAppName(), dbHandle);
      successful = true;
    } catch (ServicesAvailabilityException e) {
      exceptionString = e.getClass().getName() +
        ": error while closing database: " + e.toString();
      WebLogger.getLogger(context.getAppName()).printStackTrace(e);
      WebLogger.getLogger(context.getAppName()).w(TAG, exceptionString);
    } catch (Throwable e) {
      String msg = e.getMessage();
      if ( msg == null ) {
        msg = e.toString();
      }
      exceptionString = IllegalStateException.class.getName() +
        ": unexpected exception " + e.getClass().getName() +
          " while closing database: " + msg;
      WebLogger.getLogger(context.getAppName()).printStackTrace(e);
      WebLogger.getLogger(context.getAppName()).w(TAG, exceptionString);
    } finally {
      context.removeActiveConnection(transId);
      if (successful) {
        context.reportSuccess(request.callbackJSON, request.callerID, null, data, metadata);
      } else {
        context.reportError(request.callbackJSON, request.callerID, null, exceptionString);
      }
      context.popRequest(true);
    }
  }

  /**
   * Assumes incoming stringifiedJSON map only contains integers, doubles, strings, booleans
   * and arrays or string-value maps.
   *
   * @param columns
   * @param stringifiedJSON
   * @return ContentValues object drawn from stringifiedJSON
   */
  private ContentValues convertJSON(OrderedColumns columns, String stringifiedJSON) {
    ContentValues cvValues = new ContentValues();
    if (stringifiedJSON == null) {
      return cvValues;
    }
    try {
      HashMap map = ODKFileUtils.mapper.readValue(stringifiedJSON, HashMap.class);
      // populate cvValues from the map...
      for (Object okey : map.keySet()) {
        String key = (String) okey;
        // the only 3 metadata fields that the user should update are formId, locale, and creator
        // and administrators or super-users can modify the filter type and filter value
        if ( !key.equals(DataTableColumns.FORM_ID) &&
             !key.equals(DataTableColumns.LOCALE) &&
             !key.equals(DataTableColumns.SAVEPOINT_CREATOR) &&
             !key.equals(DataTableColumns.DEFAULT_ACCESS) &&
             !key.equals(DataTableColumns.ROW_OWNER) &&
             !key.equals(DataTableColumns.GROUP_READ_ONLY) &&
             !key.equals(DataTableColumns.GROUP_MODIFY) &&
             !key.equals(DataTableColumns.GROUP_PRIVILEGED)) {
          ColumnDefinition cd = columns.find(key);
          if (!cd.isUnitOfRetention()) {
            throw new IllegalStateException("key is not a database column name: " + key);
          }
        }
        // the only types are integer/long, float/double, string, boolean
        // complex types (array, object) should come across the interface as strings
        Object value = map.get(key);
        if (value == null) {
          cvValues.putNull(key);
        } else if (value instanceof Long) {
          cvValues.put(key, (Long) value);
        } else if (value instanceof Integer) {
          cvValues.put(key, (Integer) value);
        } else if (value instanceof Float) {
          cvValues.put(key, (Float) value);
        } else if (value instanceof Double) {
          cvValues.put(key, (Double) value);
        } else if (value instanceof String) {
          cvValues.put(key, (String) value);
        } else if (value instanceof Boolean) {
          cvValues.put(key, (Boolean) value);
        } else {
          throw new IllegalStateException("unimplemented case");
        }
      }
      return cvValues;
    } catch (IOException e) {
      WebLogger.getLogger(context.getAppName()).printStackTrace(e);
      throw new IllegalStateException("should never be reached");
    }
  }

  private void updateExecutorContext() {
    request.oldContext.releaseResources("switching to new WebFragment");
    context.popRequest(false);
  }

  private void getRolesList() throws ServicesAvailabilityException {
    String rolesList = dbInterface.getRolesList(context.getAppName());
    reportRolesListSuccessAndCleanUp(rolesList);
  }

  private void getDefaultGroup() throws ServicesAvailabilityException {
    String defaultGroup = dbInterface.getDefaultGroup(context.getAppName());
    reportDefaultGroupSuccessAndCleanUp(defaultGroup);
  }

  private void getUsersList() throws ServicesAvailabilityException {
    String usersList = dbInterface.getUsersList(context.getAppName());
    reportUsersListSuccessAndCleanUp(usersList);
  }

  private void getAllTableIds() throws ServicesAvailabilityException {
    List<String> tableIds = dbInterface.getAllTableIds(context.getAppName(), dbHandle);
    if (tableIds == null) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to obtain list of all tableIds");
    } else {
      reportListOfTableIdsSuccessAndCleanUp(tableIds);
    }
  }

  private void arbitraryQuery() throws ServicesAvailabilityException {
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }
    BaseTable baseTable = dbInterface
        .arbitrarySqlQuery(context.getAppName(), dbHandle, request.tableId, request.sqlCommand,
            request.sqlBindParams, request.limit, request.offset);

    if ( baseTable == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to rawQuery against: " + request.tableId +
          " sql: " + request.sqlCommand );
    } else {
      reportArbitraryQuerySuccessAndCleanUp(columns, baseTable);
    }
  }

  private void populateKeyValueStoreList(Map<String, Object> metadata,
      List<KeyValueStoreEntry> entries) {
    // keyValueStoreList
    if (entries != null) {
      // It is unclear how to most easily represent the KVS for access.
      // We use the convention in ODK Survey, which is a list of maps,
      // one per KVS row, with integer, number and boolean resolved to JS
      // types. objects and arrays are left unchanged.
      List<Map<String, Object>> kvsArray = new ArrayList<Map<String, Object>>();
      for (KeyValueStoreEntry entry : entries) {
        Object value = null;
        try {
          ElementDataType type = ElementDataType.valueOf(entry.type);
          if (entry.value != null) {
            if (type == ElementDataType.integer) {
              value = Long.parseLong(entry.value);
            } else if (type == ElementDataType.bool) {
              // This is broken - a value
              // of "TRUE" is returned some times
              value = entry.value;
              if (value != null) {
                try {
                  value = DataHelper.intToBool(Integer.parseInt(entry.value));
                } catch (Exception e) {
                  WebLogger.getLogger(context.getAppName()).e(TAG,
                      "ElementDataType: " + entry.type + " could not be converted from int");
                  try {
                    value = DataHelper.stringToBool(entry.value);
                  } catch (Exception e2) {
                    WebLogger.getLogger(context.getAppName()).e(TAG,
                        "ElementDataType: " + entry.type + " could not be converted from string");
                    e2.printStackTrace();
                  }
                }
              }
            } else if (type == ElementDataType.number) {
              value = Double.parseDouble(entry.value);
            } else {
              // string, array, object, rowpath, configpath and
              // anything else -- do not attempt to convert.
              // Leave as string.
              value = entry.value;
            }
          }
        } catch (IllegalArgumentException e) {
          // ignore?
          value = entry.value;
          WebLogger.getLogger(context.getAppName())
              .e(TAG, "Unrecognized ElementDataType: " + entry.type);
          WebLogger.getLogger(context.getAppName()).printStackTrace(e);
        }

        Map<String, Object> anEntry = new HashMap<String, Object>();
        anEntry.put("partition", entry.partition);
        anEntry.put("aspect", entry.aspect);
        anEntry.put("key", entry.key);
        anEntry.put("type", entry.type);
        anEntry.put("value", value);

        kvsArray.add(anEntry);
      }
      metadata.put("keyValueStoreList", kvsArray);
    }
  }

  private void reportArbitraryQuerySuccessAndCleanUp(OrderedColumns columnDefinitions,
      BaseTable userTable) throws ServicesAvailabilityException {
    List<KeyValueStoreEntry> entries = null;

    // We are assuming that we always have the KVS
    // otherwise use the request.includeKeyValueStoreMap
    //if (request.includeKeyValueStoreMap) {
    entries = dbInterface
        .getTableMetadata(context.getAppName(), dbHandle, request.tableId, null, null, null,
            userTable.getMetaDataRev()).getEntries();
    //}
    TableDefinitionEntry tdef = dbInterface
        .getTableDefinitionEntry(context.getAppName(), dbHandle, request.tableId);

    HashMap<String, Integer> elementKeyToIndexMap = new HashMap<String, Integer>();

    ArrayList<List<Object>> data = new ArrayList<List<Object>>();

    if ( userTable != null ) {
        int idx;
        // resolve the data types of all of the columns in the result set.
        Class<?>[] classes = new Class<?>[userTable.getWidth()];
        for ( idx = 0 ; idx < userTable.getWidth(); ++idx ) {
          // String is the default
          classes[idx] = String.class;
          // clean up the column name...
          String colName = userTable.getElementKey(idx);
          // set up the map -- use full column name here
          elementKeyToIndexMap.put(colName, idx);
          // remove any table alias qualifier from the name (assumes no quoting of column names)
          if ( colName.lastIndexOf('.') != -1 ) {
            colName = colName.substring(colName.lastIndexOf('.')+1);
          }
          // and try to deduce what type it should be...
        // we keep object and array as String
        // integer
          if ( colName.equals(DataTableColumns.CONFLICT_TYPE) ) {
            classes[idx] = Integer.class;
          } else {
            try {
              ColumnDefinition defn = columnDefinitions.find(colName);
              ElementDataType dataType = defn.getType().getDataType();
            Class<?> clazz = ColumnUtil.get().getOdkDataIfType(dataType);
              classes[idx] = clazz;
            } catch ( Exception e ) {
              // ignore
            }
          }
        }

        // assemble the data array
        for (int i = 0; i < userTable.getNumberOfRows(); ++i) {
          Row r = userTable.getRowAtIndex(i);
          Object[] values = new Object[userTable.getWidth()];

          for ( idx = 0 ; idx < userTable.getWidth() ; ++idx ) {
            values[idx] = r.getDataType(idx, classes[idx]);
          }
          data.add(Arrays.asList(values));
        }
    }

    Map<String, Object> metadata = new HashMap<String, Object>();
    ResumableQuery q = userTable.getQuery();
    if ( q != null ) {
      metadata.put("limit", q.getSqlLimit());
      metadata.put("offset", q.getSqlOffset());
    }
    metadata.put("canCreateRow", userTable.getEffectiveAccessCreateRow());
    metadata.put("tableId", columnDefinitions.getTableId());
    metadata.put("schemaETag", tdef.getSchemaETag());
    metadata.put("lastDataETag", tdef.getLastDataETag());
    metadata.put("lastSyncTime", tdef.getLastSyncTime());

    // elementKey -> index in row within row list
    metadata.put("elementKeyMap", elementKeyToIndexMap);
    // dataTableModel -- JS nested schema struct { elementName : extended_JS_schema_struct, ...}
    TreeMap<String, Object> dataTableModel = columnDefinitions.getExtendedDataModel();
    metadata.put("dataTableModel", dataTableModel);
    // keyValueStoreList
    populateKeyValueStoreList(metadata, entries);

    // raw queries are not extended.
    reportSuccessAndCleanUp(data, metadata);
  }

  private void reportRolesListSuccessAndCleanUp(String rolesList) throws
      ServicesAvailabilityException {

    ArrayList<String> roles = null;
    if ( rolesList != null ) {
      TypeReference<ArrayList<String>> type = new TypeReference<ArrayList<String>>() {};
      try {
        roles = ODKFileUtils.mapper.readValue(rolesList, type);
      } catch (IOException e) {
        WebLogger.getLogger(context.getAppName()).printStackTrace(e);
      }
    }

    Map<String, Object> metadata = new HashMap<String, Object>();
    metadata.put("roles", roles);

    reportSuccessAndCleanUp(null, metadata);
  }

  private void reportDefaultGroupSuccessAndCleanUp(String defaultGroup) throws
      ServicesAvailabilityException {

    Map<String, Object> metadata = new HashMap<String, Object>();
    if ( defaultGroup != null && defaultGroup.length() != 0 ) {
      metadata.put("defaultGroup", defaultGroup);
    }

    reportSuccessAndCleanUp(null, metadata);
  }

  private void reportUsersListSuccessAndCleanUp(String usersList) throws
      ServicesAvailabilityException {

    ArrayList<HashMap<String,Object>> users = null;
    if ( usersList != null ) {
      TypeReference<ArrayList<HashMap<String,Object>>> type = new
          TypeReference<ArrayList<HashMap<String,Object>>>() {};
      try {
        users = ODKFileUtils.mapper.readValue(usersList, type);
      } catch (IOException e) {
        WebLogger.getLogger(context.getAppName()).printStackTrace(e);
      }
    }

    Map<String, Object> metadata = new HashMap<String, Object>();
    metadata.put("users", users);

    reportSuccessAndCleanUp(null, metadata);
  }

  private void reportListOfTableIdsSuccessAndCleanUp(List<String> tableIds) throws ServicesAvailabilityException {

    Map<String, Object> metadata = new HashMap<String, Object>();
    metadata.put("tableIds", tableIds);

    // raw queries are not extended.
    reportSuccessAndCleanUp(null, metadata);
  }

  private void userTableQuery() throws ServicesAvailabilityException {
    String[] emptyArray = {};
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }
    UserTable t = dbInterface
        .simpleQuery(context.getAppName(), dbHandle, request.tableId, columns, request.whereClause,
            request.sqlBindParams, request.groupBy, request.having,
            (request.orderByElementKey == null) ?
                emptyArray :
                new String[] { request.orderByElementKey }, (request.orderByDirection == null) ?
                emptyArray :
                new String[] { request.orderByDirection }, request.limit, request.offset);

    if (t == null) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to query " + request.tableId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void reportSuccessAndCleanUp(UserTable userTable) throws ServicesAvailabilityException {
    List<KeyValueStoreEntry> entries = null;

    // We are assuming that we always have the KVS
    // otherwise use the request.includeKeyValueStoreMap
    //if (request.includeKeyValueStoreMap) {
    entries = dbInterface
        .getTableMetadata(context.getAppName(), dbHandle, request.tableId, null, null, null,
            userTable.getMetaDataRev()).getEntries();
    //}
    TableDefinitionEntry tdef = dbInterface
        .getTableDefinitionEntry(context.getAppName(), dbHandle, request.tableId);

    // assemble the data and metadata objects
    ArrayList<List<Object>> data = new ArrayList<List<Object>>();
    Map<String, Integer> elementKeyToIndexMap = userTable.getElementKeyToIndex();
    Integer idxEffectiveAccessColumn =
        elementKeyToIndexMap.get(DataTableColumns.EFFECTIVE_ACCESS);

    OrderedColumns columnDefinitions = userTable.getColumnDefinitions();

    for (int i = 0; i < userTable.getNumberOfRows(); ++i) {
      Row r = userTable.getRowAtIndex(i);
      Object[] typedValues = new Object[elementKeyToIndexMap.size()];
      List<Object> typedValuesAsList = Arrays.asList(typedValues);
      data.add(typedValuesAsList);

      for (String name : ADMIN_COLUMNS) {
        int idx = elementKeyToIndexMap.get(name);
        if (name.equals(DataTableColumns.CONFLICT_TYPE)) {
          Integer value = r.getDataType(name, Integer.class);
          typedValues[idx] = value;
        } else {
          String value = r.getDataType(name, String.class);
          typedValues[idx] = value;
        }
      }

      ArrayList<String> elementKeys = columnDefinitions.getRetentionColumnNames();

      for (String name : elementKeys) {
        int idx = elementKeyToIndexMap.get(name);
        ColumnDefinition defn = columnDefinitions.find(name);
        ElementDataType dataType = defn.getType().getDataType();
        Class<?> clazz = ColumnUtil.get().getOdkDataIfType(dataType);
        Object value = r.getDataType(name, clazz);
        typedValues[idx] = value;
      }

      if ( idxEffectiveAccessColumn != null ) {

        typedValues[idxEffectiveAccessColumn] =
            r.getDataType(DataTableColumns.EFFECTIVE_ACCESS, String.class);
      }
    }

    Map<String, Object> metadata = new HashMap<String, Object>();
    ResumableQuery q = userTable.getQuery();
    if ( q != null ) {
      metadata.put("limit", q.getSqlLimit());
      metadata.put("offset", q.getSqlOffset());
    }
    metadata.put("canCreateRow", userTable.getEffectiveAccessCreateRow());
    metadata.put("tableId", userTable.getTableId());
    metadata.put("schemaETag", tdef.getSchemaETag());
    metadata.put("lastDataETag", tdef.getLastDataETag());
    metadata.put("lastSyncTime", tdef.getLastSyncTime());

    // elementKey -> index in row within row list
    metadata.put("elementKeyMap", elementKeyToIndexMap);
    // dataTableModel -- JS nested schema struct { elementName : extended_JS_schema_struct, ...}
    TreeMap<String, Object> dataTableModel = columnDefinitions.getExtendedDataModel();
    metadata.put("dataTableModel", dataTableModel);
    // keyValueStoreList
    populateKeyValueStoreList(metadata, entries);

    // extend the metadata with whatever else this app needs....
    // e.g., row and column color maps
    extendQueryMetadata(dbInterface, dbHandle, entries, userTable, metadata);
    reportSuccessAndCleanUp(data, metadata);
  }

  protected abstract void extendQueryMetadata(UserDbInterface dbInterface, DbHandle dbHandle,
      List<KeyValueStoreEntry> entries, UserTable userTable, Map<String, Object> metadata);

  private void getRows() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if (request.rowId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }
    UserTable t = dbInterface
        .getRowsWithId(context.getAppName(), dbHandle, request.tableId, columns, request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to getRows for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void getMostRecentRow() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if (request.rowId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }
    UserTable t = dbInterface
        .getMostRecentRowWithId(context.getAppName(), dbHandle, request.tableId, columns,
            request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to getMostRecentRow for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void updateRow() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if (request.rowId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }

    ContentValues cvValues = convertJSON(columns, request.stringifiedJSON);
    UserTable t = dbInterface
        .updateRowWithId(context.getAppName(), dbHandle, request.tableId, columns, cvValues,
            request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to updateRow for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void changeAccessFilterRow() throws ServicesAvailabilityException,
      ActionNotAuthorizedException {
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if (request.rowId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }

    ContentValues cvValues = convertJSON(columns, request.stringifiedJSON);
    String defaultAccess = cvValues.getAsString(DataTableColumns.DEFAULT_ACCESS);
    String owner = cvValues.getAsString(DataTableColumns.ROW_OWNER);
    String groupReadOnly = cvValues.getAsString(DataTableColumns.GROUP_READ_ONLY);
    String groupModify = cvValues.getAsString(DataTableColumns.GROUP_MODIFY);
    String groupPrivileged = cvValues.getAsString(DataTableColumns.GROUP_PRIVILEGED);

    UserTable t = dbInterface
        .changeRowFilterWithId(context.getAppName(), dbHandle, request.tableId,
            columns, defaultAccess, owner, groupReadOnly, groupModify, groupPrivileged, request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to changeAccessFilterRow for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void deleteRow() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if (request.rowId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }

    UserTable t = dbInterface
        .deleteRowWithId(context.getAppName(), dbHandle, request.tableId, columns, request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to deleteRow for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void addRow() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if (request.rowId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }

    ContentValues cvValues = convertJSON(columns, request.stringifiedJSON);
    UserTable t = dbInterface
        .insertRowWithId(context.getAppName(), dbHandle, request.tableId, columns, cvValues,
            request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to addRow for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void addCheckpoint() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if ( request.tableId == null ) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if ( request.rowId == null ) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if ( columns == null ) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }

    ContentValues cvValues = convertJSON(columns, request.stringifiedJSON);
    UserTable t = dbInterface
        .insertCheckpointRowWithId(context.getAppName(), dbHandle, request.tableId, columns,
            cvValues, request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to addCheckpoint for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void saveCheckpointAsIncomplete() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if (request.rowId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }

    UserTable t = dbInterface
        .saveAsIncompleteMostRecentCheckpointRowWithId(context.getAppName(), dbHandle,
            request.tableId, columns, request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to saveCheckpointAsIncomplete for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void saveCheckpointAsComplete() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if (request.tableId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if (request.rowId == null) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }
    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }

    UserTable t = dbInterface
        .saveAsCompleteMostRecentCheckpointRowWithId(context.getAppName(), dbHandle,
            request.tableId, columns, request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to saveCheckpointAsComplete for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void deleteLastCheckpoint() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if ( request.tableId == null ) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if ( request.rowId == null ) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }

    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }

    UserTable t = dbInterface
        .deleteLastCheckpointRowWithId(context.getAppName(), dbHandle, request.tableId, columns,
            request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to deleteLastCheckpoint for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

  private void deleteAllCheckpoints() throws ServicesAvailabilityException, ActionNotAuthorizedException {
    if ( request.tableId == null ) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": tableId cannot be null");
      return;
    }
    if ( request.rowId == null ) {
      reportErrorAndCleanUp(IllegalArgumentException.class.getName() + ": rowId cannot be null");
      return;
    }

    OrderedColumns columns = context.getOrderedColumns(request.tableId);
    if (columns == null) {
      columns = dbInterface.getUserDefinedColumns(context.getAppName(), dbHandle, request.tableId);
      context.putOrderedColumns(request.tableId, columns);
    }

    //ContentValues cvValues = convertJSON(columns, request.stringifiedJSON);
    UserTable t = dbInterface
        .deleteAllCheckpointRowsWithId(context.getAppName(), dbHandle, request.tableId, columns,
            request.rowId);

    if ( t == null ) {
      reportErrorAndCleanUp(IllegalStateException.class.getName() + ": Unable to deleteAllCheckpoints for " +
          request.tableId + "._id = " +  request.rowId);
    } else {
      reportSuccessAndCleanUp(t);
    }
  }

}
