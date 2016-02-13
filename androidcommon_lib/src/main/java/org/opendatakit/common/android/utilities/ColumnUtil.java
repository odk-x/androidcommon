/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.common.android.utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.type.CollectionType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.common.android.application.CommonApplication;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.JoinColumn;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;
import org.opendatakit.database.service.KeyValueStoreEntry;
import org.opendatakit.database.service.OdkDbHandle;

import android.os.RemoteException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class ColumnUtil {
  
  private static ColumnUtil columnUtil = new ColumnUtil();
 
  static {
    // register a state-reset manipulator for 'columnUtil' field.
    StaticStateManipulator.get().register(50, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        columnUtil = new ColumnUtil();
      }
      
    });
  }
  
  public static ColumnUtil get() {
    return columnUtil;
  }
 
  /**
   * For mocking -- supply a mocked object.
   * 
   * @param util
   */
  public static void set(ColumnUtil util) {
    columnUtil = util;
  }

  protected ColumnUtil() {}

  /**
   * Return the element key for the column based on the element path.
   * By convention, if the key has nested elements, we can just replace 
   * all dots with underscores to generate the elementKey from the path.
   *
   * @param elementPath
   * @return elementKey
   */
  public String getElementKeyFromElementPath(String elementPath) {
    // TODO: should we verify that this key actually exists?
    String hackPath = elementPath.replace(".", "_");
    return hackPath;
  }

  public String getLocalizedDisplayName(CommonApplication ctxt, String appName, OdkDbHandle db, String tableId, String elementKey) throws RemoteException {

    String jsonDisplayName = getRawDisplayName(ctxt, appName, db, tableId, elementKey);
    String displayName = ODKDataUtils.getLocalizedDisplayName(jsonDisplayName);
    return displayName;
  }

  public String getRawDisplayName(CommonApplication ctxt, String appName, OdkDbHandle db, String tableId, String elementKey) throws RemoteException {

    List<KeyValueStoreEntry> displayNameList =
            ctxt.getDatabase().getDBTableMetadata(appName, db, tableId,
                    KeyValueStoreConstants.PARTITION_COLUMN, elementKey, KeyValueStoreConstants.COLUMN_DISPLAY_NAME);
    if ( displayNameList.size() != 1 ) {
      // default to the column elementKey
      return NameUtil.normalizeDisplayName(NameUtil.constructSimpleDisplayName(elementKey));
    }
    String jsonDisplayName = KeyValueStoreUtils.getObject(appName, displayNameList.get(0));
    if ( jsonDisplayName == null ) {
      jsonDisplayName = NameUtil.normalizeDisplayName(NameUtil.constructSimpleDisplayName(elementKey));
    }
    return jsonDisplayName;
  }

  public ArrayList<Map<String,Object>> getDisplayChoicesList(CommonApplication ctxt, String appName, OdkDbHandle db, String tableId, String elementKey) throws RemoteException {

    List<KeyValueStoreEntry> choicesListList =
            ctxt.getDatabase().getDBTableMetadata(appName, db, tableId,
                    KeyValueStoreConstants.PARTITION_COLUMN, elementKey, KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST);
    if ( choicesListList.size() != 1 ) {
      // default to none
      return new ArrayList<Map<String,Object>>();
    }
    /*
     * Getting the choiceListId
     */
    String choiceListId = KeyValueStoreUtils.getString(appName, choicesListList.get(0));
    if (choiceListId == null || choiceListId.trim().length() == 0) {
      return new ArrayList<Map<String,Object>>();
    }
    /*
     * Use that to get the choiceListJSON
     */
    String choiceListJSON = ctxt.getDatabase().getChoiceList(appName, db, choiceListId);
    if (choiceListJSON == null || choiceListJSON.trim().length() == 0) {
      return new ArrayList<Map<String,Object>>();
    }
    // and transform the JSON into an array of objects holding the
    // choice value and the language-to-displayName translation maps
    CollectionType javaType =
        ODKFileUtils.mapper.getTypeFactory().constructCollectionType(ArrayList.class, Map.class);
    ArrayList<Map> result = null;
    try {
        result = ODKFileUtils.mapper.readValue(choiceListJSON, javaType);
    } catch (JsonParseException e) {
      WebLogger.getLogger(appName).e("ColumnUtil",
          "getDisplayChoicesList: problem parsing json list entry from the kvs");
      WebLogger.getLogger(appName).printStackTrace(e);
    } catch (JsonMappingException e) {
      WebLogger.getLogger(appName).e("ColumnUtil",
          "getDisplayChoicesList: problem mapping json list entry from the kvs");
      WebLogger.getLogger(appName).printStackTrace(e);
    } catch (IOException e) {
      WebLogger.getLogger(appName).e("ColumnUtil",
          "getDisplayChoicesList: i/o problem with json for list entry from the kvs");
      WebLogger.getLogger(appName).printStackTrace(e);
    }
    if(result == null) {
      return new ArrayList<Map<String,Object>>();
    }
    
    ArrayList<Map<String,Object>> jsonDisplayChoices = new ArrayList<Map<String,Object>>();
    for ( Map m : result) {
      @SuppressWarnings("unchecked")
      Map<String,Object> tm = (Map<String,Object>) m;
      jsonDisplayChoices.add(tm);
    }
    return jsonDisplayChoices;
  }

  public void setDisplayChoicesList( CommonApplication ctxt, String appName, OdkDbHandle db, String tableId, ColumnDefinition cd, ArrayList<Map<String,Object>> choices) throws RemoteException {
    String choiceListJSON = null;
    try {
      choiceListJSON = ODKFileUtils.mapper.writeValueAsString(choices);
    } catch (JsonProcessingException e1) {
      e1.printStackTrace();
      throw new IllegalArgumentException("Unexpected displayChoices conversion failure!");
    }
    String choiceListId = ctxt.getDatabase().setChoiceList(appName, db, choiceListJSON);
    KeyValueStoreEntry e = KeyValueStoreUtils.buildEntry(tableId,
        KeyValueStoreConstants.PARTITION_COLUMN, cd.getElementKey(),
        KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST, ElementDataType.string, choiceListId);
    ctxt.getDatabase().replaceDBTableMetadata(appName, db, e);
  }
  
  public ArrayList<JoinColumn> getJoins(CommonApplication ctxt, String appName, OdkDbHandle db, String tableId, String elementKey) throws RemoteException {

    List<KeyValueStoreEntry> joinsList =
            ctxt.getDatabase().getDBTableMetadata(appName, db, tableId,
                KeyValueStoreConstants.PARTITION_COLUMN, elementKey,
                KeyValueStoreConstants.COLUMN_JOINS);
    if ( joinsList.size() != 1 ) {
      return new ArrayList<JoinColumn>();
    }

    ArrayList<JoinColumn> joins = null;
    try {
      joins = JoinColumn.fromSerialization(KeyValueStoreUtils.getObject(appName, joinsList.get(0)));
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return (joins == null) ? new ArrayList<JoinColumn>() : joins;
  }

  public int getColumnWidth( CommonApplication ctxt, String appName, OdkDbHandle db, String tableId, String elementKey) throws RemoteException {
    List<KeyValueStoreEntry> kvsList =
            ctxt.getDatabase().getDBTableMetadata(appName, db, tableId,
                    LocalKeyValueStoreConstants.Spreadsheet.PARTITION,
                    elementKey,
                    LocalKeyValueStoreConstants.Spreadsheet.KEY_COLUMN_WIDTH);
    if (kvsList.size() != 1) {
      return LocalKeyValueStoreConstants.Spreadsheet.DEFAULT_COL_WIDTH;
    }
    Integer value = KeyValueStoreUtils.getInteger(appName, kvsList.get(0));
    if (value == null || value <= 0) {
      return LocalKeyValueStoreConstants.Spreadsheet.DEFAULT_COL_WIDTH;
    }
    if ( value > LocalKeyValueStoreConstants.Spreadsheet.MAX_COL_WIDTH ) {
      return LocalKeyValueStoreConstants.Spreadsheet.MAX_COL_WIDTH;
    }
    return value;
  }

  /**
   * Set the width of the given column.
   *
   * @param ctxt
   * @param appName
   * @param db
   * @param tableId
   * @param elementKey
   * @param width
   * @throws RemoteException
   */
  public void setColumnWidth( CommonApplication ctxt, String appName, OdkDbHandle db, String tableId, String elementKey, Integer width) throws RemoteException {
    KeyValueStoreEntry e = KeyValueStoreUtils.buildEntry(tableId,
            LocalKeyValueStoreConstants.Spreadsheet.PARTITION,
            elementKey,
            LocalKeyValueStoreConstants.Spreadsheet.KEY_COLUMN_WIDTH,
            ElementDataType.integer, (width == null) ? null : Integer.toString(width));
    ctxt.getDatabase().replaceDBTableMetadata(appName, db, e);
  }

  /**
   * Wrapper to handle database interactions for setIndexColumn()
   *
   * @param ctxt
   * @param appName
   * @param tableId
   * @param elementKey
   * @throws RemoteException
   */
  public void atomicSetColumnWidth( CommonApplication ctxt, String appName, String tableId, String elementKey, Integer width) throws RemoteException {
    OdkDbHandle db = null;
    try {
      db = ctxt.getDatabase().openDatabase(appName);

      setColumnWidth(ctxt, appName, db, tableId, elementKey, width);
    } catch (RemoteException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      throw e;
    } finally {
      if ( db != null ) {
        try {
          ctxt.getDatabase().closeDatabase(appName, db);
        } catch (RemoteException e) {
          WebLogger.getLogger(appName).printStackTrace(e);
          throw e;
        }
      }
    }
  }

  public Map<String, Integer> getColumnWidths( CommonApplication ctxt, String appName, OdkDbHandle db, String tableId, OrderedColumns columns) throws RemoteException {
    List<KeyValueStoreEntry> kvsList =
            ctxt.getDatabase().getDBTableMetadata(appName, db, tableId,
                    LocalKeyValueStoreConstants.Spreadsheet.PARTITION,
                    null,
                    LocalKeyValueStoreConstants.Spreadsheet.KEY_COLUMN_WIDTH);
    Map<String, Integer> colWidths = new HashMap<String, Integer>();
    for ( KeyValueStoreEntry entry : kvsList ) {
      Integer value = KeyValueStoreUtils.getInteger(appName, entry);
      if (value == null || value <= 0) {
        value = LocalKeyValueStoreConstants.Spreadsheet.DEFAULT_COL_WIDTH;
      }
      if ( value > LocalKeyValueStoreConstants.Spreadsheet.MAX_COL_WIDTH ) {
        value = LocalKeyValueStoreConstants.Spreadsheet.MAX_COL_WIDTH;
      }
      colWidths.put(entry.aspect, value);
    }
    for ( ColumnDefinition cd : columns.getColumnDefinitions() ) {
      if ( !colWidths.containsKey(cd.getElementKey()) ) {
        colWidths.put(cd.getElementKey(), LocalKeyValueStoreConstants.Spreadsheet.DEFAULT_COL_WIDTH);
      }
    }
    return colWidths;
  }

  /**
   * These are the data type mappings used across the odkData Javascript interface.
   * NOTE: integer data types are mapped to Long values.
   * This is consistent with the support for Long values in Javascript.
   *
   * @param dataType
   * @return
   */
  public Class<?> getOdkDataIfType(ElementDataType dataType) {
    
    if ( dataType == ElementDataType.array ) {
      return String.class;
    }
    if (dataType == ElementDataType.object) {
      return String.class;
    }
    
    if ( dataType == ElementDataType.integer ) {
      return Long.class;
    }

    if ( dataType == ElementDataType.number ) {
      return Double.class;
    }

    if ( dataType == ElementDataType.bool ) {
      return Boolean.class;
    }

    return String.class;
  }
}
