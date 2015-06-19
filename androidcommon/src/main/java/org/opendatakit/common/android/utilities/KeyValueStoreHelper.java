/*
 * Copyright (C) 2012 University of Washington
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
import java.util.List;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.common.android.application.CommonApplication;
import org.opendatakit.common.android.database.DatabaseConstants;
import org.opendatakit.database.service.KeyValueStoreEntry;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.database.service.OdkDbInterface;

import android.os.RemoteException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.type.CollectionType;

/**
 * A helper class to access values in the key value store. The partition must
 * be set in the creation of the object, ensuring that this helper can only
 * access those keys in its partition.
 * @author sudar.sam@gmail.com
 *
 */
public class KeyValueStoreHelper implements KeyValueHelper {

  private static final String TAG = KeyValueStoreHelper.class.getName();

  /**
   * This is the default aspect that will be used when interacting with the
   * key value store via this object. If a named aspect is required (note that
   * it cannot share the same name as this field),
   * {@link AspectKeyValueStoreHelper} must be used.
   */

  /*
   * This is the partition which this helper will be restricted to.
   */
  private final String partition;
  private final String tableId;
  private final String appName;
  private final CommonApplication context;
  private final OdkDbHandle dbHandle;

  public KeyValueStoreHelper(CommonApplication context, String appName, OdkDbHandle dbHandle, String tableId, String partition) {
    if ( dbHandle == null ) {
      throw new IllegalArgumentException("Unexpected null value for dbHandle");
    }
    this.context = context;
    this.appName = appName;
    this.tableId = tableId;
    this.partition = partition;
    this.dbHandle = dbHandle;
  }

  private OdkDbInterface getDatabase() {
    return context.getDatabase();
  }
  
  /**
   * Get the accessor for the partition specified by this object as well as
   * the given aspect.
   * @param aspect
   * @return
   */
  public AspectHelper getAspectHelper(String aspect) {
    return new AspectHelper(aspect);
  }

  /**
   * The partition of the key value store.
   * @return
   */
  public String getPartition() {
    return this.partition;
  }
  
  public String getTableId() {
    return this.tableId;
  }

  public String getAppName() {
    return this.appName;
  }
  
  @Override
  public Integer getInteger(String key) throws RemoteException {
    return getInteger(KeyValueStoreConstants.ASPECT_DEFAULT, key);
  }

  private Integer getInteger(String aspect, String key) throws RemoteException {
    KeyValueStoreEntry entry = getEntry(aspect, key);
    if (entry == null) {
      return null;
    }
    if (!entry.type.equals(ElementDataType.integer.name())) {
      throw new IllegalArgumentException("requested int entry for " +
          "key: " + key + ", but the corresponding entry in the store was " +
          "not of type: " + ElementDataType.integer.name());
    }
    return Integer.parseInt(entry.value);
  }

  @Override
  public <T> ArrayList<T> getArray(String key, Class<T> clazz) throws RemoteException {
    return getArray(KeyValueStoreConstants.ASPECT_DEFAULT, key, clazz);
  }

  private <T> ArrayList<T> getArray(String aspect, String key, Class<T> clazz) throws RemoteException {
    CollectionType javaType =
        ODKFileUtils.mapper.getTypeFactory().constructCollectionType(ArrayList.class, clazz);
    KeyValueStoreEntry entry = getEntry(aspect, key);
    if (entry == null) {
      return null;
    }
    if (!entry.type.equals(ElementDataType.array.name())) {
      throw new IllegalArgumentException("requested list entry for " +
          "key: " + key + ", but the corresponding entry in the store was " +
          "not of type: " + ElementDataType.array.name());
    }
    ArrayList<T> result = null;
    try {
      if ( entry.value != null && entry.value.length() != 0 ) {
        result = ODKFileUtils.mapper.readValue(entry.value, javaType);
      }
    } catch (JsonParseException e) {
      WebLogger.getLogger(appName).e(TAG, "problem parsing json list entry from the kvs");
      WebLogger.getLogger(appName).printStackTrace(e);
    } catch (JsonMappingException e) {
      WebLogger.getLogger(appName).e(TAG, "problem mapping json list entry from the kvs");
      WebLogger.getLogger(appName).printStackTrace(e);
    } catch (IOException e) {
      WebLogger.getLogger(appName).e(TAG, "i/o problem with json for list entry from the kvs");
      WebLogger.getLogger(appName).printStackTrace(e);
    }
    return result;
  }

  @Override
  public String getString(String key) throws RemoteException {
    return getString(KeyValueStoreConstants.ASPECT_DEFAULT, key);
  }

  private String getString(String aspect, String key) throws RemoteException {
    KeyValueStoreEntry entry = getEntry(aspect, key);
    if (entry == null) {
      return null;
    }
    if (!entry.type.equals(ElementDataType.string.name())) {
      throw new IllegalArgumentException("requested string entry for " +
          "key: " + key + ", but the corresponding entry in the store was " +
          "not of type: " + ElementDataType.string.name());
    }
    return entry.value;
  }

  @Override
  public String getObject(String key) throws RemoteException {
    return getObject(KeyValueStoreConstants.ASPECT_DEFAULT, key);
  }

  private String getObject(String aspect, String key) throws RemoteException {
    KeyValueStoreEntry entry = getEntry(aspect, key);
    if (entry == null) {
      return null;
    }
    if (!entry.type.equals(ElementDataType.object.name()) &&
        !entry.type.equals(ElementDataType.array.name())) {
      throw new IllegalArgumentException("requested object entry for " +
          "key: " + key + ", but the corresponding entry in the store was " +
          "not of type: " + ElementDataType.object.name() +
          " or: "  + ElementDataType.array.name());
    }
    return entry.value;
  }

  @Override
  public Boolean getBoolean(String key) throws RemoteException {
    return getBoolean(KeyValueStoreConstants.ASPECT_DEFAULT, key);
  }

  private Boolean getBoolean(String aspect, String key) throws RemoteException {
    KeyValueStoreEntry entry = getEntry(aspect, key);
    if (entry == null) {
      return null;
    }
    if (!entry.type.equals(ElementDataType.bool.name())) {
      throw new IllegalArgumentException("requested boolean entry for " +
          "key: " + key + ", but the corresponding entry in the store was " +
          "not of type: " + ElementDataType.bool.name());
    }
    return DataHelper.intToBool(Integer.parseInt(entry.value));
  }

  @Override
  public Double getNumber(String key) throws RemoteException {
    return getNumber(KeyValueStoreConstants.ASPECT_DEFAULT, key);
  }

  private Double getNumber(String aspect, String key) throws RemoteException {
    KeyValueStoreEntry entry = getEntry(aspect, key);
    if (entry == null) {
      return null;
    }
    if (!entry.type.equals(ElementDataType.number.name())) {
      throw new IllegalArgumentException("requested number entry for " +
          "key: " + key + ", but the corresponding entry in the store was " +
          "not of type: " + ElementDataType.number.name());
    }
    return Double.parseDouble(entry.value);
  }

  @Override
  public void setInteger(String key, Integer value) throws RemoteException {
    setIntegerEntry(KeyValueStoreConstants.ASPECT_DEFAULT, key, value);
  }

  private void setIntegerEntry(String aspect, String key, Integer value) throws RemoteException {
    KeyValueStoreEntry entry = new KeyValueStoreEntry();
    entry.tableId = this.getTableId();
    entry.partition = this.getPartition();
    entry.aspect = aspect;
    entry.key = key;
    entry.type = ElementDataType.integer.name();
    entry.value = Integer.toString(value);
    getDatabase().replaceDBTableMetadata(appName, dbHandle, entry);
  }

  @Override
  public void setNumber(String key, Double value) throws RemoteException {
    setNumberEntry(KeyValueStoreConstants.ASPECT_DEFAULT, key, value);
  }

  private void setNumberEntry(String aspect, String key, Double value) throws RemoteException {
    KeyValueStoreEntry entry = new KeyValueStoreEntry();
    entry.tableId = this.getTableId();
    entry.partition = this.getPartition();
    entry.aspect = aspect;
    entry.key = key;
    entry.type = ElementDataType.number.name();
    entry.value = Double.toString(value);
    getDatabase().replaceDBTableMetadata(appName, dbHandle, entry);
  }

  @Override
  public void setObject(String key, String jsonOfObject) throws RemoteException {
    setObjectEntry(KeyValueStoreConstants.ASPECT_DEFAULT, key, jsonOfObject);
  }

  private void setObjectEntry(String aspect, String key, String jsonOfObject) throws RemoteException {
    KeyValueStoreEntry entry = new KeyValueStoreEntry();
    entry.tableId = this.getTableId();
    entry.partition = this.getPartition();
    entry.aspect = aspect;
    entry.key = key;
    entry.type = ElementDataType.object.name();
    entry.value = jsonOfObject;
    getDatabase().replaceDBTableMetadata(appName, dbHandle, entry);
  }

  @Override
  public void setBoolean(String key, Boolean value) throws RemoteException {
    setBooleanEntry(KeyValueStoreConstants.ASPECT_DEFAULT, key, value);
  }

  /**
   * Set the boolean entry for this aspect and key.
   * @param aspect
   * @param key
   * @param value
   * @throws RemoteException 
   */
  private void setBooleanEntry(String aspect, String key, Boolean value) throws RemoteException {
    KeyValueStoreEntry entry = new KeyValueStoreEntry();
    entry.tableId = this.getTableId();
    entry.partition = this.getPartition();
    entry.aspect = aspect;
    entry.key = key;
    entry.type = ElementDataType.bool.name();
    entry.value = Integer.toString(DataHelper.boolToInt(value));
    getDatabase().replaceDBTableMetadata(appName, dbHandle, entry);
  }

  @Override
  public void setString(String key, String value) throws RemoteException {
    setStringEntry(KeyValueStoreConstants.ASPECT_DEFAULT, key, value);
  }

  /**
   * Set the given String entry.
   * @param aspect
   * @param key
   * @param value
   * @throws RemoteException 
   */
  private void setStringEntry(String aspect, String key, String value) throws RemoteException {
    KeyValueStoreEntry entry = new KeyValueStoreEntry();
    entry.tableId = this.getTableId();
    entry.partition = this.getPartition();
    entry.aspect = aspect;
    entry.key = key;
    entry.type = ElementDataType.string.name();
    entry.value = value;
    getDatabase().replaceDBTableMetadata(appName, dbHandle, entry);
  }

  /**
   * API fo ruse when called within a transaction.
   *
   * @param db
   * @param key
   * @param value
   * @throws RemoteException 
   */
  public void setString(OdkDbHandle db, String key, String value) throws RemoteException {
	  setStringEntry(db, KeyValueStoreConstants.ASPECT_DEFAULT, key, value);
  }

  /**
   * API for use when called within a transaction.
   *
   * @param db
   * @param aspect
   * @param key
   * @param value
   * @throws RemoteException 
   */
  public void setStringEntry(OdkDbHandle db, String aspect, String key, String value) throws RemoteException {
    KeyValueStoreEntry entry = new KeyValueStoreEntry();
    entry.tableId = this.getTableId();
    entry.partition = this.getPartition();
    entry.aspect = aspect;
    entry.key = key;
    entry.type = ElementDataType.string.name();
    entry.value = value;
    getDatabase().replaceDBTableMetadata(appName, db, entry);
  }

  @Override
  public <T> void setArray(String key, ArrayList<T> value) throws RemoteException {
    setArrayEntry(KeyValueStoreConstants.ASPECT_DEFAULT, key, value);
  }

  /**
   * Set the list entry for the given aspect and key.
   * @param aspect
   * @param key
   * @param value
   * @throws RemoteException 
   */
  private <T> void setArrayEntry(String aspect, String key,
      ArrayList<T> value) throws RemoteException {
    String entryValue = null;
    try {
      if (value != null && value.size() > 0) {
        entryValue = ODKFileUtils.mapper.writeValueAsString(value);
      } else {
        entryValue = ODKFileUtils.mapper.writeValueAsString(new ArrayList<T>());
      }
    } catch (JsonGenerationException e) {
      WebLogger.getLogger(appName).e(TAG, "problem parsing json list entry while writing to the kvs");
      WebLogger.getLogger(appName).printStackTrace(e);
    } catch (JsonMappingException e) {
      WebLogger.getLogger(appName).e(TAG, "problem mapping json list entry while writing to the kvs");
      WebLogger.getLogger(appName).printStackTrace(e);
    } catch (IOException e) {
      WebLogger.getLogger(appName).e(TAG, "i/o exception with json list entry while writing to the" +
            " kvs");
      WebLogger.getLogger(appName).printStackTrace(e);
    }
    if (entryValue == null) {
      WebLogger.getLogger(appName).e(TAG, "problem parsing list to json, not updating key");
      return;
    }
    KeyValueStoreEntry entry = new KeyValueStoreEntry();
    entry.tableId = this.getTableId();
    entry.partition = this.getPartition();
    entry.aspect = aspect;
    entry.key = key;
    entry.type = ElementDataType.array.name();
    entry.value = entryValue;
    getDatabase().replaceDBTableMetadata(appName, dbHandle, entry);
  }

  @Override
  public void removeKey(String key) throws RemoteException {
    removeEntry(KeyValueStoreConstants.ASPECT_DEFAULT, key);
  }

  /**
   * Remove the entries for the given aspect and key.
   * @param aspect
   * @param key
   * @return
   * @throws RemoteException 
   */
  private void removeEntry(String aspect, String key) throws RemoteException {
    getDatabase().deleteDBTableMetadata(appName, dbHandle, this.getTableId(), this.getPartition(), aspect, key);
  }

  @Override
  public KeyValueStoreEntry getEntry(String key) throws RemoteException {
    return getEntry(KeyValueStoreConstants.ASPECT_DEFAULT, key);
  }

  /**
   * Return the entry for the given aspect and key, using the partition field.
   * <p>
   * Return null if the given entry doesn't exist. Logging is done if there is
   * more than one key matching the specifications, as this as an error. The
   * first entry in the list is still returned, however.
   * @param aspect
   * @param key
   * @return
   * @throws RemoteException 
   */
  private KeyValueStoreEntry getEntry(String aspect, String key) throws RemoteException {
    List<KeyValueStoreEntry> entries =
        getDatabase().getDBTableMetadata(appName, dbHandle, this.getTableId(), this.getPartition(), aspect, key);
    // Do some sanity checking. There should only ever be one entry per key.
    if (entries.size() > 1) {
      WebLogger.getLogger(appName).e(TAG, "request for key: " + key + " in KVS " +
          DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME +
          " for table: " + this.getTableId() + " returned " + entries.size() +
          "entries. It should return at most 1, as it is a key in a set.");
    }
    if (entries.size() == 0) {
      return null;
    } else {
      return entries.get(0);
    }
  }

  /**
   * Much like the outer KeyValueStoreHelper class, except that this also
   * specifies an aspect. All the methods apply to the partition of the
   * enclosing class and the aspect of this class.
   * @author sudar.sam@gmail.com
   *
   */
  public class AspectHelper implements KeyValueHelper {

    private final String aspect;

    /**
     * Private so that you can only get it via the factory class.
     * @param aspect
     */
    private AspectHelper(String aspect) {
      this.aspect = aspect;
    }

    @Override
    public Integer getInteger(String key) throws RemoteException {
      return KeyValueStoreHelper.this.getInteger(aspect, key);
    }

    @Override
    public <T> ArrayList<T> getArray(String key, Class<T> clazz) throws RemoteException {
      return KeyValueStoreHelper.this.getArray(aspect, key, clazz);
    }

    @Override
    public String getString(String key) throws RemoteException {
      return KeyValueStoreHelper.this.getString(aspect, key);
    }

    @Override
    public String getObject(String key) throws RemoteException {
      return KeyValueStoreHelper.this.getObject(aspect, key);
    }

    @Override
    public Boolean getBoolean(String key) throws RemoteException {
      return KeyValueStoreHelper.this.getBoolean(aspect, key);
    }

    @Override
    public Double getNumber(String key) throws RemoteException {
      return KeyValueStoreHelper.this.getNumber(aspect, key);
    }

    @Override
    public void setInteger(String key, Integer value) throws RemoteException {
      KeyValueStoreHelper.this.setIntegerEntry(aspect, key, value);
    }

    @Override
    public void setNumber(String key, Double value) throws RemoteException {
      KeyValueStoreHelper.this.setNumberEntry(aspect, key, value);
    }

    @Override
    public void setObject(String key, String jsonOfObject) throws RemoteException {
      KeyValueStoreHelper.this.setObjectEntry(aspect, key, jsonOfObject);
    }

    @Override
    public void setBoolean(String key, Boolean value) throws RemoteException {
      KeyValueStoreHelper.this.setBooleanEntry(aspect, key, value);
    }

    @Override
    public void setString(String key, String value) throws RemoteException {
      KeyValueStoreHelper.this.setStringEntry(aspect, key, value);
    }

    @Override
    public <T> void setArray(String key, ArrayList<T> value) throws RemoteException {
      KeyValueStoreHelper.this.setArrayEntry(aspect, key, value);
    }

    @Override
    public void removeKey(String key) throws RemoteException {
      KeyValueStoreHelper.this.removeEntry(aspect, key);
    }

    @Override
    public KeyValueStoreEntry getEntry(String key) throws RemoteException {
      return KeyValueStoreHelper.this.getEntry(aspect, key);
    }

    /**
     * Delete all the entries in the given aspect.
     * @return
     * @throws RemoteException 
     */
    public void deleteAllEntriesInThisAspect() throws RemoteException {
      getDatabase().deleteDBTableMetadata(appName, dbHandle, KeyValueStoreHelper.this.getTableId(), partition, aspect, null);
    }

  }


}
