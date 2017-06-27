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
package org.opendatakit.data;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.data.utilities.ColorRuleUtil;
import org.opendatakit.database.LocalKeyValueStoreConstants;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.Row;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.service.UserDbInterface;
import org.opendatakit.database.utilities.KeyValueStoreUtils;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.provider.DataTableColumns;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A ColorRuleGroup aggregates a collection of {@link ColorRule} objects and is
 * responsible for looking through the list of rules to determine the color
 * dictated by the collection.
 *
 * @author sudar.sam@gmail.com
 */
public final class ColorRuleGroup {

  /**
   * Used for logging
   */
  private static final String TAG = ColorRuleGroup.class.getName();
  /*****************************
   * Things needed for the key value store.
   *****************************/
  private static final ObjectMapper mapper;
  private static final TypeFactory typeFactory;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibility(mapper.getVisibilityChecker().withFieldVisibility(Visibility.ANY));
    mapper.setVisibility(mapper.getVisibilityChecker().withCreatorVisibility(Visibility.ANY));
    typeFactory = mapper.getTypeFactory();
  }

  // private final KeyValueStoreHelper kvsh;
  // NOTE: the aspectHelper encodes the elementKey
  // private final KeyValueHelper aspectHelper;
  // This is the list of actual rules that make up the ruler.
  private List<ColorRule> ruleList;
  private Type mType;
  private String mAppName;
  private String mTableId;
  private String mElementKey;
  private String[] mAdminColumns;
  private boolean mDefault;

  /**
   * Construct the rule group for the given column.
   *
   * @param dbInterface  a database to use
   * @param appName      the app name
   * @param db           an opened database handle to use
   * @param tableId      the id of the table that the color rule will operate on
   * @param elementKey   the id of the column that the color rule will operate on
   * @param type         The type of color rule, column, status, etc..
   * @param adminColumns A list of the hidden columns in the table
   * @throws ServicesAvailabilityException if the database is down
   */
  private ColorRuleGroup(UserDbInterface dbInterface, String appName, DbHandle db, String tableId,
      String elementKey, Type type, String[] adminColumns) throws ServicesAvailabilityException {
    this.mType = type;
    this.mAppName = appName;
    this.mTableId = tableId;
    this.mElementKey = elementKey;
    mAdminColumns = adminColumns;
    List<KeyValueStoreEntry> entries = null;
    switch (mType) {
    case COLUMN:
      entries = dbInterface.getTableMetadata(appName, db, mTableId,
          LocalKeyValueStoreConstants.ColumnColorRules.PARTITION, elementKey,
          LocalKeyValueStoreConstants.ColumnColorRules.KEY_COLOR_RULES_COLUMN, null).getEntries();
      break;
    case TABLE:
      entries = dbInterface.getTableMetadata(appName, db, mTableId,
          LocalKeyValueStoreConstants.TableColorRules.PARTITION,
          KeyValueStoreConstants.ASPECT_DEFAULT,
          LocalKeyValueStoreConstants.TableColorRules.KEY_COLOR_RULES_ROW, null).getEntries();
      break;
    case STATUS_COLUMN:
      entries = dbInterface.getTableMetadata(appName, db, mTableId,
          LocalKeyValueStoreConstants.TableColorRules.PARTITION,
          KeyValueStoreConstants.ASPECT_DEFAULT,
          LocalKeyValueStoreConstants.TableColorRules.KEY_COLOR_RULES_STATUS_COLUMN, null)
          .getEntries();
      break;
    }
    mDefault = false;
    if (entries.size() != 1) {
      this.ruleList = new ArrayList<>();
      if (mType == Type.STATUS_COLUMN) {
        this.ruleList.addAll(ColorRuleUtil.getDefaultSyncStateColorRules());
      }
    } else {
      String jsonRulesString = KeyValueStoreUtils.getObject(entries.get(0));
      this.ruleList = parseJsonString(jsonRulesString);
    }
  }

  /**
   * Returns a new color rule group with the requested parameters
   * @param dbInterface  a database to use
   * @param appName      the app name
   * @param db           an opened database handle to use
   * @param tableId      the id of the table that the color rule will operate on
   * @param elementKey   the id of the column that the color rule will operate on
   * @param adminColumns A list of the hidden columns in the table
   *
   * @return A new color rule group for columns
   * @throws ServicesAvailabilityException if the database is down
   */
  public static ColorRuleGroup getColumnColorRuleGroup(UserDbInterface dbInterface, String appName,
      DbHandle db, String tableId, String elementKey, String[] adminColumns)
      throws ServicesAvailabilityException {
    return new ColorRuleGroup(dbInterface, appName, db, tableId, elementKey, Type.COLUMN,
        adminColumns);
  }

  /**
   * Returns a new color rule group with the requested parameters
   * @param dbInterface  a database to use
   * @param appName      the app name
   * @param db           an opened database handle to use
   * @param tableId      the id of the table that the color rule will operate on
   * @param adminColumns A list of the hidden columns in the table
   *
   * @return A new color rule group for tables
   * @throws ServicesAvailabilityException if the database is down
   */
  public static ColorRuleGroup getTableColorRuleGroup(UserDbInterface dbInterface, String appName,
      DbHandle db, String tableId, String[] adminColumns) throws ServicesAvailabilityException {
    return new ColorRuleGroup(dbInterface, appName, db, tableId, null, Type.TABLE, adminColumns);
  }

  /**
   * Returns a new color rule group with the requested parameters
   * @param dbInterface  a database to use
   * @param appName      the app name
   * @param db           an opened database handle to use
   * @param tableId      the id of the table that the color rule will operate on
   * @param adminColumns A list of the hidden columns in the table
   *
   * @return A new color rule group for a status column
   * @throws ServicesAvailabilityException if the database is down
   */
  public static ColorRuleGroup getStatusColumnRuleGroup(UserDbInterface dbInterface, String appName,
      DbHandle db, String tableId, String[] adminColumns) throws ServicesAvailabilityException {
    return new ColorRuleGroup(dbInterface, appName, db, tableId, null, Type.STATUS_COLUMN,
        adminColumns);
  }

  public String[] getAdminColumns() {
    return this.mAdminColumns;
  }

  /**
   * Parse a json String of a list of {@link ColorRule} objects into a
   *
   * @param json the json string to parse into a list of color rules
   * @return A list of the rules read from the file
   */
  private List<ColorRule> parseJsonString(String json) {
    List<ColorRule> reclaimedRules = new ArrayList<>();
    if (json == null || json.isEmpty()) { // no values in the kvs
      return reclaimedRules;
    }
    try {
      reclaimedRules = mapper
          .readValue(json, typeFactory.constructCollectionType(ArrayList.class, ColorRule.class));
    } catch (IOException e) {
      WebLogger.getLogger(mAppName).e(TAG, "parsing/IO problem with mapping json to color rules");
      WebLogger.getLogger(mAppName).printStackTrace(e);
    }
    return reclaimedRules;
  }

  /**
   * Return the list of rules that makes up this column. This should only be
   * used for displaying the rules. Any changes to the list should be made via
   * the add, delete, and update methods in ColumnColorRuler.
   *
   * @return the color rules in the color rule group
   */
  public List<ColorRule> getColorRules() {
    return ruleList;
  }

  /**
   * Replace the list of rules that define this ColumnColorRuler. Does so while
   * retaining the same reference as was originally held.
   *
   * @param newRules a new list of rules for the color rule group
   */
  public void replaceColorRuleList(List<ColorRule> newRules) {
    this.ruleList.clear();
    this.ruleList.addAll(newRules);
    mDefault = false;
  }

  /**
   * Get the type of the rule group.
   *
   * @return
   */
  public Type getType() {
    return mType;
  }

  /**
   * Persist the rule list into the key value store. Does nothing if there are
   * no rules, so will not pollute the key value store unless something has been
   * added.
   *
   * @param dbInterface a database to use
   * @throws ServicesAvailabilityException if the database is down
   */
  public void saveRuleList(UserDbInterface dbInterface) throws ServicesAvailabilityException {
    if (mDefault) {
      // nothing to save
      return;
    }
    DbHandle db = null;
    try {
      db = dbInterface.openDatabase(mAppName);
      // initialize the KVS helpers...

      // set it to this default just in case something goes wrong and it is
      // somehow set. this way if you manage to set the object you will have
      // something that doesn't throw an error when you expect to get back
      // an array list. it will just be of length 0. not sure if this is a good
      // idea or not.
      try {
        String ruleListJson = mapper.writeValueAsString(ruleList);
        KeyValueStoreEntry entry = null;
        switch (mType) {
        case COLUMN:
          entry = KeyValueStoreUtils
              .buildEntry(mTableId, LocalKeyValueStoreConstants.ColumnColorRules.PARTITION,
                  mElementKey, LocalKeyValueStoreConstants.ColumnColorRules.KEY_COLOR_RULES_COLUMN,
                  ElementDataType.array, ruleListJson);
          break;
        case TABLE:
          entry = KeyValueStoreUtils
              .buildEntry(mTableId, LocalKeyValueStoreConstants.TableColorRules.PARTITION,
                  KeyValueStoreConstants.ASPECT_DEFAULT,
                  LocalKeyValueStoreConstants.TableColorRules.KEY_COLOR_RULES_ROW,
                  ElementDataType.array, ruleListJson);
          break;
        case STATUS_COLUMN:
          entry = KeyValueStoreUtils
              .buildEntry(mTableId, LocalKeyValueStoreConstants.TableColorRules.PARTITION,
                  KeyValueStoreConstants.ASPECT_DEFAULT,
                  LocalKeyValueStoreConstants.TableColorRules.KEY_COLOR_RULES_STATUS_COLUMN,
                  ElementDataType.array, ruleListJson);
          break;
        }
        dbInterface.replaceTableMetadata(mAppName, db, entry);
      } catch (IOException e) {
        WebLogger.getLogger(mAppName).e(TAG, "IO or parsing problem parsing list of color rules");
        WebLogger.getLogger(mAppName).printStackTrace(e);
      }
    } finally {
      if (db != null) {
        dbInterface.closeDatabase(mAppName, db);
      }
    }
  }

  /**
   * Replace the rule matching updatedRule's id with updatedRule.
   *
   * @param updatedRule the rule to change in the internal list of rules
   */
  public void updateRule(ColorRule updatedRule) {
    for (int i = 0; i < ruleList.size(); i++) {
      if (ruleList.get(i).getRuleId().equals(updatedRule.getRuleId())) {
        ruleList.set(i, updatedRule);
        mDefault = false;
        return;
      }
    }
    WebLogger.getLogger(mAppName).e(TAG, "tried to update a rule that matched no saved ids");
  }

  /**
   * Remove the given rule from the rule list.
   *
   * @param rule the rule to be removed from the list.
   */
  public void removeRule(ColorRule rule) {
    for (int i = 0; i < ruleList.size(); i++) {
      if (ruleList.get(i).getRuleId().equals(rule.getRuleId())) {
        ruleList.remove(i);
        mDefault = false;
        return;
      }
    }
    WebLogger.getLogger(mAppName).d(TAG,
        "a rule was passed to deleteRule that did not match" + " the id of any rules in the list");
  }

  /**
   * Returns the number of rules in the group
   * @return the number of rules in the group
   */
  public int getRuleCount() {
    return ruleList.size();
  }

  /**
   * Use the rule group to determine if it applies to the given data.
   *
   * @param orderedDefns set of columnDefinitions for the table
   * @param row          the data from the row
   * @return null or the matching rule in the group, {@link ColorGuide}.
   */
  public ColorGuide getColorGuide(OrderedColumns orderedDefns, Row row) {
    for (int i = 0; i < ruleList.size(); i++) {
      ColorRule cr = ruleList.get(i);
      // First get the data about the column. It is possible that we are trying
      // to match a metadata column, in which case there will be no
      // ColumnProperties object. At this point all such metadata elementKeys
      // must not begin with an underscore, whereas all user defined columns
      // will, so we'll also try to do a helpful check in case this invariant
      // changes in the future.
      String elementKey = cr.getColumnElementKey();
      ColumnDefinition cd = null;
      try {
        cd = orderedDefns.find(cr.getColumnElementKey());
      } catch (Exception ignored) {
        // elementKey must be a metadata column...
      }
      ElementDataType type;
      if (cd == null) {
        // Was likely a metadata column.
        if (!Arrays.asList(mAdminColumns).contains(elementKey)) {
          throw new IllegalArgumentException(
              "element key passed to " + "ColorRule#checkMatch didn't have a mapping and was "
                  + "not a metadata elementKey: " + elementKey);
        }
        // if conflict_type then integer
        if (elementKey.equals(DataTableColumns.CONFLICT_TYPE)) {
          type = ElementDataType.integer;
        } else {
          type = ElementDataType.string;
        }
      } else {
        ElementType elementType = cd.getType();
        type = elementType.getDataType();
      }
      if (cr.checkMatch(type, row)) {
        return new ColorGuide(cr.getForeground(), cr.getBackground());
      }
    }
    return null;
  }

  /**
   * The three types of color rule groups
   */
  @SuppressWarnings("JavaDoc")
  public enum Type {
    COLUMN, TABLE, STATUS_COLUMN
  }

}
