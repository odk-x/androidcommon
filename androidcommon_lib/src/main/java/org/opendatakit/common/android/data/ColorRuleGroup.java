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
package org.opendatakit.common.android.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.common.android.application.CommonApplication;
import org.opendatakit.common.android.utilities.*;
import org.opendatakit.database.service.KeyValueStoreEntry;
import org.opendatakit.database.service.OdkDbHandle;

import android.os.RemoteException;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * A ColorRuleGroup aggregates a collection of {@link ColorRule} objects and is
 * responsible for looking through the list of rules to determine the color
 * dictated by the collection.
 * 
 * @author sudar.sam@gmail.com
 *
 */
public class ColorRuleGroup {

  private static final String TAG = ColorRuleGroup.class.getName();

  /*****************************
   * Things needed for the key value store.
   *****************************/
  public static final String DEFAULT_KEY_COLOR_RULES = "[]";

  private static final ObjectMapper mapper;
  private static final TypeFactory typeFactory;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(Visibility.ANY));
    mapper
        .setVisibilityChecker(mapper.getVisibilityChecker().withCreatorVisibility(Visibility.ANY));
    typeFactory = mapper.getTypeFactory();
  }

  public enum Type {
    COLUMN, TABLE, STATUS_COLUMN;
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
   * @param ctxt
   * @param appName
   * @param db
   * @param tableId
   * @param elementKey
   * @param type
   * @param adminColumns
   * @throws RemoteException
   */
  private ColorRuleGroup(CommonApplication ctxt, String appName, OdkDbHandle db, String tableId, String elementKey,
      Type type, String[] adminColumns) throws RemoteException {
    this.mType = type;
    this.mAppName = appName;
    this.mTableId = tableId;
    this.mElementKey = elementKey;
    String jsonRulesString = DEFAULT_KEY_COLOR_RULES;
    mAdminColumns = adminColumns;
    List<KeyValueStoreEntry> entries = null;
    switch (mType) {
    case COLUMN:
      entries = ctxt.getDatabase().getDBTableMetadata(appName, db, mTableId,
              LocalKeyValueStoreConstants.ColumnColorRules.PARTITION,
              elementKey,
              LocalKeyValueStoreConstants.ColumnColorRules.KEY_COLOR_RULES_COLUMN);
      break;
    case TABLE:
      entries = ctxt.getDatabase().getDBTableMetadata(appName, db, mTableId,
              LocalKeyValueStoreConstants.TableColorRules.PARTITION,
              KeyValueStoreConstants.ASPECT_DEFAULT,
              LocalKeyValueStoreConstants.TableColorRules.KEY_COLOR_RULES_ROW);
      break;
    case STATUS_COLUMN:
      entries = ctxt.getDatabase().getDBTableMetadata(appName, db, mTableId,
              LocalKeyValueStoreConstants.TableColorRules.PARTITION,
              KeyValueStoreConstants.ASPECT_DEFAULT,
              LocalKeyValueStoreConstants.TableColorRules.KEY_COLOR_RULES_STATUS_COLUMN);
      break;
    default:
      WebLogger.getLogger(mAppName).e(TAG, "unrecognized ColorRuleGroup type: " + mType);
    }
    mDefault = false;
    if ( entries.size() != 1 ) {
      if (mType == Type.STATUS_COLUMN) {
        this.ruleList = ColorRuleUtil.getDefaultSyncStateColorRules();
        mDefault = true;
      } else {
        this.ruleList = new ArrayList<ColorRule>();
      }
    } else {
      jsonRulesString = KeyValueStoreUtils.getObject(appName, entries.get(0));
      this.ruleList = parseJsonString(jsonRulesString);
    }
  }

  public String[] getAdminColumns() {
    return this.mAdminColumns;
  }

  public static ColorRuleGroup getColumnColorRuleGroup(CommonApplication ctxt, String appName, OdkDbHandle db, 
      String tableId, String elementKey, String[] adminColumns) throws RemoteException {
    return new ColorRuleGroup(ctxt, appName, db, tableId, elementKey, Type.COLUMN, adminColumns);
  }

  public static ColorRuleGroup getTableColorRuleGroup(CommonApplication ctxt, String appName, OdkDbHandle db, 
      String tableId, String[] adminColumns) throws RemoteException {
    return new ColorRuleGroup(ctxt, appName, db, tableId, null, Type.TABLE, adminColumns);
  }

  public static ColorRuleGroup getStatusColumnRuleGroup(CommonApplication ctxt, String appName, OdkDbHandle db,
      String tableId, String[] adminColumns) throws RemoteException {
    return new ColorRuleGroup(ctxt, appName, db, tableId, null, Type.STATUS_COLUMN, adminColumns);
  }

  /**
   * Parse a json String of a list of {@link ColorRule} objects into a
   *
   * @param json
   * @return
   */
  private List<ColorRule> parseJsonString(String json) {
    if (json == null || json.equals("")) { // no values in the kvs
      return new ArrayList<ColorRule>();
    }
    List<ColorRule> reclaimedRules = new ArrayList<ColorRule>();
    try {
      reclaimedRules = mapper.readValue(json,
          typeFactory.constructCollectionType(ArrayList.class, ColorRule.class));
    } catch (JsonParseException e) {
      WebLogger.getLogger(mAppName).e(TAG, "problem parsing json to colcolorrule");
      WebLogger.getLogger(mAppName).printStackTrace(e);
    } catch (JsonMappingException e) {
      WebLogger.getLogger(mAppName).e(TAG, "problem mapping json to colcolorrule");
      WebLogger.getLogger(mAppName).printStackTrace(e);
    } catch (IOException e) {
      WebLogger.getLogger(mAppName).e(TAG, "i/o problem with json to colcolorrule");
      WebLogger.getLogger(mAppName).printStackTrace(e);
    }
    return reclaimedRules;
  }

  /**
   * Return the list of rules that makes up this column. This should only be
   * used for displaying the rules. Any changes to the list should be made via
   * the add, delete, and update methods in ColumnColorRuler.
   * 
   * @return
   */
  public List<ColorRule> getColorRules() {
    return ruleList;
  }

  /**
   * Replace the list of rules that define this ColumnColorRuler. Does so while
   * retaining the same reference as was originally held.
   * 
   * @param newRules
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
   * @throws RemoteException
   */
  public void saveRuleList(CommonApplication ctxt) throws RemoteException {
    if ( mDefault ) {
      // nothing to save
      return;
    }
    OdkDbHandle db = null;
    try {
      db = ctxt.getDatabase().openDatabase(mAppName);
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
          entry = KeyValueStoreUtils.buildEntry(mTableId, LocalKeyValueStoreConstants.ColumnColorRules.PARTITION,
                  mElementKey,
                  LocalKeyValueStoreConstants.ColumnColorRules.KEY_COLOR_RULES_COLUMN,
                  ElementDataType.array, ruleListJson);
          break;
        case TABLE:
          entry = KeyValueStoreUtils.buildEntry(mTableId, LocalKeyValueStoreConstants.TableColorRules.PARTITION,
                  KeyValueStoreConstants.ASPECT_DEFAULT,
                  LocalKeyValueStoreConstants.TableColorRules.KEY_COLOR_RULES_ROW,
                  ElementDataType.array, ruleListJson);
          break;
        case STATUS_COLUMN:
          entry = KeyValueStoreUtils.buildEntry(mTableId, LocalKeyValueStoreConstants.TableColorRules.PARTITION,
                  KeyValueStoreConstants.ASPECT_DEFAULT,
                  LocalKeyValueStoreConstants.TableColorRules.KEY_COLOR_RULES_STATUS_COLUMN,
                  ElementDataType.array, ruleListJson);
          break;
        }
        ctxt.getDatabase().replaceDBTableMetadata(mAppName, db, entry);
      } catch (JsonGenerationException e) {
        WebLogger.getLogger(mAppName).e(TAG, "problem parsing list of color rules");
        WebLogger.getLogger(mAppName).printStackTrace(e);
      } catch (JsonMappingException e) {
        WebLogger.getLogger(mAppName).e(TAG, "problem mapping list of color rules");
        WebLogger.getLogger(mAppName).printStackTrace(e);
      } catch (IOException e) {
        WebLogger.getLogger(mAppName).e(TAG, "i/o problem with json list of color rules");
        WebLogger.getLogger(mAppName).printStackTrace(e);
      }
    } finally {
      if (db != null) {
        ctxt.getDatabase().closeDatabase(mAppName, db);
      }
    }
  }

  /**
   * Replace the rule matching updatedRule's id with updatedRule.
   * 
   * @param updatedRule
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
   * @param rule
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

  public int getRuleCount() {
    return ruleList.size();
  }

  /**
   * Use the rule group to determine if it applies to the given data.
   *
   * @param orderedDefns set of columnDefinitions for the table
   * @param row the data from the row
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
      } catch (Exception e) {
        // elementKey must be a metadata column...
      }
      ElementDataType type;
      if (cd == null) {
        // Was likely a metadata column.
        if (!Arrays.asList(mAdminColumns).contains(elementKey)) {
          throw new IllegalArgumentException("element key passed to "
              + "ColorRule#checkMatch didn't have a mapping and was "
              + "not a metadata elementKey: " + elementKey);
        }
        type = ElementDataType.string;
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

}
