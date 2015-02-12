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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.RFC4180CsvReader;
import org.opendatakit.aggregate.odktables.rest.RFC4180CsvWriter;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.KeyValueStoreEntry;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.UserTable.Row;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;

import android.content.ContentValues;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;

/**
 * Various utilities for importing/exporting tables from/to CSV.
 *
 * @author sudar.sam@gmail.com
 * @author unknown
 *
 */
public class CsvUtil {

  public interface ExportListener {

    public void exportComplete(boolean outcome);
  };

  public interface ImportListener {
    public void updateProgressDetail(String progressDetailString);

    public void importComplete(boolean outcome);
  }

  private static final String TAG = CsvUtil.class.getSimpleName();

  private final Context context;
  private final String appName;

  public CsvUtil(Context context, String appName) {
    this.context = context;
    this.appName = appName;
  }

  // ===========================================================================================
  // EXPORT
  // ===========================================================================================

  /**
   * Export the given tableId. Exports three csv files to the output/csv
   * directory under the appName:
   * <ul>
   * <li>tableid.fileQualifier.csv - data table</li>
   * <li>tableid.fileQualifier.definition.csv - data table column definition</li>
   * <li>tableid.fileQualifier.properties.csv - key-value store of this table</li>
   * </ul>
   * If fileQualifier is null or an empty string, then it emits to
   * <ul>
   * <li>tableid.csv - data table</li>
   * <li>tableid.definition.csv - data table column definition</li>
   * <li>tableid.properties.csv - key-value store of this table</li>
   * </ul>
   *
   * @param exportListener
   * @param tp
   * @param fileQualifier
   * @return
   */
  public boolean exportSeparable(ExportListener exportListener, SQLiteDatabase db, String tableId,
      ArrayList<ColumnDefinition> orderedDefns, String fileQualifier) {
    // building array of columns to select and header row for output file
    // then we are including all the metadata columns.
    ArrayList<String> columns = new ArrayList<String>();

    WebLogger.getLogger(appName).i(
        TAG,
        "exportSeparable: tableId: " + tableId + " fileQualifier: "
            + ((fileQualifier == null) ? "<null>" : fileQualifier));

    // put the user-relevant metadata columns in leftmost columns
    columns.add(DataTableColumns.ID);
    columns.add(DataTableColumns.FORM_ID);
    columns.add(DataTableColumns.LOCALE);
    columns.add(DataTableColumns.SAVEPOINT_TYPE);
    columns.add(DataTableColumns.SAVEPOINT_TIMESTAMP);
    columns.add(DataTableColumns.SAVEPOINT_CREATOR);

    // add the data columns
    for (ColumnDefinition cd : orderedDefns) {
      if (cd.isUnitOfRetention()) {
        columns.add(cd.getElementKey());
      }
    }

    // And now add all remaining export columns
    for (String colName : ODKDatabaseUtils.get().getExportColumns()) {
      if (columns.contains(colName)) {
        continue;
      }
      columns.add(colName);
    }

    OutputStreamWriter output = null;
    try {
      // both files go under the output/csv directory...
      File outputCsv = new File(ODKFileUtils.getOutputTableCsvFile(appName, tableId, fileQualifier));
      outputCsv.mkdirs();

      // emit properties files
      File definitionCsv = new File(ODKFileUtils.getOutputTableDefinitionCsvFile(appName, tableId,
          fileQualifier));
      File propertiesCsv = new File(ODKFileUtils.getOutputTablePropertiesCsvFile(appName, tableId,
          fileQualifier));

      if (!writePropertiesCsv(db, tableId, orderedDefns, definitionCsv, propertiesCsv)) {
        return false;
      }

      // getting data
      String whereString = DataTableColumns.SAVEPOINT_TYPE + " IS NOT NULL AND ("
          + DataTableColumns.CONFLICT_TYPE + " IS NULL OR " + DataTableColumns.CONFLICT_TYPE
          + " = " + Integer.toString(ConflictType.LOCAL_UPDATED_UPDATED_VALUES) + ")";

      UserTable table = ODKDatabaseUtils.get().rawSqlQuery(db, appName, tableId, orderedDefns,
          whereString, null, null, null, null, null);

      // emit data table...
      File file = new File(outputCsv, tableId
          + ((fileQualifier != null && fileQualifier.length() != 0) ? ("." + fileQualifier) : "")
          + ".csv");
      FileOutputStream out = new FileOutputStream(file);
      output = new OutputStreamWriter(out, CharEncoding.UTF_8);
      RFC4180CsvWriter cw = new RFC4180CsvWriter(output);
      // don't have to worry about quotes in elementKeys...
      cw.writeNext(columns.toArray(new String[columns.size()]));
      String[] row = new String[columns.size()];
      for (int i = 0; i < table.getNumberOfRows(); i++) {
        Row dataRow = table.getRowAtIndex(i);
        for (int j = 0; j < columns.size(); ++j) {
          row[j] = dataRow.getRawDataOrMetadataByElementKey(columns.get(j));
          ;
        }
        cw.writeNext(row);
      }
      cw.flush();
      cw.close();

      return true;
    } catch (IOException e) {
      return false;
    } finally {
      try {
        output.close();
      } catch (IOException e) {
      }
    }
  }

  /**
   * Writes the definition and properties files for the given tableId. This is
   * written to:
   * <ul>
   * <li>tables/tableId/definition.csv - data table column definition</li>
   * <li>tables/tableId/properties.csv - key-value store of this table</li>
   * </ul>
   * The definition.csv file contains the schema definition. md5hash of it
   * corresponds to the former schemaETag.
   *
   * The properties.csv file contains the table-level metadata (key-value
   * store). The md5hash of it corresponds to the propertiesETag.
   *
   * For use by the sync mechanism.
   *
   * @param tp
   * @return
   */
  public boolean writePropertiesCsv(SQLiteDatabase db, String tableId,
      ArrayList<ColumnDefinition> orderedDefns) {
    File definitionCsv = new File(ODKFileUtils.getTableDefinitionCsvFile(appName, tableId));
    File propertiesCsv = new File(ODKFileUtils.getTablePropertiesCsvFile(appName, tableId));
    return writePropertiesCsv(db, tableId, orderedDefns, definitionCsv, propertiesCsv);
  }

  /**
   * Common routine to write the definition and properties files.
   *
   * @param tp
   * @param definitionCsv
   * @param propertiesCsv
   * @return
   */
  private boolean writePropertiesCsv(SQLiteDatabase db, String tableId,
      ArrayList<ColumnDefinition> orderedDefns, File definitionCsv, File propertiesCsv) {
    WebLogger.getLogger(appName).i(TAG, "writePropertiesCsv: tableId: " + tableId);

    // writing metadata
    FileOutputStream out;
    RFC4180CsvWriter cw;
    OutputStreamWriter output = null;
    try {
      // emit definition.csv table...
      out = new FileOutputStream(definitionCsv);
      output = new OutputStreamWriter(out, CharEncoding.UTF_8);
      cw = new RFC4180CsvWriter(output);

      // Emit ColumnDefinitions

      ArrayList<String> colDefHeaders = new ArrayList<String>();
      colDefHeaders.add(ColumnDefinitionsColumns.ELEMENT_KEY);
      colDefHeaders.add(ColumnDefinitionsColumns.ELEMENT_NAME);
      colDefHeaders.add(ColumnDefinitionsColumns.ELEMENT_TYPE);
      colDefHeaders.add(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS);

      cw.writeNext(colDefHeaders.toArray(new String[colDefHeaders.size()]));
      String[] colDefRow = new String[colDefHeaders.size()];

      /**
       * Since the md5Hash of the file identifies identical schemas, ensure that
       * the list of columns is in alphabetical order.
       */
      for (ColumnDefinition cd : orderedDefns) {
        colDefRow[0] = cd.getElementKey();
        colDefRow[1] = cd.getElementName();
        colDefRow[2] = cd.getElementType();
        colDefRow[3] = cd.getListChildElementKeys();
        cw.writeNext(colDefRow);
      }

      cw.flush();
      cw.close();

      // emit properties.csv...
      out = new FileOutputStream(propertiesCsv);
      output = new OutputStreamWriter(out, CharEncoding.UTF_8);
      cw = new RFC4180CsvWriter(output);

      // Emit KeyValueStore

      ArrayList<String> kvsHeaders = new ArrayList<String>();
      kvsHeaders.add(KeyValueStoreColumns.PARTITION);
      kvsHeaders.add(KeyValueStoreColumns.ASPECT);
      kvsHeaders.add(KeyValueStoreColumns.KEY);
      kvsHeaders.add(KeyValueStoreColumns.VALUE_TYPE);
      kvsHeaders.add(KeyValueStoreColumns.VALUE);

      /**
       * Since the md5Hash of the file identifies identical properties, ensure
       * that the list of KVS entries is in alphabetical order.
       */
      List<KeyValueStoreEntry> kvsEntries = ODKDatabaseUtils.get().getDBTableMetadata(db, tableId,
          null, null, null);
      Collections.sort(kvsEntries, new Comparator<KeyValueStoreEntry>() {

        @Override
        public int compare(KeyValueStoreEntry lhs, KeyValueStoreEntry rhs) {
          int outcome;
          if (lhs.partition == null && rhs.partition == null) {
            outcome = 0;
          } else if (lhs.partition == null) {
            return -1;
          } else if (rhs.partition == null) {
            return 1;
          } else {
            outcome = lhs.partition.compareTo(rhs.partition);
          }
          if (outcome != 0)
            return outcome;
          if (lhs.aspect == null && rhs.aspect == null) {
            outcome = 0;
          } else if (lhs.aspect == null) {
            return -1;
          } else if (rhs.aspect == null) {
            return 1;
          } else {
            outcome = lhs.aspect.compareTo(rhs.aspect);
          }
          if (outcome != 0)
            return outcome;
          if (lhs.key == null && rhs.key == null) {
            outcome = 0;
          } else if (lhs.key == null) {
            return -1;
          } else if (rhs.key == null) {
            return 1;
          } else {
            outcome = lhs.key.compareTo(rhs.key);
          }
          return outcome;
        }
      });

      cw.writeNext(kvsHeaders.toArray(new String[kvsHeaders.size()]));
      String[] kvsRow = new String[kvsHeaders.size()];
      for (int i = 0; i < kvsEntries.size(); i++) {
        KeyValueStoreEntry entry = kvsEntries.get(i);
        kvsRow[0] = entry.partition;
        kvsRow[1] = entry.aspect;
        kvsRow[2] = entry.key;
        kvsRow[3] = entry.type;
        kvsRow[4] = entry.value;
        cw.writeNext(kvsRow);
      }
      cw.flush();
      cw.close();

      return true;
    } catch (IOException e) {
      return false;
    } finally {
      try {
        output.close();
      } catch (IOException e) {
      }
    }
  }

  private int countUpToLastNonNullElement(String[] row) {
    for (int i = row.length - 1; i >= 0; --i) {
      if (row[i] != null) {
        return (i + 1);
      }
    }
    return 0;
  }

  /**
   * Update tableId from
   * <ul>
   * <li>tables/tableId/properties.csv</li>
   * <li>tables/tableId/definition.csv</li>
   * </ul>
   *
   * This will either create a table, or verify that the table structure matches
   * that defined in the csv. It will then override all the KVS entries with
   * those present in the file.
   *
   * @param importListener
   * @param tableId
   * @throws IOException
   */
  public void updateTablePropertiesFromCsv(ImportListener importListener, String tableId)
      throws IOException {

    WebLogger.getLogger(appName).i(TAG, "updateTablePropertiesFromCsv: tableId: " + tableId);

    SQLiteDatabase db = null;
    try {
      db = DatabaseFactory.get().getDatabase(context, appName);
      List<Column> columns = new ArrayList<Column>();

      // reading data
      File file = null;
      FileInputStream in = null;
      InputStreamReader input = null;
      RFC4180CsvReader cr = null;
      try {
        file = new File(ODKFileUtils.getTableDefinitionCsvFile(appName, tableId));
        in = new FileInputStream(file);
        input = new InputStreamReader(in, CharEncoding.UTF_8);
        cr = new RFC4180CsvReader(input);

        String[] row;

        // Read ColumnDefinitions
        // get the column headers
        String[] colHeaders = cr.readNext();
        int colHeadersLength = countUpToLastNonNullElement(colHeaders);
        // get the first row
        row = cr.readNext();
        while (row != null && countUpToLastNonNullElement(row) != 0) {

          String elementKeyStr = null;
          String elementNameStr = null;
          String elementTypeStr = null;
          String listChildElementKeysStr = null;
          int rowLength = countUpToLastNonNullElement(row);
          for (int i = 0; i < rowLength; ++i) {
            if (i >= colHeadersLength) {
              throw new IllegalStateException("data beyond header row of ColumnDefinitions table");
            }
            if (ColumnDefinitionsColumns.ELEMENT_KEY.equals(colHeaders[i])) {
              elementKeyStr = row[i];
            }
            if (ColumnDefinitionsColumns.ELEMENT_NAME.equals(colHeaders[i])) {
              elementNameStr = row[i];
            }
            if (ColumnDefinitionsColumns.ELEMENT_TYPE.equals(colHeaders[i])) {
              elementTypeStr = row[i];
            }
            if (ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS.equals(colHeaders[i])) {
              listChildElementKeysStr = row[i];
            }
          }

          if (elementKeyStr == null || elementTypeStr == null) {
            throw new IllegalStateException("ElementKey and ElementType must be specified");
          }

          columns.add(new Column(elementKeyStr, elementNameStr, elementTypeStr,
              listChildElementKeysStr));

          // get next row or blank to end...
          row = cr.readNext();
        }

        cr.close();
        try {
          input.close();
        } catch (IOException e) {
        }
        try {
          in.close();
        } catch (IOException e) {
        }

        ArrayList<ColumnDefinition> colDefns = ColumnDefinition.buildColumnDefinitions(appName,
            tableId, columns);
        Map<String, List<KeyValueStoreEntry>> colEntries = new TreeMap<String, List<KeyValueStoreEntry>>();

        file = new File(ODKFileUtils.getTablePropertiesCsvFile(appName, tableId));
        in = new FileInputStream(file);
        input = new InputStreamReader(in, CharEncoding.UTF_8);
        cr = new RFC4180CsvReader(input);
        // Read KeyValueStore
        // read the column headers
        String[] kvsHeaders = cr.readNext();
        int kvsHeadersLength = countUpToLastNonNullElement(kvsHeaders);
        String displayName = null;
        List<KeyValueStoreEntry> kvsEntries = new ArrayList<KeyValueStoreEntry>();
        // read the first row
        row = cr.readNext();
        while (row != null && countUpToLastNonNullElement(row) != 0) {
          KeyValueStoreEntry kvsEntry = new KeyValueStoreEntry();
          kvsEntry.tableId = tableId;
          int rowLength = countUpToLastNonNullElement(row);
          for (int i = 0; i < rowLength; ++i) {
            if (KeyValueStoreColumns.PARTITION.equals(kvsHeaders[i])) {
              kvsEntry.partition = row[i];
            }
            if (KeyValueStoreColumns.ASPECT.equals(kvsHeaders[i])) {
              kvsEntry.aspect = row[i];
            }
            if (KeyValueStoreColumns.KEY.equals(kvsHeaders[i])) {
              kvsEntry.key = row[i];
            }
            if (KeyValueStoreColumns.VALUE_TYPE.equals(kvsHeaders[i])) {
              kvsEntry.type = row[i];
            }
            if (KeyValueStoreColumns.VALUE.equals(kvsHeaders[i])) {
              kvsEntry.value = row[i];
            }
          }
          if (KeyValueStoreConstants.PARTITION_COLUMN.equals(kvsEntry.partition)) {
            // column-specific
            String column = kvsEntry.aspect;
            List<KeyValueStoreEntry> kvList = colEntries.get(column);
            if (kvList == null) {
              kvList = new ArrayList<KeyValueStoreEntry>();
              colEntries.put(column, kvList);
            }
            try {
              ColumnDefinition.find(colDefns, column);
            } catch (IllegalArgumentException e) {
              throw new IllegalStateException("Reference to non-existent column: " + column
                  + " of tableId: " + tableId);
            }
            kvList.add(kvsEntry);
          } else {
            // not column-specific
            // see if we can find the displayName
            if (KeyValueStoreConstants.PARTITION_TABLE.equals(kvsEntry.partition)
                && KeyValueStoreConstants.ASPECT_DEFAULT.equals(kvsEntry.aspect)
                && KeyValueStoreConstants.TABLE_DISPLAY_NAME.equals(kvsEntry.key)) {
              displayName = kvsEntry.value;
            }
            // still put it in the kvsEntries -- displayName is not stored???
            kvsEntries.add(kvsEntry);
          }
          // get next row or blank to end...
          row = cr.readNext();
        }
        cr.close();
        try {
          input.close();
        } catch (IOException e) {
        }
        try {
          in.close();
        } catch (IOException e) {
        }

        if (ODKDatabaseUtils.get().hasTableId(db, tableId)) {
          ArrayList<ColumnDefinition> existingDefns = TableUtil.get().getColumnDefinitions(db,
              appName, tableId);

          // confirm that the column definitions are unchanged...
          if (existingDefns.size() != colDefns.size()) {
            throw new IllegalStateException(
                "Unexpectedly found tableId with different column definitions that already exists!");
          }
          for (ColumnDefinition ci : colDefns) {
            ColumnDefinition existingDefn;
            try {
              existingDefn = ColumnDefinition.find(existingDefns, ci.getElementKey());
            } catch (IllegalArgumentException e) {
              throw new IllegalStateException("Unexpectedly failed to match elementKey: "
                  + ci.getElementKey());
            }
            if (!existingDefn.getElementName().equals(ci.getElementName())) {
              throw new IllegalStateException(
                  "Unexpected mis-match of elementName for elementKey: " + ci.getElementKey());
            }
            List<ColumnDefinition> refList = existingDefn.getChildren();
            List<ColumnDefinition> ciList = ci.getChildren();
            if (refList.size() != ciList.size()) {
              throw new IllegalStateException(
                  "Unexpected mis-match of listOfStringElementKeys for elementKey: "
                      + ci.getElementKey());
            }
            for (int i = 0; i < ciList.size(); ++i) {
              if (!refList.contains(ciList.get(i))) {
                throw new IllegalStateException("Unexpected mis-match of listOfStringElementKeys["
                    + i + "] for elementKey: " + ci.getElementKey());
              }
            }
            ElementType type = ci.getType();
            ElementType existingType = existingDefn.getType();
            if (!existingType.equals(type)) {
              throw new IllegalStateException(
                  "Unexpected mis-match of elementType for elementKey: " + ci.getElementKey());
            }
          }
          // OK -- we have matching table definition
          // now just clear and update the properties...

          try {
            db.beginTransaction();
            ODKDatabaseUtils.get().replaceDBTableMetadata(db, tableId, kvsEntries, true);

            for (ColumnDefinition ci : colDefns) {
              // put the displayName into the KVS
              List<KeyValueStoreEntry> kvsList = colEntries.get(ci.getElementKey());
              if (kvsList == null) {
                kvsList = new ArrayList<KeyValueStoreEntry>();
                colEntries.put(ci.getElementKey(), kvsList);
              }
              KeyValueStoreEntry entry = null;
              for (KeyValueStoreEntry e : kvsList) {
                if (e.partition.equals(KeyValueStoreConstants.PARTITION_COLUMN)
                    && e.aspect.equals(ci.getElementKey())
                    && e.key.equals(KeyValueStoreConstants.COLUMN_DISPLAY_NAME)) {
                  entry = e;
                  break;
                }
              }

              if (entry != null && (entry.value == null || entry.value.trim().length() == 0)) {
                kvsList.remove(entry);
                entry = null;
              }

              if (entry == null) {
                entry = new KeyValueStoreEntry();
                entry.tableId = tableId;
                entry.partition = KeyValueStoreConstants.PARTITION_COLUMN;
                entry.aspect = ci.getElementKey();
                entry.key = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
                entry.type = ElementDataType.object.name();
                entry.value = ODKFileUtils.mapper.writeValueAsString(ci.getElementKey());
                kvsList.add(entry);
              }
              ODKDatabaseUtils.get().replaceDBTableMetadata(db, tableId, kvsList, false);
            }
            db.setTransactionSuccessful();
          } finally {
            db.endTransaction();
          }
        } else {

          for (ColumnDefinition ci : colDefns) {
            // put the displayName into the KVS if not supplied
            List<KeyValueStoreEntry> kvsList = colEntries.get(ci.getElementKey());
            if (kvsList == null) {
              kvsList = new ArrayList<KeyValueStoreEntry>();
              colEntries.put(ci.getElementKey(), kvsList);
            }
            KeyValueStoreEntry entry = null;
            for (KeyValueStoreEntry e : kvsList) {
              if (e.partition.equals(KeyValueStoreConstants.PARTITION_COLUMN)
                  && e.aspect.equals(ci.getElementKey())
                  && e.key.equals(KeyValueStoreConstants.COLUMN_DISPLAY_NAME)) {
                entry = e;
                break;
              }
            }

            if (entry != null && (entry.value == null || entry.value.trim().length() == 0)) {
              kvsList.remove(entry);
              entry = null;
            }

            if (entry == null) {
              entry = new KeyValueStoreEntry();
              entry.tableId = tableId;
              entry.partition = KeyValueStoreConstants.PARTITION_COLUMN;
              entry.aspect = ci.getElementKey();
              entry.key = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
              entry.type = ElementDataType.object.name();
              entry.value = ODKFileUtils.mapper.writeValueAsString(ci.getElementKey());
              kvsList.add(entry);
            }
          }

          // ensure there is a display name for the table...
          KeyValueStoreEntry e = new KeyValueStoreEntry();
          e.tableId = tableId;
          e.partition = KeyValueStoreConstants.PARTITION_TABLE;
          e.aspect = KeyValueStoreConstants.ASPECT_DEFAULT;
          e.key = KeyValueStoreConstants.TABLE_DISPLAY_NAME;
          e.type = ElementDataType.object.name();
          e.value = NameUtil.normalizeDisplayName((displayName == null ? NameUtil
              .constructSimpleDisplayName(tableId) : displayName));
          kvsEntries.add(e);

          try {
            db.beginTransaction();
            ODKDatabaseUtils.get().createOrOpenDBTableWithColumns(db, appName, tableId, columns);

            ODKDatabaseUtils.get().replaceDBTableMetadata(db, tableId, kvsEntries, false);

            // we have created the table...
            for (ColumnDefinition ci : colDefns) {
              List<KeyValueStoreEntry> kvsList = colEntries.get(ci.getElementKey());
              ODKDatabaseUtils.get().replaceDBTableMetadata(db, tableId, kvsList, false);
            }
            db.setTransactionSuccessful();
          } finally {
            db.endTransaction();
          }
        }
      } finally {
        try {
          if (input != null) {
            input.close();
          }
        } catch (IOException e) {
        }
      }

      // And update the inserted properties so that
      // the known entries have their expected types.
      try {
        db.beginTransaction();

        ODKDatabaseUtils.get().enforceTypesDBTableMetadata(db, tableId);

        db.setTransactionSuccessful();
      } finally {
        db.endTransaction();
      }
    } finally {
      if (db != null) {
        db.close();
      }
    }
  }

  /**
   * Imports data from a csv file with elementKey headings. This csv file is
   * assumed to be under:
   * <ul>
   * <li>assets/csv/tableId.fileQualifier.csv</li>
   * </ul>
   * If the table does not exist, it attempts to create it using the schema and
   * metadata located here:
   * <ul>
   * <li>tables/tableId/definition.csv - data table definition</li>
   * <li>tables/tableId/properties.csv - key-value store</li>
   * </ul>
   *
   * @param importListener
   * @param tableId
   * @param fileQualifier
   * @param createIfNotPresent
   *          -- true if we should try to create the table.
   * @return
   */
  public boolean importSeparable(ImportListener importListener, String tableId,
      String fileQualifier, boolean createIfNotPresent) {

    SQLiteDatabase db = null;
    try {
      db = DatabaseFactory.get().getDatabase(context, appName);
      if (!ODKDatabaseUtils.get().hasTableId(db, tableId)) {
        if (createIfNotPresent) {
          updateTablePropertiesFromCsv(importListener, tableId);
          if (!ODKDatabaseUtils.get().hasTableId(db, tableId)) {
            return false;
          }
        } else {
          return false;
        }
      }

      ArrayList<ColumnDefinition> orderedDefns = TableUtil.get().getColumnDefinitions(db, appName,
          tableId);

      WebLogger.getLogger(appName).i(
          TAG,
          "importSeparable: tableId: " + tableId + " fileQualifier: "
              + ((fileQualifier == null) ? "<null>" : fileQualifier));

      // reading data
      InputStreamReader input = null;
      try {
        // both files are read from assets/csv directory...
        File assetsCsv = new File(new File(ODKFileUtils.getAssetsFolder(appName)), "csv");

        // read data table...
        File file = new File(assetsCsv, tableId
            + ((fileQualifier != null && fileQualifier.length() != 0) ? ("." + fileQualifier) : "")
            + ".csv");
        FileInputStream in = new FileInputStream(file);
        input = new InputStreamReader(in, CharEncoding.UTF_8);
        RFC4180CsvReader cr = new RFC4180CsvReader(input);
        // don't have to worry about quotes in elementKeys...
        String[] columnsInFile = cr.readNext();
        int columnsInFileLength = countUpToLastNonNullElement(columnsInFile);

        String v_id;
        String v_form_id;
        String v_locale;
        String v_savepoint_type;
        String v_savepoint_creator;
        String v_savepoint_timestamp;
        String v_row_etag;
        String v_filter_type;
        String v_filter_value;

        Map<String, String> valueMap = new HashMap<String, String>();

        int rowCount = 0;
        String[] row;
        for (;;) {
          row = cr.readNext();
          rowCount++;
          if (rowCount % 5 == 0) {
            importListener.updateProgressDetail("Row " + rowCount);
          }
          if (row == null || countUpToLastNonNullElement(row) == 0) {
            break;
          }
          int rowLength = countUpToLastNonNullElement(row);

          // default values for metadata columns if not provided
          v_id = UUID.randomUUID().toString();
          v_form_id = null;
          v_locale = ODKDatabaseUtils.DEFAULT_LOCALE;
          v_savepoint_type = SavepointTypeManipulator.complete();
          v_savepoint_creator = ODKDatabaseUtils.DEFAULT_CREATOR;
          v_savepoint_timestamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
          v_row_etag = null;
          v_filter_type = null;
          v_filter_value = null;
          // clear value map
          valueMap.clear();

          boolean foundId = false;
          for (int i = 0; i < columnsInFileLength; ++i) {
            if (i > rowLength)
              break;
            String column = columnsInFile[i];
            String tmp = row[i];
            if (DataTableColumns.ID.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                foundId = true;
                v_id = tmp;
              }
              continue;
            }
            if (DataTableColumns.FORM_ID.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_form_id = tmp;
              }
              continue;
            }
            if (DataTableColumns.LOCALE.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_locale = tmp;
              }
              continue;
            }
            if (DataTableColumns.SAVEPOINT_TYPE.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_savepoint_type = tmp;
              }
              continue;
            }
            if (DataTableColumns.SAVEPOINT_CREATOR.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_savepoint_creator = tmp;
              }
              continue;
            }
            if (DataTableColumns.SAVEPOINT_TIMESTAMP.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_savepoint_timestamp = tmp;
              }
              continue;
            }
            if (DataTableColumns.ROW_ETAG.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_row_etag = tmp;
              }
              continue;
            }
            if (DataTableColumns.FILTER_TYPE.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_filter_type = tmp;
              }
              continue;
            }
            if (DataTableColumns.FILTER_VALUE.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_filter_value = tmp;
              }
              continue;
            }
            try {
              ColumnDefinition.find(orderedDefns, column);
              valueMap.put(column, tmp);
            } catch (IllegalArgumentException e) {
              // this is OK --
              // the csv contains an extra column
            }
          }

          // TODO: should resolve this properly when we have conflict rows and
          // uncommitted edits. For now, we just add our csv import to those,
          // rather
          // than resolve the problems.
          UserTable table = ODKDatabaseUtils.get().getDataInExistingDBTableWithId(db, appName,
              tableId, orderedDefns, v_id);
          if (table.getNumberOfRows() > 1) {
            throw new IllegalStateException(
                "There are either checkpoint or conflict rows in the destination table");
          }

          SyncState syncState = null;
          if (foundId && table.getNumberOfRows() == 1) {
            String syncStateStr = table.getRowAtIndex(0).getRawDataOrMetadataByElementKey(
                DataTableColumns.SYNC_STATE);
            if (syncStateStr == null) {
              throw new IllegalStateException("Unexpected null syncState value");
            }
            syncState = SyncState.valueOf(syncStateStr);
          }
          /**
           * Insertion will set the SYNC_STATE to new_row.
           *
           * If the table is sync'd to the server, this will cause one sync
           * interaction with the server to confirm that the server also has
           * this record.
           *
           * If a record with this same rowId already exists, if it is in an
           * new_row sync state, we update it here. Otherwise, if there were any
           * local changes, we leave the row unchanged.
           */
          if (syncState != null) {

            ContentValues cv = new ContentValues();
            if (v_id != null) {
              cv.put(DataTableColumns.ID, v_id);
            }
            for (String column : valueMap.keySet()) {
              if (column != null) {
                cv.put(column, valueMap.get(column));
              }
            }

            // The admin columns get added here
            cv.put(DataTableColumns.FORM_ID, v_form_id);
            cv.put(DataTableColumns.LOCALE, v_locale);
            cv.put(DataTableColumns.SAVEPOINT_TYPE, v_savepoint_type);
            cv.put(DataTableColumns.SAVEPOINT_TIMESTAMP, v_savepoint_timestamp);
            cv.put(DataTableColumns.SAVEPOINT_CREATOR, v_savepoint_creator);
            cv.put(DataTableColumns.ROW_ETAG, v_row_etag);
            cv.put(DataTableColumns.FILTER_TYPE, v_filter_type);
            cv.put(DataTableColumns.FILTER_VALUE, v_filter_value);

            cv.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());

            if (syncState == SyncState.new_row) {
              // we do the actual update here
              ODKDatabaseUtils.get().updateDataInExistingDBTableWithId(db, tableId, orderedDefns,
                  cv, v_id);
            }
            // otherwise, do NOT update the row.

          } else {
            ContentValues cv = new ContentValues();
            for (String column : valueMap.keySet()) {
              if (column != null) {
                cv.put(column, valueMap.get(column));
              }
            }

            if (v_id == null) {
              v_id = ODKDataUtils.genUUID();
            }

            // The admin columns get added here
            cv.put(DataTableColumns.FORM_ID, v_form_id);
            cv.put(DataTableColumns.LOCALE, v_locale);
            cv.put(DataTableColumns.SAVEPOINT_TYPE, v_savepoint_type);
            cv.put(DataTableColumns.SAVEPOINT_TIMESTAMP, v_savepoint_timestamp);
            cv.put(DataTableColumns.SAVEPOINT_CREATOR, v_savepoint_creator);
            cv.put(DataTableColumns.ROW_ETAG, v_row_etag);
            cv.put(DataTableColumns.FILTER_TYPE, v_filter_type);
            cv.put(DataTableColumns.FILTER_VALUE, v_filter_value);

            cv.put(DataTableColumns.ID, v_id);

            ODKDatabaseUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedDefns,
                cv, v_id);
          }
        }
        cr.close();
        return true;
      } catch (IOException e) {
        return false;
      } finally {
        try {
          input.close();
        } catch (IOException e) {
        }
      }
    } catch (IOException e) {
      return false;
    } finally {
      if (db != null) {
        db.close();
      }
    }
  }

}
