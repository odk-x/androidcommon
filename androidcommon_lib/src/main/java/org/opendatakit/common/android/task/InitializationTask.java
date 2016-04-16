/*
 * Copyright (C) 2013 University of Washington
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

package org.opendatakit.common.android.task;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.opendatakit.androidcommon.R;
import org.opendatakit.common.android.application.CommonApplication;
import org.opendatakit.common.android.listener.InitializationListener;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.provider.FormsProviderAPI;
import org.opendatakit.common.android.utilities.CsvUtil;
import org.opendatakit.common.android.utilities.CsvUtil.ImportListener;
import org.opendatakit.common.android.utilities.ODKCursorUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.service.OdkDbHandle;

import android.content.ContentValues;
import android.content.res.AssetFileDescriptor;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.RemoteException;

/**
 * Background task for exploding the built-in zipfile resource into the
 * framework directory of the application and doing forms discovery on this
 * appName.
 *
 * @author mitchellsundt@gmail.com
 */
public class InitializationTask extends AsyncTask<Void, String, ArrayList<String>>
    implements ImportListener {

  private static final String t = "InitializationTask";

  private CommonApplication appContext;
  private InitializationListener mStateListener;
  private String appName;

  private enum InitAction {DEFINE_TABLE, IMPORT_CSV}

  ;
  private InitAction actionType;

  private boolean problemDefiningTables = false;
  private boolean problemImportingCSVs = false;
  private Set<String> mFileNotFoundSet = new HashSet<String>();
  private boolean mSuccess = false;
  private ArrayList<String> mPendingResult;
  private ArrayList<String> mResult = new ArrayList<String>();

  private boolean mPendingSuccess = false;

  @Override
  protected ArrayList<String> doInBackground(Void... values) {
    mPendingSuccess = true;
    mPendingResult = new ArrayList<String>();

    // ///////////////////////////////////////////////
    // check that the framework zip has been exploded
    String toolName = getApplication().getToolName();
    if (toolName.equalsIgnoreCase("Survey")) {
      if (!ODKFileUtils.isConfiguredSurveyApp(appName, getApplication().getVersionCodeString())) {
        publishProgress(appContext.getString(R.string.expansion_unzipping_begins), null);

        extractFromRawZip(getApplication().getSystemZipResourceId(), true, mPendingResult);
        extractFromRawZip(getApplication().getConfigZipResourceId(), false, mPendingResult);

        ODKFileUtils.assertConfiguredSurveyApp(appName, getApplication().getVersionCodeString());
      }
    } else if (toolName.equalsIgnoreCase("Tables")) {

      if (!ODKFileUtils.isConfiguredTablesApp(appName, getApplication().getVersionCodeString())) {
        publishProgress(appContext.getString(R.string.expansion_unzipping_begins), null);

        extractFromRawZip(getApplication().getSystemZipResourceId(), true, mPendingResult);
        extractFromRawZip(getApplication().getConfigZipResourceId(), false, mPendingResult);

        ODKFileUtils.assertConfiguredTablesApp(appName, getApplication().getVersionCodeString());
      }

    } else if (toolName.equalsIgnoreCase("Scan")) {

      if (!ODKFileUtils.isConfiguredScanApp(appName, getApplication().getVersionCodeString())) {
        publishProgress(appContext.getString(R.string.expansion_unzipping_begins), null);

        extractFromRawZip(getApplication().getSystemZipResourceId(), true, mPendingResult);
        extractFromRawZip(getApplication().getConfigZipResourceId(), false, mPendingResult);

        ODKFileUtils.assertConfiguredScanApp(appName, getApplication().getVersionCodeString());
      }
    }

    try {
      updateTableDirs();
    } catch (RemoteException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      WebLogger.getLogger(appName).e(t, "Error accesssing database during table creation sweep");
      mPendingResult.add(appContext.getString(R.string.abort_error_accessing_database));
      return mPendingResult;
    }

    updateFormDirs();

    try {
      mPendingSuccess = mPendingSuccess && initTables();
    } catch (RemoteException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      WebLogger.getLogger(appName).e(t, "Error accesssing database during CSV import sweep");
      mPendingResult.add(appContext.getString(R.string.abort_error_accessing_database));
      return mPendingResult;
    }

    return mPendingResult;
  }

  interface ZipAction {
    public void doWorker(ZipEntry entry, ZipInputStream zipInputStream, int indexIntoZip, long size)
        throws FileNotFoundException, IOException;

    public void done(int totalCount);
  }

  private final void doActionOnRawZip(int resourceId, boolean overwrite, ArrayList<String> result,
      ZipAction action) {
    String message = null;
    InputStream rawInputStream = null;
    try {
      rawInputStream = appContext.getResources().openRawResource(resourceId);
      ZipInputStream zipInputStream = null;
      ZipEntry entry = null;
      try {
        int countFiles = 0;

        zipInputStream = new ZipInputStream(rawInputStream);
        while ((entry = zipInputStream.getNextEntry()) != null) {
          message = null;
          if (isCancelled()) {
            message = "cancelled";
            result.add(entry.getName() + " " + message);
            break;
          }
          ++countFiles;
          action.doWorker(entry, zipInputStream, countFiles, 0);
        }
        zipInputStream.close();

        action.done(countFiles);
      } catch (IOException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        mPendingSuccess = false;
        if (e.getCause() != null) {
          message = e.getCause().getMessage();
        } else {
          message = e.getMessage();
        }
        if (entry != null) {
          result.add(entry.getName() + " " + message);
        } else {
          result.add("Error accessing zipfile resource " + message);
        }
      } finally {
        if (zipInputStream != null) {
          try {
            zipInputStream.close();
          } catch (IOException e) {
            WebLogger.getLogger(appName).printStackTrace(e);
            WebLogger.getLogger(appName).e(t, "Closing of ZipFile failed: " + e.toString());
          }
        }
      }
    } catch (Exception e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      mPendingSuccess = false;
      if (e.getCause() != null) {
        message = e.getCause().getMessage();
      } else {
        message = e.getMessage();
      }
      result.add("Error accessing zipfile resource " + message);
    } finally {
      if (rawInputStream != null) {
        try {
          rawInputStream.close();
        } catch (IOException e) {
          WebLogger.getLogger(appName).printStackTrace(e);
        }
      }
    }
  }

  static class ZipEntryCounter implements ZipAction {
    int totalFiles = -1;

    @Override
    public void doWorker(ZipEntry entry, ZipInputStream zipInputStream, int indexIntoZip,
        long size) {
      // no-op
    }

    @Override
    public void done(int totalCount) {
      totalFiles = totalCount;
    }
  }

  ;

  private final void extractFromRawZip(int resourceId, final boolean overwrite,
      ArrayList<String> result) {

    final ZipEntryCounter countTotal = new ZipEntryCounter();

    if (resourceId == -1) {
      return;
    }

    doActionOnRawZip(resourceId, overwrite, result, countTotal);

    if (countTotal.totalFiles == -1) {
      return;
    }

    ZipAction worker = new ZipAction() {

      long bytesProcessed = 0L;
      long lastBytesProcessedThousands = 0L;

      @Override
      public void doWorker(ZipEntry entry, ZipInputStream zipInputStream, int indexIntoZip,
          long size) throws IOException {

        File tempFile = new File(ODKFileUtils.getAppFolder(appName), entry.getName());
        String formattedString = appContext
            .getString(R.string.expansion_unzipping_without_detail, entry.getName(), indexIntoZip,
                countTotal.totalFiles);
        String detail;
        if (entry.isDirectory()) {
          detail = appContext.getString(R.string.expansion_create_dir_detail);
          publishProgress(formattedString, detail);
          tempFile.mkdirs();
        } else if (overwrite || !tempFile.exists()) {
          int bufferSize = 8192;
          OutputStream out = new BufferedOutputStream(new FileOutputStream(tempFile, false),
              bufferSize);
          byte buffer[] = new byte[bufferSize];
          int bread;
          while ((bread = zipInputStream.read(buffer)) != -1) {
            bytesProcessed += bread;
            long curThousands = (bytesProcessed / 1000L);
            if (curThousands != lastBytesProcessedThousands) {
              detail = appContext
                  .getString(R.string.expansion_unzipping_detail, bytesProcessed, indexIntoZip);
              publishProgress(formattedString, detail);
              lastBytesProcessedThousands = curThousands;
            }
            out.write(buffer, 0, bread);
          }
          out.flush();
          out.close();

          detail = appContext
              .getString(R.string.expansion_unzipping_detail, bytesProcessed, indexIntoZip);
          publishProgress(formattedString, detail);
        }
        WebLogger.getLogger(appName).i(t, "Extracted ZipEntry: " + entry.getName());
      }

      @Override
      public void done(int totalCount) {
        String completionString = appContext
            .getString(R.string.expansion_unzipping_complete, totalCount);
        publishProgress(completionString, null);
      }

    };

    doActionOnRawZip(resourceId, overwrite, result, worker);
  }

  /**
   * Remove definitions from the Forms database that are no longer present on
   * disk.
   */
  private final void removeStaleFormInfo(List<File> discoveredFormDefDirs) {
    Uri formsProviderContentUri = Uri.parse("content://" + FormsProviderAPI.AUTHORITY);

    String completionString = appContext.getString(R.string.searching_for_deleted_forms);
    publishProgress(completionString, null);

    WebLogger.getLogger(appName).i(t, "removeStaleFormInfo " + appName + " begin");
    ArrayList<Uri> badEntries = new ArrayList<Uri>();
    Cursor c = null;
    try {
      c = appContext.getContentResolver()
          .query(Uri.withAppendedPath(formsProviderContentUri, appName), null, null, null, null);

      if (c == null) {
        WebLogger.getLogger(appName)
            .w(t, "removeStaleFormInfo " + appName + " null cursor returned from query.");
        return;
      }

      if (c.moveToFirst()) {
        do {
          String tableId = ODKCursorUtils
              .getIndexAsString(c, c.getColumnIndex(FormsColumns.TABLE_ID));
          String formId = ODKCursorUtils
              .getIndexAsString(c, c.getColumnIndex(FormsColumns.FORM_ID));
          Uri otherUri = Uri.withAppendedPath(
              Uri.withAppendedPath(Uri.withAppendedPath(formsProviderContentUri, appName), tableId),
              formId);

          String examString = appContext.getString(R.string.examining_form, tableId, formId);
          publishProgress(examString, null);

          String formDir = ODKFileUtils.getFormFolder(appName, tableId, formId);
          File f = new File(formDir);
          File formDefJson = new File(f, ODKFileUtils.FORMDEF_JSON_FILENAME);
          if (!f.exists() || !f.isDirectory() || !formDefJson.exists() || !formDefJson.isFile()) {
            // the form definition does not exist
            badEntries.add(otherUri);
            continue;
          } else {
            // ////////////////////////////////
            // formdef.json exists. See if it is
            // unchanged...
            String json_md5 = ODKCursorUtils
                .getIndexAsString(c, c.getColumnIndex(FormsColumns.JSON_MD5_HASH));
            String fileMd5 = ODKFileUtils.getMd5Hash(appName, formDefJson);
            if (json_md5.equals(fileMd5)) {
              // it is unchanged -- no need to rescan it
              discoveredFormDefDirs.remove(f);
            }
          }
        } while (c.moveToNext());
      }
    } catch (Exception e) {
      WebLogger.getLogger(appName)
          .e(t, "removeStaleFormInfo " + appName + " exception: " + e.toString());
      WebLogger.getLogger(appName).printStackTrace(e);
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }

    // delete the other entries (and directories)
    for (Uri badUri : badEntries) {
      WebLogger.getLogger(appName)
          .i(t, "removeStaleFormInfo: " + appName + " deleting: " + badUri.toString());
      try {
        appContext.getContentResolver().delete(badUri, null, null);
      } catch (Exception e) {
        WebLogger.getLogger(appName)
            .e(t, "removeStaleFormInfo " + appName + " exception: " + e.toString());
        WebLogger.getLogger(appName).printStackTrace(e);
        // and continue -- don't throw an error
      }
    }
    WebLogger.getLogger(appName).i(t, "removeStaleFormInfo " + appName + " end");
  }

  /**
   * Construct a directory name that is unused in the stale path and move
   * mediaPath there.
   *
   * @param mediaPath
   * @param baseStaleMediaPath -- the stale directory corresponding to the mediaPath container
   * @return the directory within the stale directory that the mediaPath was
   * renamed to.
   * @throws IOException
   */
  private final File moveToStaleDirectory(File mediaPath, String baseStaleMediaPath)
      throws IOException {
    // we have a 'framework' form in the forms directory.
    // Move it to the stale directory.
    // Delete all records referring to this directory.
    int i = 0;
    File tempMediaPath = new File(
        baseStaleMediaPath + mediaPath.getName() + "_" + Integer.toString(i));
    while (tempMediaPath.exists()) {
      ++i;
      tempMediaPath = new File(
          baseStaleMediaPath + mediaPath.getName() + "_" + Integer.toString(i));
    }
    FileUtils.moveDirectory(mediaPath, tempMediaPath);
    return tempMediaPath;
  }

  private String displayTablesProgress;
  private String tableIdInProgress;
  private Map<String, Boolean> importStatus = new TreeMap<String, Boolean>();

  private final void updateTableDirs() throws RemoteException {
    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // Scan the tables directory, looking for tableIds with definition.csv
    // files.
    // If the tableId does not exist, try to create it using these files.
    // If the tableId already exists, do nothing -- assume everything is
    // up-to-date.
    // This means we don't pick up properties.csv changes, but the
    // definition.csv
    // should never change. If properties.csv changes, we assume the process
    // that
    // changed it will be triggering a reload of it through other means.
    actionType = InitAction.DEFINE_TABLE;

    CsvUtil util = new CsvUtil(getApplication(), appName);
    File tablesDir = new File(ODKFileUtils.getTablesFolder(appName));
    File[] tableIdDirs = tablesDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });

    List<String> tableIds;
    ODKFileUtils.assertDirectoryStructure(appName);
    OdkDbHandle db = null;
    try {
      db = getApplication().getDatabase().openDatabase(appName);
      tableIds = getApplication().getDatabase().getAllTableIds(appName, db);
    } finally {
      if (db != null) {
        getApplication().getDatabase().closeDatabase(appName, db);
      }
    }

    for (int i = 0; i < tableIdDirs.length; ++i) {
      File tableIdDir = tableIdDirs[i];
      String tableId = tableIdDir.getName();
      if (tableIds.contains(tableId)) {
        // assume it is up-to-date
        continue;
      }

      File definitionCsv = new File(ODKFileUtils.getTableDefinitionCsvFile(appName, tableId));
      File propertiesCsv = new File(ODKFileUtils.getTablePropertiesCsvFile(appName, tableId));
      if (definitionCsv.exists() && definitionCsv.isFile() && propertiesCsv.exists()
          && propertiesCsv.isFile()) {

        String formattedString = appContext
            .getString(R.string.scanning_for_table_definitions, tableId, (i + 1),
                tableIdDirs.length);
        String detail = appContext.getString(R.string.processing_file);
        displayTablesProgress = formattedString;
        tableIdInProgress = tableId;
        publishProgress(formattedString, detail);

        try {
          util.updateTablePropertiesFromCsv(tableId);
        } catch (IOException e) {
          mPendingResult.add(appContext.getString(R.string.defining_tableid_error, tableId));
          WebLogger.getLogger(appName).e(t, "Unexpected error during update from csv");
        }
      }
    }

  }

  private final boolean initTables() throws RemoteException {

    final String EMPTY_STRING = "";
    final String SPACE = " ";
    final String TOP_LEVEL_KEY_TABLE_KEYS = "table_keys";
    final String COMMA = ",";
    final String KEY_SUFFIX_CSV_FILENAME = ".filename";

    actionType = InitAction.IMPORT_CSV;

    /** Stores the table's key to its filename. */
    Map<String, String> mKeyToFileMap = new HashMap<String, String>();

    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // and now process tables.init file
    File init = new File(ODKFileUtils.getTablesInitializationFile(appName));
    File completedFile = new File(ODKFileUtils.getTablesInitializationCompleteMarkerFile(appName));
    if (!init.exists()) {
      // no initialization file -- we are done!
      return true;
    }
    boolean processFile = false;
    if (!completedFile.exists()) {
      processFile = true;
    } else {
      String initMd5 = ODKFileUtils.getMd5Hash(appName, init);
      String completedFileMd5 = ODKFileUtils.getMd5Hash(appName, completedFile);
      processFile = !initMd5.equals(completedFileMd5);
    }
    if (!processFile) {
      // we are done!
      return true;
    }

    Properties prop = new Properties();
    try {
      prop.load(new FileInputStream(init));
    } catch (IOException ex) {
      WebLogger.getLogger(appName).printStackTrace(ex);
      mPendingResult.add(appContext.getString(R.string.poorly_formatted_init_file));
      return false;
    }

    // assume if we load it, we have processed it.

    // We shouldn't really do this, but it avoids an infinite
    // recycle if there is an error during the processing of the
    // file.
    try {
      FileUtils.copyFile(init, completedFile);
    } catch (IOException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      // ignore this.
    }

    // prop was loaded
    if (prop != null) {
      String table_keys = prop.getProperty(TOP_LEVEL_KEY_TABLE_KEYS);

      // table_keys is defined
      if (table_keys != null) {
        // remove spaces and split at commas to get key names
        String[] keys = table_keys.replace(SPACE, EMPTY_STRING).split(COMMA);
        int fileCount = keys.length;
        int curFileCount = 0;
        String detail = appContext.getString(R.string.processing_file);

        File file;
        CsvUtil cu = new CsvUtil(getApplication(), appName);
        for (String key : keys) {
          curFileCount++;
          String filename = prop.getProperty(key + KEY_SUFFIX_CSV_FILENAME);
          this.importStatus.put(key, false);
          file = new File(ODKFileUtils.getAppFolder(appName), filename);
          mKeyToFileMap.put(key, filename);
          if (!file.exists()) {
            mFileNotFoundSet.add(key);
            WebLogger.getLogger(appName).i(t, "putting in file not found map true: " + key);
            String formattedString = appContext.getString(R.string.csv_file_not_found, filename);
            mPendingResult.add(formattedString);
            continue;
          }

          // update dialog message with current filename
          String formattedString = appContext
              .getString(R.string.importing_file_without_detail, curFileCount, fileCount, filename);
          displayTablesProgress = formattedString;
          publishProgress(formattedString, detail);
          ImportRequest request = null;

          // If the import file is in the config/assets/csv directory
          // and if it is of the form tableId.csv or tableId.fileQualifier.csv
          // and fileQualifier is not 'properties', then assume it is the
          // new-style CSV format.
          //
          String assetsCsvDirPath = ODKFileUtils
              .asRelativePath(appName, new File(ODKFileUtils.getAssetsCsvFolder(appName)));
          if (filename.startsWith(assetsCsvDirPath)) {
            // get past the file separator
            String csvFilename = filename.substring(assetsCsvDirPath.length() + 1);
            String[] terms = csvFilename.split("\\.");
            if (terms.length == 2 && terms[1].equals("csv")) {
              String tableId = terms[0];
              String fileQualifier = null;
              request = new ImportRequest(tableId, fileQualifier);
            } else if (terms.length == 3 && terms[1].equals("properties") && terms[2]
                .equals("csv")) {
              String tableId = terms[0];
              String fileQualifier = null;
              request = new ImportRequest(tableId, fileQualifier);
            } else if (terms.length == 3 && terms[2].equals("csv")) {
              String tableId = terms[0];
              String fileQualifier = terms[1];
              request = new ImportRequest(tableId, fileQualifier);
            } else if (terms.length == 4 && terms[2].equals("properties") && terms[3]
                .equals("csv")) {
              String tableId = terms[0];
              String fileQualifier = terms[1];
              request = new ImportRequest(tableId, fileQualifier);
            }

            if (request != null) {
              tableIdInProgress = request.getTableId();
              boolean success = false;
              success = cu
                  .importSeparable(this, request.getTableId(), request.getFileQualifier(), true);
              importStatus.put(key, success);
              if (success) {
                detail = appContext.getString(R.string.import_success);
                publishProgress(appContext
                    .getString(R.string.importing_file_without_detail, curFileCount, fileCount,
                        filename), detail);
              }
            }
          }

          if (request == null) {
            mPendingResult.add(appContext.getString(R.string.poorly_formatted_init_file));
            return false;
          }
        }
      } else {
        mPendingResult.add(appContext.getString(R.string.poorly_formatted_init_file));
        return false;
      }
    }
    return true;
  }

  private final void updateFormDirs() {

    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // scan for new forms...

    String completionString = appContext.getString(R.string.searching_for_form_defs);
    publishProgress(completionString, null);

    File tablesDir = new File(ODKFileUtils.getTablesFolder(appName));

    File[] tableIdDirs = tablesDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });

    List<File> formDirs = new ArrayList<File>();
    for (File tableIdDir : tableIdDirs) {
      String tableId = tableIdDir.getName();

      File formDir = new File(ODKFileUtils.getFormsFolder(appName, tableId));
      File[] formIdDirs = formDir.listFiles(new FileFilter() {

        @Override
        public boolean accept(File pathname) {
          File formDef = new File(pathname, ODKFileUtils.FORMDEF_JSON_FILENAME);
          return pathname.isDirectory() && formDef.exists() && formDef.isFile();
        }
      });

      if (formIdDirs != null) {
        formDirs.addAll(Arrays.asList(formIdDirs));
      }
    }

    // /////////////////////////////////////////
    // remove forms that no longer exist
    // remove the forms that haven't changed
    // from the discovered list
    removeStaleFormInfo(formDirs);

    // this is the complete list of forms we need to scan and possibly add
    // to the FormsProvider
    for (int i = 0; i < formDirs.size(); ++i) {
      File formDir = formDirs.get(i);

      String formId = formDir.getName();
      String tableId = formDir.getParentFile().getParentFile().getName();

      // specifically target this form...
      WebLogger.getLogger(appName).i(t, "updateFormInfo: form: " + formDir.getAbsolutePath());

      String examString = appContext
          .getString(R.string.updating_form_information, formDir.getName(), i + 1, formDirs.size());
      publishProgress(examString, null);

      updateFormDir(tableId, formId, formDir, true,
          ODKFileUtils.getPendingDeletionTablesFolder(appName) + File.separator);
    }

  }

  /**
   * Scan the given formDir and update the Forms database. If it is the
   * formsFolder, then any 'framework' forms should be forbidden. If it is not
   * the formsFolder, only 'framework' forms should be allowed
   *
   * @param tableId
   * @param formId
   * @param formDir
   * @param isFormsFolder
   * @param baseStaleMediaPath -- path prefix to the stale forms/framework directory.
   */
  private final void updateFormDir(String tableId, String formId, File formDir,
      boolean isFormsFolder, String baseStaleMediaPath) {
    Uri formsProviderContentUri = Uri.parse("content://" + FormsProviderAPI.AUTHORITY);
    String formDirectoryPath = formDir.getAbsolutePath();
    WebLogger.getLogger(appName).i(t, "updateFormDir: " + formDirectoryPath);

    String successMessage = appContext.getString(R.string.form_register_success, tableId, formId);
    String failureMessage = appContext.getString(R.string.form_register_failure, tableId, formId);

    Cursor c = null;
    try {
      String selection = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=?";
      String[] selectionArgs = { tableId, formId };
      c = appContext.getContentResolver()
          .query(Uri.withAppendedPath(formsProviderContentUri, appName), null, selection,
              selectionArgs, null);

      if (c == null) {
        WebLogger.getLogger(appName)
            .w(t, "updateFormDir: " + formDirectoryPath + " null cursor -- cannot update!");
        mPendingResult.add(failureMessage);
        return;
      }

      if (c.getCount() > 1) {
        c.close();
        WebLogger.getLogger(appName).w(t, "updateFormDir: " + formDirectoryPath
            + " multiple records from cursor -- delete all and restore!");
        // we have multiple records for this one directory.
        // Rename the directory. Delete the records, and move the
        // directory back.
        File tempMediaPath = moveToStaleDirectory(formDir, baseStaleMediaPath);

        appContext.getContentResolver()
            .delete(Uri.withAppendedPath(formsProviderContentUri, appName), selection,
                selectionArgs);

        FileUtils.moveDirectory(tempMediaPath, formDir);

        ContentValues cv = new ContentValues();
        cv.put(FormsColumns.TABLE_ID, tableId);
        cv.put(FormsColumns.FORM_ID, formId);
        appContext.getContentResolver()
            .insert(Uri.withAppendedPath(formsProviderContentUri, appName), cv);
      } else if (c.getCount() == 1) {
        c.close();
        ContentValues cv = new ContentValues();
        cv.put(FormsColumns.TABLE_ID, tableId);
        cv.put(FormsColumns.FORM_ID, formId);
        appContext.getContentResolver()
            .update(Uri.withAppendedPath(formsProviderContentUri, appName), cv, null, null);
      } else if (c.getCount() == 0) {
        c.close();
        ContentValues cv = new ContentValues();
        cv.put(FormsColumns.TABLE_ID, tableId);
        cv.put(FormsColumns.FORM_ID, formId);
        appContext.getContentResolver()
            .insert(Uri.withAppendedPath(formsProviderContentUri, appName), cv);
      }
    } catch (IOException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      WebLogger.getLogger(appName)
          .e(t, "updateFormDir: " + formDirectoryPath + " exception: " + e.toString());
      mPendingResult.add(failureMessage);
      return;
    } catch (IllegalArgumentException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      WebLogger.getLogger(appName)
          .e(t, "updateFormDir: " + formDirectoryPath + " exception: " + e.toString());
      try {
        FileUtils.deleteDirectory(formDir);
        WebLogger.getLogger(appName).i(t,
            "updateFormDir: " + formDirectoryPath + " Removing -- unable to parse formDef file: "
                + e.toString());
      } catch (IOException e1) {
        WebLogger.getLogger(appName).printStackTrace(e1);
        WebLogger.getLogger(appName).i(t,
            "updateFormDir: " + formDirectoryPath + " Removing -- unable to delete form directory: "
                + formDir.getName() + " error: " + e.toString());
      }
      mPendingResult.add(failureMessage);
      return;
    } catch (Exception e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      WebLogger.getLogger(appName)
          .e(t, "updateFormDir: " + formDirectoryPath + " exception: " + e.toString());
      mPendingResult.add(failureMessage);
      return;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    mPendingResult.add(successMessage);
  }

  @Override
  protected void onPostExecute(ArrayList<String> result) {
    synchronized (this) {
      mResult = result;
      mSuccess = mPendingSuccess && !problemDefiningTables &&
          !problemImportingCSVs && mFileNotFoundSet.isEmpty();

      if (mStateListener != null) {
        mStateListener.initializationComplete(mSuccess, mResult);
      }
    }
  }

  @Override
  protected void onCancelled(ArrayList<String> result) {
    synchronized (this) {
      // can be null if cancelled before task executes
      mResult = (result == null) ? new ArrayList<String>() : result;
      mSuccess = false;
      if (mStateListener != null) {
        mStateListener.initializationComplete(mSuccess, mResult);
      }
    }
  }

  @Override
  protected void onProgressUpdate(String... values) {
    synchronized (this) {
      if (mStateListener != null) {
        // update progress and total
        mStateListener.initializationProgressUpdate(
            values[0] + ((values[1] != null) ? "\n(" + values[1] + ")" : ""));
      }
    }

  }

  @Override
  public void updateProgressDetail(String displayDetail) {
    publishProgress(displayTablesProgress, displayDetail);
  }

  @Override
  public void importComplete(boolean outcome) {
    if (actionType == InitAction.IMPORT_CSV) {
      if (outcome) {
        mPendingResult.add(appContext.getString(R.string.import_csv_success, tableIdInProgress));
      } else {
        mPendingResult.add(appContext.getString(R.string.import_csv_failure, tableIdInProgress));
      }
      problemImportingCSVs = problemImportingCSVs || !outcome;
    } else {
      if (outcome) {
        mPendingResult
            .add(appContext.getString(R.string.defining_tableid_success, tableIdInProgress));
      } else {
        mPendingResult
            .add(appContext.getString(R.string.defining_tableid_failure, tableIdInProgress));
      }
      problemDefiningTables = problemDefiningTables || !outcome;
    }
  }

  public boolean getOverallSuccess() {
    return mSuccess;
  }

  public ArrayList<String> getResult() {
    return mResult;
  }

  public void setInitializationListener(InitializationListener sl) {
    synchronized (this) {
      mStateListener = sl;
    }
  }

  public void setAppName(String appName) {
    synchronized (this) {
      this.appName = appName;
    }
  }

  public String getAppName() {
    return appName;
  }

  public void setApplication(CommonApplication appContext) {
    synchronized (this) {
      this.appContext = appContext;
    }
  }

  public CommonApplication getApplication() {
    return appContext;
  }

}
