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

package org.opendatakit.common.android.provider.impl;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.opendatakit.common.android.logic.FormInfo;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteException;
import android.net.Uri;
import android.util.Log;

public final class FormsDiscoveryRunnable implements Runnable {
  private static String t = "FormsDiscoveryRunnable";

  private static int counter = 0;
  private static final Map<String, Integer> appInstanceCounterStart = new HashMap<String, Integer>();

  private int instanceCounter;
  private Context context;
  private Uri formsProviderContentUri;
  private String appName;

  private static synchronized final int getNextCount() {
    int newCount = ++counter;
    return newCount;
  }

  public FormsDiscoveryRunnable(FormsProviderImpl impl, String appName) {
    context = impl.getContext();
    formsProviderContentUri = Uri.parse("content://" + impl.getFormsAuthority());
    this.appName = appName;
    this.instanceCounter = getNextCount();
  }

  /**
   * Remove definitions from the Forms database that are no longer present on
   * disk.
   */
  private final void removeStaleFormInfo() {
    Log.i(t, "[" + instanceCounter + "] removeStaleFormInfo " + appName + " begin");
    ArrayList<Uri> badEntries = new ArrayList<Uri>();
    Cursor c = null;
    try {
      c = context.getContentResolver().query(
          Uri.withAppendedPath(formsProviderContentUri, appName), null, null, null, null);

      if (c.moveToFirst()) {
        do {
          String id = c.getString(c.getColumnIndex(FormsColumns.FORM_ID));
          Uri otherUri = Uri.withAppendedPath(
              Uri.withAppendedPath(formsProviderContentUri, appName), id);

          String formMediaPath = c.getString(c.getColumnIndex(FormsColumns.FORM_MEDIA_PATH));
          File f = new File(formMediaPath);
          if (!f.exists() || !f.isDirectory()) {
            // the form definition does not exist
            badEntries.add(otherUri);
          }
        } while (c.moveToNext());
      }
    } catch (Exception e) {
      Log.e(
          t,
          "[" + instanceCounter + "] removeStaleFormInfo " + appName + " exception: "
              + e.toString());
      e.printStackTrace();
      return;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }

    // delete the other entries (and directories)
    for (Uri badUri : badEntries) {
      Log.i(
          t,
          "[" + instanceCounter + "] removeStaleFormInfo: " + appName + " deleting: "
              + badUri.toString());
      context.getContentResolver().delete(badUri, null, null);
    }
    Log.i(t, "[" + instanceCounter + "] removeStaleFormInfo " + appName + " end");
  }

  /**
   * Construct a directory name that is unused in the stale path and move
   * mediaPath there.
   *
   * @param mediaPath
   * @param baseStaleMediaPath
   *          -- the stale directory corresponding to the mediaPath container
   * @return the directory within the stale directory that the mediaPath was
   *         renamed to.
   * @throws IOException
   */
  private final File moveToStaleDirectory(File mediaPath, String baseStaleMediaPath)
      throws IOException {
    // we have a 'default' form in the forms directory.
    // Move it to the stale directory.
    // Delete all records referring to this directory.
    int i = 0;
    File tempMediaPath = new File(baseStaleMediaPath + mediaPath.getName() + "_"
        + Integer.toString(i));
    while (tempMediaPath.exists()) {
      ++i;
      tempMediaPath = new File(baseStaleMediaPath + mediaPath.getName() + "_" + Integer.toString(i));
    }
    FileUtils.moveDirectory(mediaPath, tempMediaPath);
    return tempMediaPath;
  }

  /**
   * Scan the given formDir and update the Forms database. If it is the
   * formsFolder, then any 'default' forms should be forbidden. If it is not the
   * formsFolder, only 'default' forms should be allowed
   *
   * @param mediaPath
   *          -- full formDir
   * @param isFormsFolder
   * @param baseStaleMediaPath
   *          -- path prefix to the stale forms/framework directory.
   */
  private final void updateFormDir(File mediaPath, boolean isFormsFolder, String baseStaleMediaPath) {
    Log.i(t, "[" + instanceCounter + "] updateFormInfo: " + mediaPath.getAbsolutePath());

    boolean needUpdate = true;
    FormInfo fi = null;
    Uri uri = null;
    Cursor c = null;
    try {
      File formDef = new File(mediaPath, ODKFileUtils.FORMDEF_JSON_FILENAME);

      String selection = FormsColumns.FORM_MEDIA_PATH + "=?";
      String[] selectionArgs = { mediaPath.getAbsolutePath() };
      c = context.getContentResolver().query(
          Uri.withAppendedPath(formsProviderContentUri, appName), null, selection, selectionArgs,
          null);

      if (c.getCount() > 1) {
        c.close();
        // we have multiple records for this one directory.
        // Rename the directory. Delete the records, and move the
        // directory back.
        File tempMediaPath = moveToStaleDirectory(mediaPath, baseStaleMediaPath);
        context.getContentResolver().delete(Uri.withAppendedPath(formsProviderContentUri, appName),
            selection, selectionArgs);
        FileUtils.moveDirectory(tempMediaPath, mediaPath);
        // we don't know which of the above records was correct, so
        // reparse this to get ground truth...
        fi = new FormInfo(context, formDef);
      } else if (c.getCount() == 1) {
        c.moveToFirst();
        String id = c.getString(c.getColumnIndex(FormsColumns.FORM_ID));
        uri = Uri.withAppendedPath(Uri.withAppendedPath(formsProviderContentUri, appName), id);
        Long lastModificationDate = c.getLong(c.getColumnIndex(FormsColumns.DATE));
        Long formDefModified = formDef.lastModified();
        if (lastModificationDate.compareTo(formDefModified) == 0) {
          Log.i(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
              + " formDef unchanged");
          fi = new FormInfo(c, false);
          needUpdate = false;
        } else {
          fi = new FormInfo(context, formDef);
        }
      } else if (c.getCount() == 0) {
        // it should be new, try to parse it...
        fi = new FormInfo(context, formDef);
      }

      // Enforce that a formId == FormsColumns.COMMON_BASE_FORM_ID can only be
      // in the Framework directory
      // and that no other formIds can be in that directory. If this is not the
      // case, ensure that
      // this record is moved to the stale directory.

      if (fi.formId.equals(FormsColumns.COMMON_BASE_FORM_ID)) {
        if (isFormsFolder) {
          // we have a 'default' form in the forms directory.
          // Move it to the stale directory.
          // Delete all records referring to this directory.
          moveToStaleDirectory(mediaPath, baseStaleMediaPath);
          context.getContentResolver().delete(
              Uri.withAppendedPath(formsProviderContentUri, appName), selection, selectionArgs);
          return;
        }
      } else {
        if (!isFormsFolder) {
          // we have a non-'default' form in the framework directory.
          // Move it to the stale directory.
          // Delete all records referring to this directory.
          moveToStaleDirectory(mediaPath, baseStaleMediaPath);
          context.getContentResolver().delete(
              Uri.withAppendedPath(formsProviderContentUri, appName), selection, selectionArgs);
          return;
        }
      }
    } catch (SQLiteException e) {
      e.printStackTrace();
      Log.e(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
          + " exception: " + e.toString());
      return;
    } catch (IOException e) {
      e.printStackTrace();
      Log.e(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
          + " exception: " + e.toString());
      return;
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
      Log.e(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
          + " exception: " + e.toString());
      try {
        FileUtils.deleteDirectory(mediaPath);
        Log.i(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
            + " Removing -- unable to parse formDef file: " + e.toString());
      } catch (IOException e1) {
        e1.printStackTrace();
        Log.i(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
            + " Removing -- unable to delete form directory: " + mediaPath.getName() + " error: "
            + e.toString());
      }
      return;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }

    // Delete any entries matching this FORM_ID, but not the same directory and
    // which have a version that is equal to or older than this version.
    String selection;
    String[] selectionArgs;
    if (fi.formVersion == null) {
      selection = FormsColumns.FORM_MEDIA_PATH + "!=? AND " + FormsColumns.FORM_ID + "=? AND "
          + FormsColumns.FORM_VERSION + " IS NULL";
      String[] temp = { mediaPath.getAbsolutePath(), fi.formId };
      selectionArgs = temp;
    } else {
      selection = FormsColumns.FORM_MEDIA_PATH + "!=? AND " + FormsColumns.FORM_ID + "=? AND "
          + "( " + FormsColumns.FORM_VERSION + " IS NULL" + " OR " + FormsColumns.FORM_VERSION
          + " <=?" + " )";
      String[] temp = { mediaPath.getAbsolutePath(), fi.formId, fi.formVersion };
      selectionArgs = temp;
    }

    context.getContentResolver().delete(Uri.withAppendedPath(formsProviderContentUri, appName),
        selection, selectionArgs);

    // See if we have any newer versions already present...
    if (fi.formVersion == null) {
      selection = FormsColumns.FORM_MEDIA_PATH + "!=? AND " + FormsColumns.FORM_ID + "=? AND "
          + FormsColumns.FORM_VERSION + " IS NOT NULL";
      String[] temp = { mediaPath.getAbsolutePath(), fi.formId };
      selectionArgs = temp;
    } else {
      selection = FormsColumns.FORM_MEDIA_PATH + "!=? AND " + FormsColumns.FORM_ID + "=? AND "
          + FormsColumns.FORM_VERSION + " >?";
      String[] temp = { mediaPath.getAbsolutePath(), fi.formId, fi.formVersion };
      selectionArgs = temp;
    }

    try {
      c = context.getContentResolver().query(
          Uri.withAppendedPath(formsProviderContentUri, appName), null, selection, selectionArgs,
          null);

      if (c.moveToFirst()) {
        // the directory we are processing is stale -- move it to stale
        // directory
        moveToStaleDirectory(mediaPath, baseStaleMediaPath);
        return;
      }
    } catch (SQLiteException e) {
      e.printStackTrace();
      Log.e(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
          + " exception: " + e.toString());
      return;
    } catch (IOException e) {
      e.printStackTrace();
      Log.e(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
          + " exception: " + e.toString());
      return;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }

    if (!needUpdate) {
      // no change...
      return;
    }

    try {
      // Now insert or update the record...
      ContentValues v = new ContentValues();
      String[] values = fi.asRowValues(FormsColumns.formsDataColumnNames);
      for (int i = 0; i < values.length; ++i) {
        v.put(FormsColumns.formsDataColumnNames[i], values[i]);
      }

      if (uri != null) {
        int count = context.getContentResolver().update(uri, v, null, null);
        Log.i(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath() + " "
            + count + " records successfully updated");
      } else {
        context.getContentResolver().insert(Uri.withAppendedPath(formsProviderContentUri, appName),
            v);
        Log.i(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
            + " one record successfully inserted");
      }

    } catch (SQLiteException ex) {
      ex.printStackTrace();
      Log.e(t, "[" + instanceCounter + "] updateFormDir: " + mediaPath.getAbsolutePath()
          + " exception: " + ex.toString());
      return;
    }
  }

  /**
   * Scan for new directories in the given formsDir and add them to the
   * database. If it is the formsFolder, then any 'default' forms should be
   * forbidden. If it is not the formsFolder, only 'default' forms should be
   * allowed
   *
   * @param formsDir
   * @param isFormsFolder
   * @param baseStaleMediaPath
   */
  private final void updateFormInfoCommon(File formsDir, boolean isFormsFolder,
      String baseStaleMediaPath) {

    File[] candidates = formsDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        if (!pathname.isDirectory())
          return false;
        File f = new File(pathname, ODKFileUtils.FORMDEF_JSON_FILENAME);
        return (f.exists() && f.isFile());
      }
    });

    ArrayList<File> formDirs = new ArrayList<File>();
    if (candidates != null) {
      for (File mediaDir : candidates) {
        formDirs.add(mediaDir);
      }
    }

    // sort the directories so we process the newest formDef.json files
    // first. We don't care if the directory was modified, as much as
    // whether the formDef.json was modified.
    Collections.sort(formDirs, new Comparator<File>() {

      @Override
      public int compare(File lhs, File rhs) {
        File lhf = new File(lhs, ODKFileUtils.FORMDEF_JSON_FILENAME);
        File rhf = new File(rhs, ODKFileUtils.FORMDEF_JSON_FILENAME);
        if (!rhs.exists() || !rhf.exists())
          return -1;
        if (!lhs.exists() || !lhf.exists())
          return 1;

        return (lhf.lastModified() > rhf.lastModified()) ? -1 : ((lhf.lastModified() == rhf
            .lastModified()) ? 0 : 1);
      }
    });

    for (File f : formDirs) {
      Log.i(t, "[" + instanceCounter + "] updateFormInfo: " + f.getAbsolutePath());
      updateFormDir(f, isFormsFolder, baseStaleMediaPath);
    }
  }

  /**
   * Scan for new forms directories in both the forms and framework areas and
   * add them to Forms database.
   */
  private final void updateFormInfo() {
    Log.i(t, "[" + instanceCounter + "] updateFormInfo: " + appName + " begin");

    File formsDir = new File(ODKFileUtils.getFormsFolder(appName));
    updateFormInfoCommon(formsDir, true, ODKFileUtils.getStaleFormsFolder(appName) + File.separator);

    File frameworkDir = new File(ODKFileUtils.getFrameworkFolder(appName));
    updateFormInfoCommon(frameworkDir, false, ODKFileUtils.getStaleFrameworkFolder(appName)
        + File.separator);

    Log.i(t, "[" + instanceCounter + "] updateFormInfo: " + appName + " end");
  }

  @Override
  public void run() {

    // ensure that there is one and only one scan happening at any one time
    // should be handled by ExecutorService, but just in case...
    synchronized (appInstanceCounterStart) {
      Integer ic = appInstanceCounterStart.get(appName);
      if (ic == null || ic < instanceCounter) {
        // this task was created after the start of the last task that searched
        // and updated the appName tree. So we should execute it.
        int startCounter = counter;
        Log.i(t, "[" + instanceCounter + "] doInBackground begins! " + appName + " baseCounter: "
            + ic + " startCounter: " + startCounter);

        try {
          removeStaleFormInfo();
          updateFormInfo();
        } finally {
          Log.i(t, "[" + instanceCounter + "] doInBackground ends! " + appName);
          appInstanceCounterStart.put(appName, startCounter);
        }
      } else {
        Log.i(t, "[" + instanceCounter + "] doInBackground skipped! " + appName + " baseCounter: "
            + ic);
      }
    }
  }

}
