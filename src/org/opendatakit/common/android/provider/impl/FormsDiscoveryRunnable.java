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
  private static final Map<String, Integer> appInstanceCounterStart = new HashMap<String,Integer>();

  private int instanceCounter;
  private Context context;
  private Uri formsProviderContentUri;
  private String appName;

  public FormsDiscoveryRunnable(FormsProviderImpl impl, String appName) {
    context = impl.getContext();
    formsProviderContentUri = Uri.parse("content://" + impl.getFormsAuthority());
    this.appName = appName;
    this.instanceCounter = ++counter;
  }

  /**
   * Remove definitions from the Forms database that are no longer present on disk.
   *
   * @param appContext
   * @param appName
   * @param errors
   * @param instance
   */
  private final void removeStaleFormInfo(String appName,
      StringBuilder errors, int instance) {
     ArrayList<Uri> badEntries = new ArrayList<Uri>();
     Cursor c = null;
     try {
        c = context.getContentResolver()
              .query(Uri.withAppendedPath(formsProviderContentUri, appName), null, null, null, null);

        if (c.moveToFirst()) {
           do {
              String id = c.getString(c.getColumnIndex(FormsColumns.FORM_ID));
              Uri otherUri = Uri.withAppendedPath(
                  Uri.withAppendedPath(formsProviderContentUri, appName), id);

              String formMediaPath = c.getString(c
                    .getColumnIndex(FormsColumns.FORM_MEDIA_PATH));
              File f = new File(formMediaPath);
              if (!f.exists() || !f.isDirectory()) {
                 // the form definition does not exist
                 badEntries.add(otherUri);
              }
           } while (c.moveToNext());
        }
     } catch (SQLiteException e) {
        e.printStackTrace();
        errors.append(e.toString()).append("\r\n");
        return;
     } finally {
        if (c != null && !c.isClosed()) {
           c.close();
        }
     }

     // delete the other entries (and directories)
     for (Uri badUri : badEntries) {
       context.getContentResolver().delete(badUri, null, null);
     }
  }

  public final void updateFormInfo(StringBuilder errors, File mediaPath, int instance) {
     Log.i(t, "[" + instance + "] doInBackground begins!");
     String appName = mediaPath.getParentFile()/* forms folder*/.getParentFile()/*app*/.getName();

     boolean needUpdate = true;
     FormInfo fi = null;
     Uri uri = null;
     Cursor c = null;
     try {
        File formDef = new File(mediaPath, ODKFileUtils.FORMDEF_JSON_FILENAME);

        String selection = FormsColumns.FORM_MEDIA_PATH + "=?";
        String[] selectionArgs = { mediaPath.getAbsolutePath() };
        c = context.getContentResolver()
              .query(Uri.withAppendedPath(formsProviderContentUri, appName), null, selection,
                    selectionArgs, null);

        if (c.getCount() > 1) {
           c.close();
           // we have multiple records for this one directory.
           // Rename the directory. Delete the records, and move the
           // directory back.
           int i = 0;
           File tempMediaPath = new File(ODKFileUtils.getStaleFormsFolder(appName),
                 mediaPath.getName() + "_" + Integer.toString(i));
           while (tempMediaPath.exists()) {
              ++i;
              tempMediaPath = new File(ODKFileUtils.getStaleFormsFolder(appName),
                    mediaPath.getName() + "_" + Integer.toString(i));
           }
           FileUtils.moveDirectory(mediaPath, tempMediaPath);
           context.getContentResolver()
                 .delete(Uri.withAppendedPath(formsProviderContentUri, appName), selection,
                       selectionArgs);
           FileUtils.moveDirectory(tempMediaPath, mediaPath);
           // we don't know which of the above records was correct, so
           // reparse this to get ground truth...
           fi = new FormInfo(context, formDef);
        } else if (c.getCount() == 1) {
           c.moveToFirst();
           String id = c.getString(c.getColumnIndex(FormsColumns.FORM_ID));
           uri = Uri.withAppendedPath(Uri.withAppendedPath(formsProviderContentUri, appName), id);
           Long lastModificationDate = c.getLong(c.getColumnIndex(FormsColumns.DATE));
           if (lastModificationDate.compareTo(formDef.lastModified()) == 0) {
              Log.i(t, "[" + instance + "] formDef unchanged: " + mediaPath.getName());
              fi = new FormInfo(c, false);
              needUpdate = false;
           } else {
              fi = new FormInfo(context, formDef);
           }
        } else if (c.getCount() == 0) {
           // it should be new, try to parse it...
           fi = new FormInfo(context, formDef);
        }

     } catch (SQLiteException e) {
        e.printStackTrace();
        errors.append(e.toString()).append("\r\n");
        return;
     } catch (IOException e) {
        e.printStackTrace();
        errors.append(e.toString()).append("\r\n");
        return;
     } catch (IllegalArgumentException e) {
        e.printStackTrace();
        errors.append(e.toString()).append("\r\n");
        try {
           FileUtils.deleteDirectory(mediaPath);
           Log.i(t,
                 "["
                       + instance
                       + "] Removing -- unable to parse formDef file: "
                       + e.toString());
        } catch (IOException e1) {
           e1.printStackTrace();
           Log.i(t, "[" + instance
                 + "] Removing -- unable to delete form directory: "
                 + mediaPath.getName() + " error: " + e.toString());
        }
        return;
     } finally {
        if (c != null && !c.isClosed()) {
           c.close();
        }
     }

     // Delete any entries matching this FORM_ID, but not the same directory and which
     // have a version that is equal to or older than this version.
     String selection;
     String[] selectionArgs;
     if ( fi.formVersion == null ) {
         selection = FormsColumns.FORM_MEDIA_PATH + "!=? AND " +
              FormsColumns.FORM_ID + "=? AND " +
              FormsColumns.FORM_VERSION + " IS NULL";
         String[] temp = { mediaPath.getAbsolutePath(), fi.formId };
         selectionArgs = temp;
     } else {
       selection = FormsColumns.FORM_MEDIA_PATH + "!=? AND " +
           FormsColumns.FORM_ID + "=? AND " +
           FormsColumns.FORM_VERSION + " <=?";
      String[] temp = { mediaPath.getAbsolutePath(), fi.formId, fi.formVersion };
      selectionArgs = temp;
     }

     context.getContentResolver().delete(Uri.withAppendedPath(formsProviderContentUri, appName), selection, selectionArgs);

     // See if we have any newer versions already present...
     if ( fi.formVersion == null ) {
       selection = FormsColumns.FORM_MEDIA_PATH + "!=? AND " +
            FormsColumns.FORM_ID + "=? AND " +
            FormsColumns.FORM_VERSION + " IS NOT NULL";
       String[] temp = { mediaPath.getAbsolutePath(), fi.formId };
       selectionArgs = temp;
     } else {
       selection = FormsColumns.FORM_MEDIA_PATH + "!=? AND " +
           FormsColumns.FORM_ID + "=? AND " +
           FormsColumns.FORM_VERSION + " >?";
      String[] temp = { mediaPath.getAbsolutePath(), fi.formId, fi.formVersion };
      selectionArgs = temp;
     }

     try {
       c = context.getContentResolver()
         .query(Uri.withAppendedPath(formsProviderContentUri, appName), null, selection,
               selectionArgs, null);

       if (c.moveToFirst()) {
         // the directory we are processing is stale -- move it to stale directory
         int i = 0;
         File tempMediaPath = new File(ODKFileUtils.getStaleFormsFolder(appName),
               mediaPath.getName() + "_" + Integer.toString(i));
         while (tempMediaPath.exists()) {
            ++i;
            tempMediaPath = new File(ODKFileUtils.getStaleFormsFolder(appName),
                  mediaPath.getName() + "_" + Integer.toString(i));
         }
         FileUtils.moveDirectory(mediaPath, tempMediaPath);
         return;
       }
     } catch (SQLiteException e) {
        e.printStackTrace();
        errors.append(e.toString()).append("\r\n");
        return;
     } catch (IOException e) {
        e.printStackTrace();
        errors.append(e.toString()).append("\r\n");
        return;
    } finally {
        if (c != null && !c.isClosed()) {
           c.close();
        }
     }

     if ( !needUpdate ) {
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
             int count = context.getContentResolver()
                   .update(uri, v, null, null);
             Log.i(t, "[" + instance + "] " + count
                   + " records successfully updated");
          } else {
            context.getContentResolver()
                   .insert(Uri.withAppendedPath(formsProviderContentUri, appName), v);
             Log.i(t, "[" + instance + "] one record successfully inserted");
          }

     } catch (SQLiteException ex) {
        ex.printStackTrace();
        errors.append(ex.toString()).append("\r\n");
        return;
     }
  }

  public final void updateFormInfo(String appName, StringBuilder errors, int instance) {

     File formsDir = new File(ODKFileUtils.getFormsFolder(appName));

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
     if ( candidates != null ) {
       for (File mediaDir : candidates) {
          formDirs.add(mediaDir);
       }
     }

     // sort the directories so we process the ones that are newest first.
     // this will ensure that if there are two directories with the same
     // form definitions, we keep the one with the most recent timestamp.
     Collections.sort(formDirs, new Comparator<File>() {

        @Override
        public int compare(File lhs, File rhs) {
           return (lhs.lastModified() > rhs.lastModified()) ? -1 :
             ((lhs.lastModified() == rhs.lastModified()) ? 0 : 1);
        }
     });

     for (File f : formDirs) {
        Log.i(t,
              "Directory: " + f.getName() + " lastModified: "
                    + f.lastModified());
        updateFormInfo(errors, f, instance);
     }
  }

  @Override
  public void run() {

    // ensure that there is one and only one scan happening at any one time
    // should be handled by ExecutorService, but just in case...
    synchronized (appInstanceCounterStart) {
      Integer ic = appInstanceCounterStart.get(appName);
      if ( ic == null || ic < instanceCounter ) {
        // this task was created after the start of the last task that searched
        // and updated the appName tree. So we should execute it.
        int startCounter = counter;
        Log.i(t, "[" + instanceCounter + "] doInBackground begins! " + appName + " baseCounter: " + ic + " startCounter: " + startCounter);

        try {
           // Process everything then report what didn't work.
           StringBuilder errors = new StringBuilder();

           removeStaleFormInfo(appName, errors, instanceCounter);
           updateFormInfo(appName, errors, instanceCounter);
        } finally {
           Log.i(t, "[" + instanceCounter + "] doInBackground ends! " + appName);
           appInstanceCounterStart.put(appName, startCounter);
        }
      } else {
        Log.i(t, "[" + instanceCounter + "] doInBackground skipped! " + appName + " baseCounter: " + ic);
      }
    }
  }

}
