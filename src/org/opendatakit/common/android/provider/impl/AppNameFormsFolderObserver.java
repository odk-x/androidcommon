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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import android.os.FileObserver;
import android.util.Log;

/**
 * Monitor changes to the forms folder within an appName, i.e.,
 * /odk/appName/tables/tableId/forms
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameFormsFolderObserver extends FileObserver {
  private static final String t = "AppNameFormsFolderObserver";

  private AppNameTablesTableDirObserver parent;
  private String tableDirName;
  private boolean stopping = false;

  Map<String, AppNameFormsFormDirObserver> formDirsWatch = new HashMap<String, AppNameFormsFormDirObserver>();

  public AppNameFormsFolderObserver(AppNameTablesTableDirObserver parent, String tableDirName) {
    super(parent.getFormsDirPath(tableDirName), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.tableDirName = tableDirName;
    this.parent = parent;

    this.startWatching();
    update();
  }

  public void start() {

    Log.i(t, "start() " + parent.getFormsDirPath(tableDirName));

    for ( AppNameFormsFormDirObserver obs : formDirsWatch.values() ) {
      obs.start();
    }
  }

  public String getFormDirPath(String formDirName) {
    return parent.getFormsDirPath(tableDirName) + File.separator + formDirName;
  }

  private void update() {
    if ( stopping ) return;

    File formsFolder = new File(parent.getFormsDirPath(tableDirName));

    File[] formDirs = formsFolder.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });

    // formDirs is the list of forms sub-directories. Monitor these for changes.

    if (formDirs != null) {
      // add ones we don't know about
      for (File f : formDirs) {
        String formDirName = f.getName();
        if (!formDirsWatch.containsKey(formDirName)) {
          addFormDirWatch(formDirName);
        }
      }
      // find ones to remove...
      Set<String> toRetain = new HashSet<String>();
      for (File f : formDirs) {
        toRetain.add(f.getName());
      }
      Set<String> toRemove = new HashSet<String>();
      for (String formDirName : formDirsWatch.keySet()) {
        if (!toRetain.contains(formDirName)) {
          toRemove.add(formDirName);
        }
      }
      // remove the ones that are no longer present
      for (String formDirName : toRemove) {
        removeFormDirWatch(formDirName);
      }
    }
  }

  public void stop() {
    stopping = true;
    this.stopWatching();
    // remove watches on the formDef files...
    for (AppNameFormsFormDirObserver fdo : formDirsWatch.values()) {
      fdo.stop();
    }
    formDirsWatch.clear();
    Log.i(t, "stop() " + parent.getFormsDirPath(tableDirName));
  }

  public void addFormDirWatch(String formDir) {
    AppNameFormsFormDirObserver v = formDirsWatch.get(formDir);
    if (v != null) {
      v.stop();
    }
    Log.i(t, "addFormDirWatch() " + getFormDirPath(formDir));
    formDirsWatch.put(formDir, new AppNameFormsFormDirObserver(this, formDir));
  }

  public void removeFormDirWatch(String formDir) {
    AppNameFormsFormDirObserver v = formDirsWatch.get(formDir);
    if (v != null) {
      Log.i(t, "removeFormDirWatch() " + getFormDirPath(formDir));
      formDirsWatch.remove(formDir);
      v.stop();
      launchFormsDiscovery(formDir, "monitoring removed: " + getFormDirPath(formDir));
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeFormsFolderWatch();
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      // find out whether we are still where we think we are -- if not, remove ourselves.
      File f = new File(parent.getFormsDirPath(tableDirName));
      if ( !f.exists() ) {
        stop();
        parent.removeFormsFolderWatch();
      }
      return;
    }

    if ((event & FileObserver.MOVED_TO) != 0) {
      // The folder won't be in the folders list yet... return so we don't do a no-op
      if (path != null && !formDirsWatch.containsKey(path)) {
        addFormDirWatch(path);
        return;
      }
    }

    update();
  }

  public void launchFormsDiscovery(String formDir, String reason) {
    parent.launchFormsDiscovery(formDir, reason);
  }
}