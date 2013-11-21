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
 * Monitor changes to the tables folder within an appName, i.e.,
 * /odk/appName/tables
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameTablesFolderObserver extends FileObserver {
  private static final String t = "AppNameTablesFolderObserver";

  private AppNameFolderObserver parent;
  private boolean stopping = false;

  Map<String, AppNameTablesTableDirObserver> tableDirsWatch = new HashMap<String, AppNameTablesTableDirObserver>();

  public AppNameTablesFolderObserver(AppNameFolderObserver parent) {
    super(parent.getTablesDirPath(), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.parent = parent;

    this.startWatching();
    update();
  }

  public String getTableDirPath(String tableDirName) {
    return parent.getTablesDirPath() + File.separator + tableDirName;
  }

  public void start() {

    Log.i(t, "start() " + parent.getTablesDirPath());

    for ( AppNameTablesTableDirObserver obs : tableDirsWatch.values() ) {
      obs.start();
    }

  }

  private void update() {
    if ( stopping ) return;

    File tablesFolder = new File(parent.getTablesDirPath());

    File[] tableDirs = tablesFolder.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });

    // formDirs is the list of forms sub-directories. Monitor these for changes.

    if (tableDirs != null) {
      // add ones we don't know about
      for (File f : tableDirs) {
        String tableDirName = f.getName();
        if (!tableDirsWatch.containsKey(tableDirName)) {
          addTableDirWatch(tableDirName);
        }
      }
      // find ones to remove...
      Set<String> toRetain = new HashSet<String>();
      for (File f : tableDirs) {
        toRetain.add(f.getName());
      }
      Set<String> toRemove = new HashSet<String>();
      for (String tableDirName : tableDirsWatch.keySet()) {
        if (!toRetain.contains(tableDirName)) {
          toRemove.add(tableDirName);
        }
      }
      // remove the ones that are no longer present
      for (String tableDirName : toRemove) {
        removeTableDirWatch(tableDirName);
      }
    }
  }

  public void stop() {
    stopping = true;

    this.stopWatching();
    // remove watches on the formDef files...
    for (AppNameTablesTableDirObserver fdo : tableDirsWatch.values()) {
      fdo.stop();
    }
    tableDirsWatch.clear();
    Log.i(t, "stop() " + parent.getTablesDirPath());
  }

  public void addTableDirWatch(String tableDir) {
    AppNameTablesTableDirObserver v = tableDirsWatch.get(tableDir);
    if (v != null) {
      tableDirsWatch.remove(tableDir);
      v.stop();
    }
    Log.i(t, "addTableDirWatch() " + getTableDirPath(tableDir));
    tableDirsWatch.put(tableDir, new AppNameTablesTableDirObserver(this, tableDir));
  }

  public void removeTableDirWatch(String tableDir) {
    AppNameTablesTableDirObserver v = tableDirsWatch.get(tableDir);
    if (v != null) {
      Log.i(t, "removeTableDirWatch() " + getTableDirPath(tableDir));
      tableDirsWatch.remove(tableDir);
      v.stop();
      launchFormsDiscovery(tableDir, null, "monitoring removed: " + getTableDirPath(tableDir));
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeTablesFolderWatch();
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      // find out whether we are still where we think we are -- if not, remove ourselves.
      File f = new File(parent.getTablesDirPath());
      if ( !f.exists() ) {
        stop();
        parent.removeTablesFolderWatch();
      }
      return;
    }

    if ((event & FileObserver.MOVED_TO) != 0) {
      // The folder won't be in the folders list yet... return so we don't do a no-op
      if (path != null && !tableDirsWatch.containsKey(path)) {
        addTableDirWatch(path);
        return;
      }
    }

    update();
  }

  public void launchFormsDiscovery(String tableDir, String formDir, String reason) {
    parent.launchFormsDiscovery(tableDir, formDir, reason);
  }
}