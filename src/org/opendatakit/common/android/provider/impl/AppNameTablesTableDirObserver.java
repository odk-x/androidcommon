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

import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.os.FileObserver;
import android.util.Log;

/**
 * Monitor changes to a specific table's folder within an appName.
 * i.e., /odk/appName/tables/tableDirName
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameTablesTableDirObserver extends FileObserver {
  private static final String t = "AppNameTablesTableDirObserver";

  private AppNameTablesFolderObserver parent;
  private String tableDirName;
  private AppNameFormsFolderObserver formsWatch = null;
  private boolean stopping = false;

  public AppNameTablesTableDirObserver(AppNameTablesFolderObserver parent, String tableDir) {
    super(parent.getTableDirPath(tableDir), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.tableDirName = tableDir;
    this.parent = parent;

    this.startWatching();
    update();
  }

  public void start() {

    Log.i(t, "start() " + getFormsDirPath(tableDirName));

    if ( formsWatch != null ) {
      formsWatch.start();
    }

  }

  public String getFormsDirPath(String tableDirName) {
    return parent.getTableDirPath(tableDirName) + File.separator + ODKFileUtils.FORMS_FOLDER_NAME;
  }

  private void update() {
    if ( stopping ) return;

    File formsFolder = new File(getFormsDirPath(tableDirName));

    if (formsFolder.exists() && formsFolder.isDirectory()) {
      if (formsWatch == null) {
        addFormsFolderWatch();
      }
    } else {
      removeFormsFolderWatch();
    }
  }

  public void stop() {
    stopping = true;

    this.stopWatching();

    // remove watches on the tables formDef files...
    if (formsWatch != null) {
      formsWatch.stop();
    }
    formsWatch = null;

    Log.i(t, "stop() " + getFormsDirPath(tableDirName));
  }

  private void addFormsFolderWatch() {
    if (formsWatch != null) {
      formsWatch.stop();
    }
    Log.i(t, "addFormsFolderWatch() " + getFormsDirPath(tableDirName));
    formsWatch = new AppNameFormsFolderObserver(this, tableDirName);
  }

  public void removeFormsFolderWatch() {
    if (formsWatch != null) {
      Log.i(t, "removeFormsFolderWatch() " + getFormsDirPath(tableDirName));
      AppNameFormsFolderObserver fo = formsWatch;
      formsWatch = null;
      fo.stop();
      launchFormsDiscovery(null, "monitoring removed: " + getFormsDirPath(tableDirName));
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeTableDirWatch(tableDirName);
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      // find out whether we are still where we think we are -- if not, remove ourselves.
      File f = new File(parent.getTableDirPath(tableDirName));
      if ( !f.exists() ) {
        stop();
        parent.removeTableDirWatch(tableDirName);
      }
      return;
    }

    if ((event & FileObserver.MOVED_TO) != 0) {
      // The folder won't be in the folders list yet... return so we don't do a no-op
      if (formsWatch == null) {
        addFormsFolderWatch();
        return;
      }
    }

    update();
  }

  public void launchFormsDiscovery(String formDir, String reason) {
    parent.launchFormsDiscovery(tableDirName, formDir, reason);
  }
}