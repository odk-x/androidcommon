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
  private boolean active = true;
  private String appName;
  private String tableDirName;
  private AppNameFormsFolderObserver formsWatch = null;

  public AppNameTablesTableDirObserver(AppNameTablesFolderObserver parent, String appName,
      String tableDir) {
    super(parent.getTableDirPath(tableDir), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.tableDirName = tableDir;
    this.appName = appName;
    this.parent = parent;
    this.startWatching();

    update();
  }

  public String getFormsDirPath(String tableDirName) {
    return ODKFileUtils.getFormsFolder(appName, tableDirName);
  }

  private void update() {

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
    active = false;
    this.stopWatching();

    // remove watches on the tables formDef files...
    if (formsWatch != null) {
      formsWatch.stop();
    }
    formsWatch = null;

    Log.i(t, "stop() " + getFormsDirPath(tableDirName));
  }

  private void addFormsFolderWatch() {
    if (!active)
      return;
    if (formsWatch != null) {
      formsWatch.stop();
    }
    formsWatch = new AppNameFormsFolderObserver(this, appName, tableDirName);
  }

  public void removeFormsFolderWatch() {
    if (!active)
      return;
    if (formsWatch != null) {
      formsWatch.stop();
      formsWatch = null;
      launchFormsDiscovery(null, "monitoring removed: " + getFormsDirPath(tableDirName));
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));
    if (!active)
      return;

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeTableDirWatch(tableDirName);
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      stop();
      parent.removeTableDirWatch(tableDirName);
      return;
    }

    update();
  }

  public void launchFormsDiscovery(String formDir, String reason) {
    parent.launchFormsDiscovery(tableDirName, formDir, reason);
  }
}