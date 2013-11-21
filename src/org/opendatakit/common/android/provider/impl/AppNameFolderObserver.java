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
 * Monitor changes to the appName folder i.e., /odk/appName
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameFolderObserver extends FileObserver {
  private static final String t = "AppNameFolderObserver";

  private ODKFolderObserver parent;
  private String appName;
  private boolean stopping = false;

  // the /odk/appName/tables observer...
  private AppNameTablesFolderObserver appNameTablesWatch = null;
  // the /odk/appName/framework observer...
  private AppNameFrameworkFolderObserver appNameFrameworkWatch = null;

  public AppNameFolderObserver(ODKFolderObserver parent, String appName) {
    super(ODKFileUtils.getAppFolder(appName), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.appName = appName;
    this.parent = parent;

    this.startWatching();
    update();
  }
  public String getTablesDirPath() {
    return ODKFileUtils.getTablesFolder(appName);
  }

  public String getFrameworkDirPath() {
    return ODKFileUtils.getFrameworkFolder(appName);
  }

  public void start() {

    Log.i(t, "start() " + ODKFileUtils.getAppFolder(appName));

    if ( appNameTablesWatch != null ) {
      appNameTablesWatch.start();
    }

    if ( appNameFrameworkWatch != null ) {
      appNameFrameworkWatch.start();
    }
  }

  private void update() {
    if ( stopping ) return;
    File tablesFolder = new File(ODKFileUtils.getTablesFolder(appName));

    if (tablesFolder.exists() && tablesFolder.isDirectory()) {
      if (appNameTablesWatch == null) {
        addTablesFolderWatch();
      }
    } else {
      removeTablesFolderWatch();
    }

    File frameworkFolder = new File(ODKFileUtils.getFrameworkFolder(appName));

    if (frameworkFolder.exists() && frameworkFolder.isDirectory()) {
      if (appNameFrameworkWatch == null) {
        addFrameworkFolderWatch();
      }
    } else {
      removeFrameworkFolderWatch();
    }
  }

  public void stop() {
    stopping = true;
    this.stopWatching();

    // remove watches on the tables formDef files...
    if (appNameTablesWatch != null) {
      appNameTablesWatch.stop();
    }
    appNameTablesWatch = null;

    // remove watches on the framework formDef files...
    if (appNameFrameworkWatch != null) {
      appNameFrameworkWatch.stop();
    }
    appNameFrameworkWatch = null;

    Log.i(t, "stop() " + ODKFileUtils.getAppFolder(appName));
  }

  private void addTablesFolderWatch() {
    if (appNameTablesWatch != null) {
      appNameTablesWatch.stop();
    }
    Log.i(t, "addTablesFolderWatch() " + ODKFileUtils.getTablesFolder(appName));
    appNameTablesWatch = new AppNameTablesFolderObserver(this);
  }

  public void removeTablesFolderWatch() {
    if (appNameTablesWatch != null) {
      Log.i(t, "removeTablesFolderWatch() " + ODKFileUtils.getTablesFolder(appName));
      AppNameTablesFolderObserver fo = appNameTablesWatch;
      appNameTablesWatch = null;
      fo.stop();
      launchFormsDiscovery(null, null, "monitoring removed: " + ODKFileUtils.getTablesFolder(appName));
    }
  }

  private void addFrameworkFolderWatch() {
    if (appNameFrameworkWatch != null) {
      appNameFrameworkWatch.stop();
    }
    Log.i(t, "addFrameworkFolderWatch() " + ODKFileUtils.getFrameworkFolder(appName));
    appNameFrameworkWatch = new AppNameFrameworkFolderObserver(this);
  }

  public void removeFrameworkFolderWatch() {
    if (appNameFrameworkWatch != null) {
      Log.i(t, "removeFrameworkFolderWatch() " + ODKFileUtils.getFrameworkFolder(appName));
      AppNameFrameworkFolderObserver fo = appNameFrameworkWatch;
      appNameFrameworkWatch = null;
      fo.stop();
      launchFrameworkDiscovery("monitoring removed: " + ODKFileUtils.getFrameworkFolder(appName));
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeAppNameWatch(appName);
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      // find out whether we are still where we think we are -- if not, remove ourselves.
      File f = new File(ODKFileUtils.getAppFolder(appName));
      if ( !f.exists() ) {
        stop();
        parent.removeAppNameWatch(appName);
      }
      return;
    }

    update();
  }

  public void launchFormsDiscovery(String tableDir, String formDir, String reason) {
    parent.launchFormsDiscovery(appName, tableDir, formDir, reason);
  }

  public void launchFrameworkDiscovery(String reason) {
    parent.launchFrameworkDiscovery(appName, reason);
  }
}