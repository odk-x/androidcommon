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
  private static final String t = "AppNameFormsObserver";

  private ODKFolderObserver parent;
  private boolean active = true;
  private String appName;

  // the /odk/appName/forms observer...
  private AppNameFormsFolderObserver appNameformsWatch = null;
  // the /odk/appName/framework observer...
  private AppNameFrameworkFolderObserver appNameframeworkWatch = null;

  public AppNameFolderObserver(ODKFolderObserver parent, String appName) {
    super(ODKFileUtils.getAppFolder(appName), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.appName = appName;
    this.parent = parent;
    this.startWatching();

    update();
  }

  private void update() {
    File formsFolder = new File(ODKFileUtils.getFormsFolder(appName));

    if (formsFolder.exists() && formsFolder.isDirectory()) {
      if (appNameformsWatch == null) {
        addFormsFolderWatch();
      }
    } else {
      removeFormsFolderWatch();
    }

    File frameworkFolder = new File(ODKFileUtils.getFrameworkFolder(appName));

    if (frameworkFolder.exists() && frameworkFolder.isDirectory()) {
      if (appNameframeworkWatch == null) {
        addFrameworkFolderWatch();
      }
    } else {
      removeFrameworkFolderWatch();
    }
  }

  public void stop() {
    active = false;
    this.stopWatching();
    // remove watches on the forms formDef files...
    if (appNameformsWatch != null) {
      appNameformsWatch.stop();
    }
    appNameformsWatch = null;

    // remove watches on the framework formDef files...
    if (appNameframeworkWatch != null) {
      appNameframeworkWatch.stop();
    }
    appNameframeworkWatch = null;

    Log.i(t, "stop() " + ODKFileUtils.getAppFolder(appName));
  }

  private void addFormsFolderWatch() {
    if (!active)
      return;
    if (appNameformsWatch != null) {
      appNameformsWatch.stop();
    }
    appNameformsWatch = new AppNameFormsFolderObserver(this, appName);
  }

  public void removeFormsFolderWatch() {
    if (!active)
      return;
    if (appNameformsWatch != null) {
      appNameformsWatch.stop();
      appNameformsWatch = null;
      launchFormsDiscovery("monitoring removed: " + ODKFileUtils.getFormsFolder(appName));
    }
  }

  private void addFrameworkFolderWatch() {
    if (!active)
      return;
    if (appNameframeworkWatch != null) {
      appNameframeworkWatch.stop();
    }
    appNameframeworkWatch = new AppNameFrameworkFolderObserver(this, appName);
  }

  public void removeFrameworkFolderWatch() {
    if (!active)
      return;
    if (appNameframeworkWatch != null) {
      appNameframeworkWatch.stop();
      appNameframeworkWatch = null;
      launchFormsDiscovery("monitoring removed: " + ODKFileUtils.getFrameworkFolder(appName));
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));
    if (!active)
      return;

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeAppNameWatch(appName);
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      stop();
      parent.removeAppNameWatch(appName);
      return;
    }

    update();
  }

  public void launchFormsDiscovery(String reason) {
    parent.launchFormsDiscovery(appName, reason);
  }
}