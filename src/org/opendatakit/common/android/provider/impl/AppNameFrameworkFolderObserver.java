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
 * Monitor changes to the framework folder within an appName. Only pay
 * attention to changes to the existence of the formDef.json file.
 *
 * i.e., /odk/appName/framework
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameFrameworkFolderObserver extends FileObserver {
  private static final String t = "AppNameFrameworkFolderObserver";

  private AppNameFolderObserver parent;
  private AppNameFrameworkFormDefJsonObserver formDefJsonWatch = null;
  private boolean stopping = false;

  public AppNameFrameworkFolderObserver(AppNameFolderObserver parent) {
    super(parent.getFrameworkDirPath(), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.parent = parent;

    this.startWatching();
    update();
  }

  public String getFrameworkFormDefJsonFilePath() {
    return parent.getFrameworkDirPath() + File.separator + ODKFileUtils.FORMDEF_JSON_FILENAME;
  }

  public void start() {

    Log.i(t, "start() " + parent.getFrameworkDirPath());

    if ( formDefJsonWatch != null ) {
      formDefJsonWatch.start();
    }

  }

  public void update() {
    if ( stopping ) return;

    File formDefJson = new File(getFrameworkFormDefJsonFilePath());

    if (formDefJson.exists() && formDefJson.isFile()) {
      if (formDefJsonWatch == null) {
        addFormDefJsonWatch();
      }
    } else if (formDefJsonWatch != null) {
      removeFormDefJsonWatch();
    }
  }

  public void stop() {
    stopping = true;

    this.stopWatching();
    // remove watch on the formDef files...
    if (formDefJsonWatch != null) {
      formDefJsonWatch.stop();
    }
    formDefJsonWatch = null;
    Log.i(t, "stop() " + parent.getFrameworkDirPath());
  }

  public void addFormDefJsonWatch() {
    if (formDefJsonWatch != null) {
      formDefJsonWatch.stop();
    }
    Log.i(t, "addFormDefJsonWatch() " + getFrameworkFormDefJsonFilePath());
    formDefJsonWatch = new AppNameFrameworkFormDefJsonObserver(this);
  }

  public void removeFormDefJsonWatch() {
    if (formDefJsonWatch != null) {
      Log.i(t, "removeFormDefJsonWatch() " + getFrameworkFormDefJsonFilePath());
      AppNameFrameworkFormDefJsonObserver fo = formDefJsonWatch;
      formDefJsonWatch = null;
      fo.stop();

      File formDefJson = new File(getFrameworkFormDefJsonFilePath());
      launchFrameworkDiscovery("monitoring removed: " + formDefJson.getAbsolutePath());
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeFrameworkFolderWatch();
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      // find out whether we are still where we think we are -- if not, remove ourselves.
      File f = new File(parent.getFrameworkDirPath());
      if ( !f.exists() ) {
        stop();
        parent.removeFrameworkFolderWatch();
      }
      return;
    }

    if ((event & FileObserver.MOVED_TO) != 0) {
      // The folder won't be in the folders list yet... return so we don't do a no-op
      if (formDefJsonWatch == null) {
        addFormDefJsonWatch();
        return;
      }
    }

    update();
  }

  public void launchFrameworkDiscovery(String reason) {
    parent.launchFrameworkDiscovery(reason);
  }
}