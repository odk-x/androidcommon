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
 * Monitor changes to a specific form's folder within an appName. Only pay
 * attention to changes to the existence of the formDef.json file.
 *
 * i.e., /odk/appName/forms/formDirName
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameFormsFormDirObserver extends FileObserver {
  private static final String t = "AppNameFormsFormDirObserver";

  private AppNameFormsFolderObserver parent;
  private String formDirName;
  private AppNameFormsFormDefJsonObserver formDefJsonWatch = null;
  private boolean stopping = false;

  public AppNameFormsFormDirObserver(AppNameFormsFolderObserver parent,
      String formDir) {
    super(parent.getFormDirPath(formDir), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.formDirName = formDir;
    this.parent = parent;

    this.startWatching();
    update();
  }

  public void start() {

    Log.i(t, "start() " + parent.getFormDirPath(formDirName));

    if ( formDefJsonWatch != null ) {
      formDefJsonWatch.start();
    }

  }

  public String getFormDefJsonFilePath() {
    return parent.getFormDirPath(formDirName) + File.separator + ODKFileUtils.FORMDEF_JSON_FILENAME;
  }

  public void update() {
    if ( stopping ) return;

    File formDefJson = new File(getFormDefJsonFilePath());

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
    Log.i(t, "stop() " + parent.getFormDirPath(formDirName));
  }

  public void addFormDefJsonWatch() {
    if (formDefJsonWatch != null) {
      formDefJsonWatch.stop();
    }
    formDefJsonWatch = new AppNameFormsFormDefJsonObserver(this);
  }

  public void removeFormDefJsonWatch() {
    if (formDefJsonWatch != null) {
      formDefJsonWatch.stop();
      formDefJsonWatch = null;

      File formDefJson = new File(getFormDefJsonFilePath());
      launchFormsDiscovery("monitoring removed: " + formDefJson.getAbsolutePath());
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeFormDirWatch(formDirName);
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      stop();
      parent.removeFormDirWatch(formDirName);
      return;
    }

    update();
  }

  public void launchFormsDiscovery(String reason) {
    parent.launchFormsDiscovery(formDirName, reason);
  }
}