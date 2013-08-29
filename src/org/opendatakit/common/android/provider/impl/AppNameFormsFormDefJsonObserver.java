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

import android.os.FileObserver;
import android.util.Log;

/**
 * Monitors the /odk/appName/forms/formDir/formDef.json file for changes. If a
 * change is detected, fires off a rescan of the appName containing that file.
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameFormsFormDefJsonObserver extends FileObserver {
  private static final String t = "AppNameFormsFormDefJsonObserver";

  private AppNameFormsFormDirObserver parent;
  private long lastModificationTime = -1L;
  private boolean stopping = false;

  public AppNameFormsFormDefJsonObserver(AppNameFormsFormDirObserver parent) {
    super(parent.getFormDefJsonFilePath(),
        ODKFolderObserver.LIKELY_CHANGE_OF_FILE);
    this.parent = parent;

    this.startWatching();
    update();
  }

  public void start() {
    // notify parent we are beginning monitoring...
    File formDefJson = new File(parent.getFormDefJsonFilePath());

    Log.i(t, "start() " + formDefJson.getAbsolutePath());
    parent.launchFormsDiscovery("monitoring added: " + formDefJson.getAbsolutePath());
  }

  private void update() {
    if ( stopping ) return;

    File formDefJson = new File(parent.getFormDefJsonFilePath());

    if (formDefJson.exists() && formDefJson.isFile()) {
      boolean first = (lastModificationTime == -1L);
      long modTime = formDefJson.lastModified();
      if (modTime != lastModificationTime) {
        lastModificationTime = modTime;
        if ( !first ) {
          parent.launchFormsDiscovery("changed: " + formDefJson.getAbsolutePath());
        }
      }
    } else {
      parent.removeFormDefJsonWatch();
    }
  }

  public void stop() {
    stopping = true;
    this.stopWatching();

    File formDefJson = new File(parent.getFormDefJsonFilePath());
    Log.i(t, "stop() " + formDefJson.getAbsolutePath());
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeFormDefJsonWatch();
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      stop();
      parent.removeFormDefJsonWatch();
      return;
    }

    update();
  }
}