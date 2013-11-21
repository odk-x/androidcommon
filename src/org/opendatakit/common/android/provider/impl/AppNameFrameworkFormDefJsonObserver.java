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
 * Monitors the /odk/appName/framework/formDir/formDef.json file for changes. If
 * a change is detected, fires off a rescan of the appName containing that file.
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameFrameworkFormDefJsonObserver extends FileObserver {
  private static final String t = "AppNameFrameworkFormDefJsonObserver";

  private AppNameFrameworkFolderObserver parent;
  private long lastModificationTime = -1L;
  private boolean stopping = false;

  public AppNameFrameworkFormDefJsonObserver(AppNameFrameworkFolderObserver parent) {
    super(parent.getFrameworkFormDefJsonFilePath(),
        ODKFolderObserver.LIKELY_CHANGE_OF_FILE);
    this.parent = parent;

    this.startWatching();
    update();
  }

  public void start() {

    File formDefJson = new File(parent.getFrameworkFormDefJsonFilePath());

    Log.i(t, "start() " + formDefJson.getAbsolutePath());
    parent.launchFrameworkDiscovery("monitoring added: " + formDefJson.getAbsolutePath());

  }

  private void update() {
    if ( stopping ) return;

    File formDefJson = new File(parent.getFrameworkFormDefJsonFilePath());

    if (formDefJson.exists() && formDefJson.isFile()) {
      long modTime = formDefJson.lastModified();
      if (modTime != lastModificationTime) {
        lastModificationTime = modTime;
        parent.launchFrameworkDiscovery("changed: " + formDefJson.getAbsolutePath());
      }
    } else {
      parent.removeFormDefJsonWatch();
    }
  }

  public void stop() {
    stopping = true;

    this.stopWatching();
    File formDefJson = new File(parent.getFrameworkFormDefJsonFilePath());

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
      // find out whether we are still where we think we are -- if not, remove ourselves.
      File f = new File(parent.getFrameworkFormDefJsonFilePath());
      if ( !f.exists() ) {
        stop();
        parent.removeFormDefJsonWatch();
      }
      return;
    }

    update();
  }
}