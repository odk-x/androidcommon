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

import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.os.FileObserver;
import android.util.Log;

/**
 * Monitor changes to the forms folder within an appName, i.e.,
 * /odk/appName/forms
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameFormsFolderObserver extends FileObserver {
  private static final String t = "AppNameFormsObserver";

  private AppNameFolderObserver parent;
  private boolean active = true;
  private String appName;

  Map<String, AppNameFormsFormDirObserver> formDirsWatch = new HashMap<String, AppNameFormsFormDirObserver>();

  public AppNameFormsFolderObserver(AppNameFolderObserver parent, String appName) {
    super(ODKFileUtils.getFormsFolder(appName), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.appName = appName;
    this.parent = parent;
    this.startWatching();

    update();
  }

  public String getFormDirPath(String formDirName) {
    return ODKFileUtils.getFormsFolder(appName) + File.separator + formDirName;
  }

  private void update() {

    File formsFolder = new File(ODKFileUtils.getFormsFolder(appName));

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
    active = false;
    this.stopWatching();
    // remove watches on the formDef files...
    for (AppNameFormsFormDirObserver fdo : formDirsWatch.values()) {
      fdo.stop();
    }
    formDirsWatch.clear();
    Log.i(t, "stop() " + ODKFileUtils.getFormsFolder(appName));
  }

  public void addFormDirWatch(String formDir) {
    if (!active)
      return;
    AppNameFormsFormDirObserver v = formDirsWatch.get(formDir);
    if (v != null) {
      v.stop();
    }
    formDirsWatch.put(formDir, new AppNameFormsFormDirObserver(this, appName, formDir));
  }

  public void removeFormDirWatch(String formDir) {
    if (!active)
      return;
    AppNameFormsFormDirObserver v = formDirsWatch.get(formDir);
    if (v != null) {
      formDirsWatch.remove(formDir);
      v.stop();
      launchFormsDiscovery("monitoring removed: " + getFormDirPath(formDir));
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));
    if (!active)
      return;

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      parent.removeFormsFolderWatch();
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      stop();
      parent.removeFormsFolderWatch();
      return;
    }

    update();
  }

  public void launchFormsDiscovery(String reason) {
    parent.launchFormsDiscovery(reason);
  }
}