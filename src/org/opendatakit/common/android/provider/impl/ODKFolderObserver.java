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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.os.FileObserver;
import android.util.Log;

/**
 * Monitor for changes to the /odk folder. Changes in the odk folder tree may
 * trigger a rescan of the forms.
 *
 * @author mitchellsundt@gmail.com
 *
 */
class ODKFolderObserver extends FileObserver {
  private static final String t = "AppFoldersObserver";

  // additional values that might be on the event sent to the callback

  /* Backing fs was unmounted */
  // private static final int IN_UNMOUNT = 0x00002000;
  /* Event queued overflowed */
  // private static final int IN_Q_OVERFLOW = 0x00004000;
  /* File was ignored */
  // private static final int IN_IGNORED = 0x00008000;

  // monitoring flags...
  static final int LIKELY_CHANGE_OF_SUBDIR = FileObserver.CREATE | FileObserver.MOVED_FROM
      | FileObserver.MOVED_TO | FileObserver.DELETE | FileObserver.DELETE_SELF
      | FileObserver.MOVE_SELF;

  static final int LIKELY_CHANGE_OF_FILE = FileObserver.CREATE | FileObserver.CLOSE_WRITE
      | FileObserver.MODIFY | FileObserver.MOVED_FROM | FileObserver.MOVED_TO
      | FileObserver.DELETE_SELF | FileObserver.MOVE_SELF;

  private FormsProviderImpl self;

  private boolean active = true;

  // A map of appName => observer for /odk/appName changes
  private Map<String, AppNameFolderObserver> appNameFoldersWatch = new HashMap<String, AppNameFolderObserver>();

  public ODKFolderObserver(FormsProviderImpl self) {
    super(ODKFileUtils.getOdkFolder(), LIKELY_CHANGE_OF_SUBDIR);
    this.self = self;
    this.startWatching();

    update();
  }

  private void update() {

    File[] appFolders = ODKFileUtils.getAppFolders();

    if (appFolders != null) {
      // add change listeners for any new app folder...
      for (File f : appFolders) {
        String appName = f.getName();
        if (!appNameFoldersWatch.containsKey(appName)) {
          addAppNameWatch(appName);
        }
      }

      // find ones to remove...
      Set<String> toRetain = new HashSet<String>();
      for (File f : appFolders) {
        toRetain.add(f.getName());
      }
      Set<String> toRemove = new HashSet<String>();
      for (String appName : appNameFoldersWatch.keySet()) {
        if (!toRetain.contains(appName)) {
          toRemove.add(appName);
        }
      }
      // remove the ones that are no longer present
      for (String appName : toRemove) {
        removeAppNameWatch(appName);
      }
    }
  }

  public void stop() {
    active = false;
    this.stopWatching();
    // remove watches on the formDef files...
    for (AppNameFolderObserver fdo : appNameFoldersWatch.values()) {
      fdo.stop();
    }
    appNameFoldersWatch.clear();
    Log.i(t, "stop() " + ODKFileUtils.getOdkFolder());
  }

  public void addAppNameWatch(String appNameFolder) {
    if (!active)
      return;
    AppNameFolderObserver v = appNameFoldersWatch.get(appNameFolder);
    if (v != null) {
      v.stop();
    }
    appNameFoldersWatch.put(appNameFolder, new AppNameFolderObserver(this, appNameFolder));
  }

  public void removeAppNameWatch(String appNameFolder) {
    if (!active)
      return;
    AppNameFolderObserver v = appNameFoldersWatch.get(appNameFolder);
    if (v != null) {
      appNameFoldersWatch.remove(appNameFolder);
      v.stop();
    }
  }

  public void launchFormsDiscovery(String appName, String reason) {
    // monitoring changes in the forms folders...
    FormsDiscoveryRunnable fd = new FormsDiscoveryRunnable(self, appName);
    FormsProviderImpl.executor.execute(fd);
    Log.i(t, reason);
  }

  public static String eventMap(int event) {
    StringBuilder b = new StringBuilder();
    if ((event & FileObserver.ACCESS) != 0) {
      b.append(" ACCESS");
    } else if ((event & FileObserver.ATTRIB) != 0) {
      b.append(" ATTRIB");
    } else if ((event & FileObserver.CLOSE_NOWRITE) != 0) {
      b.append(" CLOSE_NOWRITE");
    } else if ((event & FileObserver.CLOSE_WRITE) != 0) {
      b.append(" CLOSE_WRITE");
    } else if ((event & FileObserver.CREATE) != 0) {
      b.append(" CREATE");
    } else if ((event & FileObserver.DELETE) != 0) {
      b.append(" DELETE");
    } else if ((event & FileObserver.DELETE_SELF) != 0) {
      b.append(" DELETE_SELF");
    } else if ((event & FileObserver.MODIFY) != 0) {
      b.append(" MODIFY");
    } else if ((event & FileObserver.MOVE_SELF) != 0) {
      b.append(" MOVE_SELF");
    } else if ((event & FileObserver.MOVED_FROM) != 0) {
      b.append(" MOVED_FROM");
    } else if ((event & FileObserver.MOVED_TO) != 0) {
      b.append(" MOVED_TO");
    } else if ((event & FileObserver.OPEN) != 0) {
      b.append(" OPEN");
    }
    return b.toString();
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + eventMap(event));
    if (!active)
      return;

    if ((event & FileObserver.DELETE_SELF) != 0) {
      stop();
      FormsProviderImpl.stopScan();
      return;
    }

    if ((event & FileObserver.MOVE_SELF) != 0) {
      stop();
      FormsProviderImpl.stopScan();
      return;
    }

    update();
  }
}