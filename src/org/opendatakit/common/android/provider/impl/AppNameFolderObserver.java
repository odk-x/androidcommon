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

  public AppNameFolderObserver(ODKFolderObserver parent, String appName) {
    super(ODKFileUtils.getAppFolder(appName), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.appName = appName;
    this.parent = parent;
    this.startWatching();

    update();
  }

  private void update() {
    File formsFolder = new File(ODKFileUtils.getFormsFolder(appName));

    if ( formsFolder.exists() && formsFolder.isDirectory() ) {
      if ( appNameformsWatch == null ) {
        addFormsFolderWatch();
      }
    } else {
      removeFormsFolderWatch();
    }
  }

  public void stop() {
    active = false;
    this.stopWatching();
    // remove watches on the formDef files...
    if ( appNameformsWatch != null ) {
      appNameformsWatch.stop();
    }
    appNameformsWatch = null;
    Log.i(t, "stop() " + ODKFileUtils.getAppFolder(appName));
  }

  public void addFormsFolderWatch() {
    if ( !active ) return;
    if ( appNameformsWatch != null ) {
      appNameformsWatch.stop();
    }
    appNameformsWatch = new AppNameFormsFolderObserver(this, appName);
  }

  public void removeFormsFolderWatch() {
    if ( !active ) return;
    if ( appNameformsWatch != null ) {
      appNameformsWatch.stop();
      appNameformsWatch = null;
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));
    if ( !active ) return;

    if ( (event & FileObserver.DELETE_SELF) != 0 ) {
      stop();
      parent.removeAppNameWatch(appName);
      return;
    }

    if ( (event & FileObserver.MOVE_SELF) != 0 ) {
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