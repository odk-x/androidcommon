package org.opendatakit.common.android.provider.impl;

import java.io.File;

import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.os.FileObserver;
import android.util.Log;

/**
 * Monitor changes to a specific form's folder within an appName.
 * Only pay attention to changes to the existence of the formDef.json file.
 *
 * i.e., /odk/appName/forms/formDirName
 *
 * @author mitchellsundt@gmail.com
 *
 */
class AppNameFormsFormDirObserver extends FileObserver {
  private static final String t = "AppNameFormsObserver";

  private AppNameFormsFolderObserver parent;
  private boolean active = true;
  private String formDirName;
  private AppNameFormDefJsonObserver formDefJsonWatch = null;

  public AppNameFormsFormDirObserver(AppNameFormsFolderObserver parent, String appName, String formDir) {
    super(parent.getFormDirPath(formDir), ODKFolderObserver.LIKELY_CHANGE_OF_SUBDIR);
    this.formDirName = formDir;
    this.parent = parent;
    this.startWatching();

    update();
  }

  public String getFormDirPath() {
    return parent.getFormDirPath(formDirName);
  }

  public void update() {
    File formDirFolder = new File(getFormDirPath());
    File formDefJson = new File(formDirFolder, ODKFileUtils.FORMDEF_JSON_FILENAME);

    if ( formDefJson.exists() && formDefJson.isFile() ) {
      if ( formDefJsonWatch == null ) {
        addFormDefJsonWatch();
      }
    } else if ( formDefJsonWatch != null ) {
      removeFormDefJsonWatch();
    }
  }

  public void stop() {
    active = false;
    this.stopWatching();
    // remove watch on the formDef files...
    if ( formDefJsonWatch != null ) {
      formDefJsonWatch.stop();
    }
    formDefJsonWatch = null;
    Log.i(t, "stop() " + getFormDirPath());
  }

  public void addFormDefJsonWatch() {
    if ( !active ) return;
    if ( formDefJsonWatch != null ) {
      formDefJsonWatch.stop();
    }
    formDefJsonWatch = new AppNameFormDefJsonObserver(this);
  }

  public void removeFormDefJsonWatch() {
    if ( !active ) return;
    if ( formDefJsonWatch != null ) {
      formDefJsonWatch.stop();
      formDefJsonWatch = null;

      File formDirFolder = new File(getFormDirPath());
      File formDefJson = new File(formDirFolder, ODKFileUtils.FORMDEF_JSON_FILENAME);
      launchFormsDiscovery("monitoring removed: " + formDefJson.getAbsolutePath());
    }
  }

  @Override
  public void onEvent(int event, String path) {
    Log.i(t, "onEvent: " + path + " event: " + ODKFolderObserver.eventMap(event));
    if ( !active ) return;

    if ( (event & FileObserver.DELETE_SELF) != 0 ) {
      stop();
      parent.removeFormDirWatch(formDirName);
      return;
    }

    if ( (event & FileObserver.MOVE_SELF) != 0 ) {
      stop();
      parent.removeFormDirWatch(formDirName);
      return;
    }

    update();
  }

  public void launchFormsDiscovery(String reason) {
    parent.launchFormsDiscovery(reason);
  }
}