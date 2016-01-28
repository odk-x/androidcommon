/*
 * Copyright (C) 2012-2013 University of Washington
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

package org.opendatakit.common.android.activities;

import org.json.JSONObject;

import android.graphics.Bitmap;
import android.net.Uri;
import android.view.View;
import org.opendatakit.common.android.views.ICallbackFragment;

/**
 * Interface that implements some of the shim.js callbacks from the WebKit.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public interface ODKActivity extends IAppAwareActivity, IInitResumeActivity {

  public String getUrlBaseLocation(boolean ifChanged);

  public String getUrlLocationHash();

  public String getUploadTableId();

  public String getActiveUser();

  public String getProperty(String propertyId);

  /** for completing the uriFragment of the media attachments */
  public String getWebViewContentUri();

  public String getRefId();

  public void setInstanceId(String instanceId);

  public String getInstanceId();

  public void pushSectionScreenState();

  public void setSectionScreenState(String screenPath, String state);

  public void clearSectionScreenState();

  public String getControllerState();

  public String getScreenPath();

  public boolean hasScreenHistory();

  public String popScreenHistory();

  public boolean hasSectionStack();

  public String popSectionStack();

  public void setSessionVariable( String elementPath, String jsonValue );

  public String getSessionVariable( String elementPath );

  public void saveAllChangesCompleted(String instanceId, boolean asComplete);

  public void saveAllChangesFailed(String instanceId);

  public void ignoreAllChangesCompleted(String instanceId);

  public void ignoreAllChangesFailed(String instanceId);

  public String doAction(String page, String path, String action, JSONObject valueMap);

  /**
   * Queue the outcome of a doAction along with its UI element specifiers.
   * This is a JSON.stringify of:
   * 
   * { page: pageWaitingForData, 
   *   path: pathWaitingForData,
   *   action: actionWaitingForData,
   *   jsonValue: { status: integerOutcome ... } }
   *   
   * Within the jsonValue, if there are return values in the
   * result intent, they are under a result key within the jsonValue.
   * 
   * @param outcome -- serialization of the above structure.
   */
  public void queueActionOutcome(String outcome);
  
  /**
   * Queue a hash change action (initiated by the Java side).
   * 
   * @param hash
   */
  public void queueUrlChange(String hash);
  
  /**
   * @return the oldest recorded action outcome 
   *   or Url change. Return null if there are none.
   */
  public String getFirstQueuedAction();

  // for FormChooserListFragment
  public void chooseForm(Uri formUri);

  // for InstanceUploaderTableChooserListFragment
  public void chooseInstanceUploaderTable(String tableId);

  public ICallbackFragment getCallbackFragment();

  /**
   * Use the Activity implementation of this.
   *
   * @param r
   */
  public void runOnUiThread(Runnable r);
}
