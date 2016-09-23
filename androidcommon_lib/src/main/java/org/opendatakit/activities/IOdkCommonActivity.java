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

package org.opendatakit.activities;

import org.json.JSONObject;

/**
 * Interface that implements the odkCommon callbacks from the WebKit.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public interface IOdkCommonActivity extends IAppAwareActivity, IInitResumeActivity {

  /**
   * Prefer the email address of the user or return an assigned username
   *
   * @return  "mailto:" + getProperty(PropertyManager.EMAIL) or if not defined,
   *          "username:" + getProperty(PropertyManager.USERNAME)
   */
  public String getActiveUser();

  /**
   * Return the value for the indicated property.
   *
   * @param propertyId
   * @return
   */
  public String getProperty(String propertyId);

  /**
   *  for completing the uriFragment of the media attachments */
  /**
   * @return UrlUtils.getWebViewContentUri(this) with a trailing slash
   */
  public String getWebViewContentUri();

  /**
   * Store a persistent key-value. This lasts for the duration of this screen and is
   * retained under screen rotations. Useful if browser cookies don't work.
   *
   * @param elementPath
   * @param jsonValue
   */
  public void setSessionVariable(String elementPath, String jsonValue);

  /**
   * Retrieve a persistent value for a key. This lasts for the duration of this screen and
   * is retained under screen rotations. Useful if browser cookies don't work.
   *
   * @param elementPath
   * @return
   */
  public String getSessionVariable(String elementPath);

  /**
   * Execute an action (intent call).
   *
   * @param dispatchString   Opaque string -- typically identifies prompt and user action
   *
   * @param action    The intent. e.g.,
   *                   org.opendatakit.survey.MediaCaptureImageActivity
   *
   * @param valueMap  Map of the following structure:
   *                   {
   *                         "uri" : intent.setData(value)
   *                         "data" : intent.setData(value)  (preferred over "uri")
   *                         "package" : intent.setPackage(value)
   *                         "type" : intent.setType(value)
   *                         "extras" : { key-value map describing extras bundle }
   *                   }
   *
   *                  Within the extras, if a value is of the form:
   *                     opendatakit-macro(name)
   *                  then substitute this with the result of getProperty(name)
   *
   *                  If the action begins with "org.opendatakit." then we also
   *                  add an "appName" property into the intent extras if it was
   *                  not specified.
   *
   * @return one of:
   *          "IGNORE"                -- there is already a pending action
   *          "JSONException: " + ex.toString()
   *          "OK"                    -- request issued
   *          "Application not found" -- could not find app to handle intent
   */
  public String doAction(String dispatchString, String action, JSONObject valueMap);

  /**
   * Queue the outcome of a doAction along with its UI element specifiers.  This is used
   * by implementing Activities to enqueue the action results.  The view is separately
   * notified of the availability of the results via view.signalQueuedActionAvailable()
   *
   * @param outcome is a JSON.stringify of:
   * 
   * { dispatchString: dispatchStringWaitingForData,
   *   action: actionWaitingForData,
   *   jsonValue: { status: integerOutcome,
   *                result: {key-value map of extras returned in the result intent} } }
   */
  public void queueActionOutcome(String outcome);

  /**
   * Queue a hash change action (initiated by the Java side).
   * This is enqueued on the same queue as action outcomes so that
   * if the java side navigates within the webpage, the JS
   * properly executes that navigation before processing any
   * subsequent action outcome. At this time, this is not used
   * in ODK Tables.
   *
   * @param hash
   */
  public void queueUrlChange(String hash);

  /**
   * @return the oldest queued action outcome.
   *   or Url change. Return null if there are none.
   *   Leaves the action on the action queue.
   */
  public String viewFirstQueuedAction();

  /**
   * Removes the first queued action, if any.
   */
  public void removeFirstQueuedAction();

  /**
   * Calls through to the Activity implementation of this.
   *
   * @param r
   */
  public void runOnUiThread(Runnable r);
}
