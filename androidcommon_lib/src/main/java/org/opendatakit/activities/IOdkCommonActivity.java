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

import android.app.Activity;
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
  String getActiveUser();

  /**
   * Return the current tableId or null if not appropriate.
   */
  String getTableId();

  /**
   * Return the current instanceId or null if not appropriate
   */
  String getInstanceId();

  /**
   * Return the value for the indicated property.
   *
   * @param propertyId
   * @return
   */
  String getProperty(String propertyId);

  /**
   *  for completing the uriFragment of the media attachments */
  /**
   * @return UrlUtils.getWebViewContentUri(this) with a trailing slash
   */
  String getWebViewContentUri();

  /**
   * Store a persistent key-value. This lasts for the duration of this screen and is
   * retained under screen rotations. Useful if browser cookies don't work.
   *
   * @param elementPath
   * @param jsonValue
   */
  void setSessionVariable(String elementPath, String jsonValue);

  /**
   * Retrieve a persistent value for a key. This lasts for the duration of this screen and
   * is retained under screen rotations. Useful if browser cookies don't work.
   *
   * @param elementPath
   * @return
   */
  String getSessionVariable(String elementPath);

  /**
   * Execute an action (intent call).
   *
   * @param dispatchStructAsJSONstring  JSON.stringify(anything) -- typically identifies prompt and
   *                                    user action. If this is null, then the Javascript layer
   *                                    is not notified of the result of this action. It just
   *                                    transparently happens and the webkit might reload as a
   *                                    result of the activity swapping out.
   *
   * @param action    The intent. e.g.,
   *                   org.opendatakit.survey.MediaCaptureImageActivity
   *
   * @param intentObject  an object with the following structure:
   *                   {
   *                         "uri" : intent.setData(value)
   *                         "data" : intent.setData(value)  (preferred over "uri")
   *                         "package" : intent.setPackage(value)
   *                         "type" : intent.setType(value)
   *                         "action" : intent.setAction(value)
   *                         "category" : either a single string or a list of strings for intent.addCategory(item)
   *                         "flags" : the integer code for the values to store
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
   *          "JSONException"         -- there was a problem with the intentObject (did not launch)
   *          "OK"                    -- request issued
   *          "Application not found" -- could not find app to handle intent
   */
  String doAction(String dispatchStructAsJSONstring, String action, JSONObject intentObject);

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
  void queueActionOutcome(String outcome);

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
  void queueUrlChange(String hash);

  /**
   * @return the oldest queued action outcome.
   *   or Url change. Return null if there are none.
   *   Leaves the action on the action queue.
   */
  String viewFirstQueuedAction();

  /**
   * Removes the first queued action, if any.
   */
  void removeFirstQueuedAction();

  /**
   * Calls through to the Activity implementation of this.
   *
   * @param r
   */
  void runOnUiThread(Runnable r);
}
