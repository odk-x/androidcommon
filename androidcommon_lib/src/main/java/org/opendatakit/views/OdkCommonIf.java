/*
 * Copyright (C) 2015 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.views;

import java.lang.ref.WeakReference;

/**
 * Created by clarice on 11/3/15.
 */
public class OdkCommonIf {

  public static final String TAG = "OdkCommonIf";

  private WeakReference<OdkCommon> weakControl;

  OdkCommonIf(OdkCommon odkCommon) {
    weakControl = new WeakReference<OdkCommon>(odkCommon);
  }

  private boolean isInactive() {
    return (weakControl.get() == null) || (weakControl.get().isInactive());
  }

  /**
   * Return the platform info as a stringified json object. This is an object
   * containing the keys: container, version, appName, baseUri, logLevel.
   *
   * @return a stringified json object with the above keys
   */
  @android.webkit.JavascriptInterface public String getPlatformInfo() {
    if (isInactive())
      return null;
    return weakControl.get().getPlatformInfo();
  }

  /**
   * Take the path of a file relative to the app folder and return a url by
   * which it can be accessed.
   *
   * @param relativePath
   * @return an absolute URI to the file
   */
  @android.webkit.JavascriptInterface public String getFileAsUrl(String relativePath) {
    if (isInactive())
      return null;
    return weakControl.get().getFileAsUrl(relativePath);
  }

  /**
   * Convert the rowpath value for a media attachment (e.g., uriFragment) field
   * into a url by which it can be accessed.
   *
   * @param tableId
   * @param rowId
   * @param rowPathUri
   * @return
   */
  @android.webkit.JavascriptInterface public String getRowFileAsUrl(String tableId, String rowId,
      String rowPathUri) {
    if (isInactive())
      return null;
    return weakControl.get().getRowFileAsUrl(tableId, rowId, rowPathUri);
  }

  /**
   * @param tableId required.
   * @param formId  may be null. If null, screenPath and elementKeyToStringifiedValue must be null
   * @param instanceId may be null.
   * @param screenPath may be null.
   * @param jsonMap may be null or empty. JSON stringify of a map of elementKey -to-
   *                                      value for that elementKey. Used to
   *                                      initialized field values and session variables.
   * @return URI for this survey and its arguments.
   */
  @android.webkit.JavascriptInterface public String constructSurveyUri(String tableId,
      String formId, String instanceId, String screenPath,
      String jsonMap) {
    if (isInactive())
      return null;
    return weakControl.get().constructSurveyUri(tableId,
        formId, instanceId, screenPath, jsonMap);
  }

  /**
   * Log messages using WebLogger.
   *
   * @param level         - levels are A, D, E, I, S, V, W
   * @param loggingString - actual message to log
   * @return
   */
  @android.webkit.JavascriptInterface public void log(String level, String loggingString) {
    if (isInactive())
      return;
    weakControl.get().log(level, loggingString);
  }

  /**
   * Get active user
   *
   * @return
   */
  @android.webkit.JavascriptInterface public String getActiveUser() {
    if (isInactive())
      return null;
    return weakControl.get().getActiveUser();
  }

  /**
   * Get device properties
   *
   * @param propertyId
   * @return
   */
  @android.webkit.JavascriptInterface public String getProperty(String propertyId) {
    if (isInactive())
      return null;
    return weakControl.get().getProperty(propertyId);
  }

  /**
   * Get the base url
   *
   * @return
   */
  @android.webkit.JavascriptInterface public String getBaseUrl() {
    if (isInactive())
      return null;
    return weakControl.get().getBaseUrl();
  }

  /**
   * Store a persistent key-value. This lasts for the duration of this screen and is
   * retained under screen rotations. Useful if browser cookies don't work.
   *
   * @param elementPath
   * @param jsonValue
   */
  @android.webkit.JavascriptInterface public void setSessionVariable(String elementPath,
      String jsonValue) {
    if (isInactive())
      return;
    weakControl.get().setSessionVariable(elementPath, jsonValue);
  }

  /**
   * Retrieve a persistent key-value. This lasts for the duration of this screen and is
   * retained under screen rotations. Useful if browser cookies don't work.
   *
   * @param elementPath
   */
  @android.webkit.JavascriptInterface public String getSessionVariable(String elementPath) {
    if (isInactive())
      return null;
    return weakControl.get().getSessionVariable(elementPath);
  }

  /**
   * This is called within odkCommon.registerListener(fn) to indicate to the Java
   * layer that a listener for doAction responses has been registered. Only after
   * this has been invoked will doAction responses arriving after this cause a
   * callback to be invoked on the Javascript side to trigger that listener.
   *
   * Prior to that, the doAction responses are silently queued in the Java layer
   * awaiting their retrieval. The Javascript layer should call
   * odkCommon.viewFirstQueuedAction() to retrieve any queued actions after having
   * called odkCommon.registerListener(fn) to ensure that all actions are processed.
   */
  @android.webkit.JavascriptInterface
  public void frameworkHasLoaded() {
    if (isInactive())
      return;
    weakControl.get().frameworkHasLoaded();
  }

  /**
   * Execute an action (intent call).
   *
   * @param dispatchStructAsJSONstring Opaque string -- typically identifies prompt and user action
   *                                   If this is null, then the Javascript layer is not notified
   *                                   of the result of this action. It just transparently happens
   *                                   and the webkit might reload as a result of the activity
   *                                   swapping out.
   * @param action         The intent. e.g.,
   *                       org.opendatakit.survey.activities.MediaCaptureImageActivity
   * @param intentMap        JSON.stringify of Map of the following structure:
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
   *</p><p>
   *                  Within the extras, if a value is of the form:
   *                     opendatakit-macro(name)
   *                  then substitute this with the result of getProperty(name)
   *</p><p>
   *                  If the action begins with "org.opendatakit." then we also
   *                  add an "appName" property into the intent extras if it was
   *                  not specified.
   *</p>
   *
   * @return one of:
   * "IGNORE"                -- there is already a pending action
   * "JSONException"         -- problem with intentMap
   * "OK"                    -- request issued
   * "Application not found" -- could not find app to handle intent
   * <p/>
   * If the request has been issued, the javascript will be notified of the availability
   * of a result via the
   */
  @android.webkit.JavascriptInterface public String doAction(String dispatchStructAsJSONstring,
      String action, String intentMap) {
    if (isInactive()) {
      return "IGNORE";
    }
    return weakControl.get().doAction(dispatchStructAsJSONstring, action, intentMap);
  }

  /**
   * @return the oldest queued action outcome.
   * or Url change. Return null if there are none.
   * Leaves the action on the action queue.
   * <p/>
   * The return value is either a JSON serialization of:
   * <p/>
   * {  page: refpage,
   * path: refPath,
   * action: refAction,
   * jsonValue: {
   * status: resultCodeOfAction, // 0 === success
   * result: JSON representation of Extras bundle from result intent
   * }
   * }
   * <p/>
   * or, a JSON serialized string value beginning with #
   * <p/>
   * "#urlhash"   // if the Java code wants the Javascript to take some action without a reload
   */
  @android.webkit.JavascriptInterface public String viewFirstQueuedAction() {
    if (isInactive())
      return null;
    return weakControl.get().viewFirstQueuedAction();
  }

  /**
   * Removes the first queued action, if any.
   */
  @android.webkit.JavascriptInterface public void removeFirstQueuedAction() {
    if (isInactive())
      return;
    weakControl.get().removeFirstQueuedAction();
  }

  /**
   * Closes this window and returns with the given result code and result bundle
   * @param result a string version of integer 0 is RESULT_CANCELED, -1 is RESULT_OK
   * @param resultIntentExtrasAsJSONstring a JSON.stringify() of a key-value map that
   *                will be converted into the Bundle
   *                of extras in the result intent.
   */
 @android.webkit.JavascriptInterface public void closeWindow(String result, String resultIntentExtrasAsJSONstring) {
    if (isInactive())
      return;
    weakControl.get().closeWindow(result, resultIntentExtrasAsJSONstring);;
  }
}
