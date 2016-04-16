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

package org.opendatakit.common.android.views;

import org.json.JSONObject;

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
   * Execute an action (intent call).
   *
   * @param dispatchString Opaque string -- typically identifies prompt and user action
   * @param action         The intent. e.g.,
   *                       org.opendatakit.survey.android.activities.MediaCaptureImageActivity
   * @param jsonMap        JSON.stringify of Map of the following structure:
   *                       {
   *                       "uri" : intent.setData(value)
   *                       "data" : intent.setData(value)  (preferred over "uri")
   *                       "package" : intent.setPackage(value)
   *                       "type" : intent.setType(value)
   *                       "extras" : { key-value map describing extras bundle }
   *                       }
   *                       <p/>
   *                       Within the extras, if a value is of the form:
   *                       opendatakit-macro(name)
   *                       then substitute this with the result of getProperty(name)
   *                       <p/>
   *                       If the action begins with "org.opendatakit." then we also
   *                       add an "appName" property into the intent extras if it was
   *                       not specified.
   * @return one of:
   * "IGNORE"                -- there is already a pending action
   * "JSONException: " + ex.toString()
   * "OK"                    -- request issued
   * "Application not found" -- could not find app to handle intent
   * <p/>
   * If the request has been issued, the javascript will be notified of the availability
   * of a result via the
   */
  @android.webkit.JavascriptInterface public String doAction(String dispatchString, String action,
      String jsonMap) {
    if (isInactive())
      return "IGNORE";
    return weakControl.get().doAction(dispatchString, action, jsonMap);
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

}
