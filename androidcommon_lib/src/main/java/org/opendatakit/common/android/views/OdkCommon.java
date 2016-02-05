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

import android.net.Uri;
import android.os.Build;
import android.os.RemoteException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.opendatakit.common.android.activities.IOdkCommonActivity;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.provider.FormsProviderAPI;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by clarice on 11/3/15.
 */
public class OdkCommon {

  private WeakReference<ODKWebView> mWebView;
  private IOdkCommonActivity mActivity;

  public OdkCommonIf getJavascriptInterfaceWithWeakReference() {
    return new OdkCommonIf(this);
  }

  /**
   * This construct requires an activity rather than a context because we want
   * to be able to launch intents for result rather than merely launch them on
   * their own.
   *
   * @param activity the activity that will be holding the view
   * @throws RemoteException
   */
  public OdkCommon(IOdkCommonActivity activity, ODKWebView webView) {
    this.mActivity = activity;
    this.mWebView = new WeakReference<ODKWebView>(webView);
  }

  public boolean isInactive() {
    return (mWebView.get() == null || mWebView.get().isInactive());
  }

  /**
   * @return
   * @see {@link OdkCommonIf#getPlatformInfo()}
   */
  public String getPlatformInfo() {
    String appName = mActivity.getAppName();
    // This is based on:
    // org.opendatakit.survey.android.views.ODKShimJavascriptCallback
    Map<String, String> platformInfo = new HashMap<String, String>();
    platformInfo.put(PlatformInfoKeys.VERSION, Build.VERSION.RELEASE);
    platformInfo.put(PlatformInfoKeys.CONTAINER, "Android");
    platformInfo.put(PlatformInfoKeys.APP_NAME, appName);
    platformInfo.put(PlatformInfoKeys.BASE_URI, getBaseContentUri());
    platformInfo.put(PlatformInfoKeys.FORMS_URI, FormsProviderAPI.CONTENT_URI.toString());
    platformInfo.put(PlatformInfoKeys.ACTIVE_USER, getActiveUser());
    platformInfo.put(PlatformInfoKeys.LOG_LEVEL, "D");
    JSONObject jsonObject = new JSONObject(platformInfo);
    String result = jsonObject.toString();
    return result;
  }

  /**
   * @param relativePath
   * @return
   * @see {@link OdkCommonIf#getFileAsUrl(String)}
   */
  public String getFileAsUrl(String relativePath) {
    String baseUri = getBaseContentUri();
    String result = baseUri + relativePath;
    return result;
  }

  /**
   * @param tableId
   * @param rowId
   * @param rowPathUri
   * @return
   * @see {@link OdkCommonIf#getRowFileAsUrl(String, String, String)}
   */
  public String getRowFileAsUrl(String tableId, String rowId, String rowPathUri) {
    String appName = mActivity.getAppName();
    String baseUri = getBaseContentUri();
    File rowpathFile = ODKFileUtils.getRowpathFile(appName, tableId, rowId, rowPathUri);
    String uriFragment = ODKFileUtils.asUriFragment(appName, rowpathFile);
    return baseUri + uriFragment;
  }

  /**
   * @return
   * @see {@link OdkCommonIf#log(String, String)}
   */
  public void log(String level, String loggingString) {
    char l = (level == null) ? 'I' : level.charAt(0);
    switch (l) {
    case 'A':
      WebLogger.getLogger(this.mActivity.getAppName()).a("shim", loggingString);
      break;
    case 'D':
      WebLogger.getLogger(this.mActivity.getAppName()).d("shim", loggingString);
      break;
    case 'E':
      WebLogger.getLogger(this.mActivity.getAppName()).e("shim", loggingString);
      break;
    case 'I':
      WebLogger.getLogger(this.mActivity.getAppName()).i("shim", loggingString);
      break;
    case 'S':
      WebLogger.getLogger(this.mActivity.getAppName()).s("shim", loggingString);
      break;
    case 'V':
      WebLogger.getLogger(this.mActivity.getAppName()).v("shim", loggingString);
      break;
    case 'W':
      WebLogger.getLogger(this.mActivity.getAppName()).w("shim", loggingString);
      break;
    default:
      WebLogger.getLogger(this.mActivity.getAppName()).i("shim", loggingString);
      break;
    }
  }

  /**
   * @return
   * @see {@link OdkCommonIf#getActiveUser()}
   */
  public String getActiveUser() {
    log("I", "getActiveUser()");
    String activeUser = mActivity.getActiveUser();
    if (activeUser == null) {
      activeUser = DataTableColumns.DEFAULT_SAVEPOINT_CREATOR;
    }

    return activeUser;
  }

  /**
   * @return
   * @see {@link OdkCommonIf#getProperty(String)}
   */
  public String getProperty(String propertyId) {
    log("I", "getProperty(" + propertyId + ")");

    return mActivity.getProperty(propertyId);
  }

  /**
   * @return
   * @see {@link OdkCommonIf#getBaseUrl()}
   */
  public String getBaseUrl() {
    return ODKFileUtils.getRelativeSystemPath();
  }

  /**
   * Return the base uri for the Tables app name with a trailing separator.
   *
   * @return
   */
  private String getBaseContentUri() {
    Uri contentUri = Uri.parse(mActivity.getWebViewContentUri());
    String appName = mActivity.getAppName();
    contentUri = Uri.withAppendedPath(contentUri, Uri.encode(appName));
    return contentUri.toString() + "/";
  }

  /**
   * Store a persistent key-value. This lasts for the duration of this screen and is
   * retained under screen rotations. Useful if browser cookies don't work.
   *
   * @param elementPath
   * @param jsonValue
   */
  public void setSessionVariable(String elementPath, String jsonValue) {
    mActivity.setSessionVariable(elementPath, jsonValue);
  }

  /**
   * Retrieve a persistent key-value. This lasts for the duration of this screen and is
   * retained under screen rotations. Useful if browser cookies don't work.
   *
   * @param elementPath
   */
  public String getSessionVariable(String elementPath) {
    return mActivity.getSessionVariable(elementPath);
  }

  /**
   * @param dispatchString
   * @param action
   * @param jsonMap
   * @return
   * @see {@link OdkCommonIf#doAction(String, String, String)}
   */
  @android.webkit.JavascriptInterface public String doAction(String dispatchString, String action,
      String jsonMap) {

    JSONObject valueMap = null;
    try {
      if (jsonMap != null && jsonMap.length() != 0) {
        valueMap = (JSONObject) new JSONTokener(jsonMap).nextValue();
      }
    } catch (JSONException e) {
      e.printStackTrace();
      log("E", "doAction(" + dispatchString + ", " + action + ", ...) " + e.toString());
      return "ERROR";
    }
    return mActivity.doAction(dispatchString, action, valueMap);
  }

  /**
   * @return the oldest queued action outcome.
   * or Url change. Return null if there are none.
   */
  public String viewFirstQueuedAction() {
    return mActivity.viewFirstQueuedAction();
  }

  /**
   * Remove the first queued action, if any.
   */
  public void removeFirstQueuedAction() {
    mActivity.removeFirstQueuedAction();
  }

  /**
   * The keys for the platformInfo json object.
   *
   * @author sudar.sam@gmail.com
   */
  private static class PlatformInfoKeys {
    public static final String CONTAINER = "container";
    public static final String VERSION = "version";
    public static final String APP_NAME = "appName";
    public static final String BASE_URI = "baseUri";
    public static final String LOG_LEVEL = "logLevel";
    public static final String FORMS_URI = "formsUri";
    public static final String ACTIVE_USER = "activeUser";

  }
}
