/*
 * Copyright (C) 2014 University of Washington
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

import org.opendatakit.common.android.activities.IOdkSurveyActivity;
import android.annotation.SuppressLint;
import android.content.Context;
import android.os.Build;
import android.os.Bundle;
import android.os.Parcelable;
import android.util.AttributeSet;
import android.view.View;
import android.webkit.WebSettings;
import android.webkit.WebView;
import org.opendatakit.common.android.application.CommonApplication;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.android.utilities.WebLoggerIf;

import java.util.LinkedList;

/**
 * NOTE: assumes that the Context implements IOdkSurveyActivity.
 *
 * Wrapper for a raw WebView. The enclosing application should only call:
 * initialize(appName) addJavascriptInterface(class,name)
 * loadJavascriptUrl("javascript:...") and any View methods.
 *
 * This class handles ensuring that the framework (index.html) is loaded before
 * executing the javascript URLs.
 *
 * @author mitchellsundt@gmail.com
 *
 */
@SuppressLint("SetJavaScriptEnabled")
public class ODKWebView extends WebView {

  private static final String t = "ODKWebView";
  private static final String BASE_STATE = "BASE_STATE";
  private static final String JAVASCRIPT_REQUESTS_WAITING_FOR_PAGE_LOAD = "JAVASCRIPT_REQUESTS_WAITING_FOR_PAGE_LOAD";

  private final IOdkSurveyActivity activity;
  private WebLoggerIf log;
  private ODKShimJavascriptCallback shim;
  private OdkCommon odkCommon;
  private OdkData odkData;
  private String loadPageUrl = null;
  private boolean isLoadPageFrameworkFinished = false;
  private boolean isLoadPageFinished = false;
  private boolean isJavascriptFlushActive = false;
  private boolean isFirstPageLoad = true;
  private final LinkedList<String> javascriptRequestsWaitingForPageLoad = new LinkedList<String>();

  public void serviceChange( boolean ready) {
    if ( ready ) {
      loadPage();
    } else {
      resetLoadPageStatus(loadPageUrl);
    }
  }

  @Override
  protected Parcelable onSaveInstanceState () {
    log.i(t, "onSaveInstanceState()");
    Parcelable baseState = super.onSaveInstanceState();
    Bundle savedState = new Bundle();
    if ( baseState != null ) {
      savedState.putParcelable(BASE_STATE, baseState);
    }
    if ( javascriptRequestsWaitingForPageLoad.size() == 0 ) {
      return savedState;
    }
    String[] waitQueue = new String[javascriptRequestsWaitingForPageLoad.size()];
    int i = 0;
    for ( String s : javascriptRequestsWaitingForPageLoad ) {
      waitQueue[i++] = s;
    }
    savedState.putStringArray(JAVASCRIPT_REQUESTS_WAITING_FOR_PAGE_LOAD, waitQueue);
    return savedState;
  }

  @Override
  protected void onRestoreInstanceState (Parcelable state) {
    log.i(t, "onRestoreInstanceState()");
    Bundle savedState = (Bundle) state;
    if ( savedState.containsKey(JAVASCRIPT_REQUESTS_WAITING_FOR_PAGE_LOAD)) {
      String[] waitQueue = savedState.getStringArray(JAVASCRIPT_REQUESTS_WAITING_FOR_PAGE_LOAD);
      for ( String s : waitQueue ) {
        javascriptRequestsWaitingForPageLoad.add(s);
      }
    }
    isFirstPageLoad = true;

    if ( savedState.containsKey(BASE_STATE) ) {
      Parcelable baseState = savedState.getParcelable(BASE_STATE);
      super.onRestoreInstanceState(baseState);
    }
    loadPage();
  }

  @Override
  @SuppressLint("NewApi")
  public void onPause() {
    super.onPause();
  }

  @SuppressLint("NewApi")
  private void perhapsEnableDebugging() {
    if (Build.VERSION.SDK_INT >= 19) {
      WebView.setWebContentsDebuggingEnabled(true);
    }
  }

  public ODKWebView(Context context, AttributeSet attrs) {
    super(context, attrs);
    
    if ( Build.VERSION.SDK_INT < 11 ) {
      throw new IllegalStateException("pre-3.0 not supported!");
    }
    // Context is ALWAYS an IOdkSurveyActivity...

    activity = (IOdkSurveyActivity) context;
    String appName = activity.getAppName();
    log = WebLogger.getLogger(appName);
    log.i(t, "ODKWebView()");

    perhapsEnableDebugging();

    // for development -- always draw from source...
    WebSettings ws = getSettings();
    ws.setAllowFileAccess(true);
    ws.setAppCacheEnabled(true);
    ws.setAppCachePath(ODKFileUtils.getAppCacheFolder(appName));
    ws.setCacheMode(WebSettings.LOAD_DEFAULT);
    ws.setDatabaseEnabled(false);
    ws.setDefaultFixedFontSize(((CommonApplication) context.getApplicationContext()).getQuestionFontsize(appName));
    ws.setDefaultFontSize(((CommonApplication) context.getApplicationContext()).getQuestionFontsize(appName));
    ws.setDomStorageEnabled(true);
    ws.setGeolocationDatabasePath(ODKFileUtils.getGeoCacheFolder(appName));
    ws.setGeolocationEnabled(true);
    ws.setJavaScriptCanOpenWindowsAutomatically(true);
    ws.setJavaScriptEnabled(true);

    // disable to try to solve touch/mouse/swipe issues
    ws.setBuiltInZoomControls(true);
    ws.setSupportZoom(true);

    setFocusable(true);
    setFocusableInTouchMode(true);
    setInitialScale(100);

    // questionable value...
    setScrollBarStyle(View.SCROLLBARS_INSIDE_OVERLAY);
    setSaveEnabled(true);

    // set up the client...
    setWebChromeClient(new ODKWebChromeClient(this));
    setWebViewClient(new ODKWebViewClient(this));

    // set up the odkCommonIf
    odkCommon = new OdkCommon(activity);
    addJavascriptInterface(odkCommon.getJavascriptInterfaceWithWeakReference(), "odkCommonIf");

    odkData = new OdkData(activity);
    addJavascriptInterface(odkData.getJavascriptInterfaceWithWeakReference(), "odkDataIf");

    // stomp on the shim object...
    shim = new ODKShimJavascriptCallback(this,activity);
    addJavascriptInterface(shim, "shim");
  }

  public final WebLoggerIf getLogger() {
    return log;
  }

  /**
   * Signals that a queued action (either the result of 
   * a doAction call or a Java-initiated Url change) is
   * available.
   * <p>The Javascript side should call 
   * shim.getFirstQueuedAction() to retrieve the action.
   * If the returned value is a string, it is a Url change
   * request. if it is a struct, it is a doAction result.
   */
  public void signalQueuedActionAvailable() {
    // NOTE: this is asynchronous
    log.i(t, "signalQueuedActionAvailable()");
    loadJavascriptUrl("javascript:window.odkCommon.signalQueuedActionAvailable()");
  }

  // called to invoke a javascript method inside the webView
  private synchronized void loadJavascriptUrl(String javascriptUrl) {
    if (isLoadPageFinished || isJavascriptFlushActive) {
      log.i(t, "loadJavascriptUrl: IMMEDIATE: " + javascriptUrl);
      loadUrl(javascriptUrl);
    } else {
      log.i(t, "loadJavascriptUrl: QUEUING: " + javascriptUrl);
      javascriptRequestsWaitingForPageLoad.add(javascriptUrl);
    }
  }

  public void gotoUrlHash(String hash) {
    log.i(t, "gotoUrlHash: " + hash);
    activity.queueUrlChange(hash);
    signalQueuedActionAvailable();
  }

  public synchronized void loadPage() {
    /**
     * NOTE: Reload the web framework only if it has changed.
     */

    if ( activity.getDatabase() == null ) {
      // do not initiate reload until we have the database set up...
      return;
    }

    log.i(t, "loadPage: current loadPageUrl: " + loadPageUrl);
    String baseUrl = activity.getUrlBaseLocation(isLoadPageFrameworkFinished && loadPageUrl != null);
    String hash = activity.getUrlLocationHash();

    if ( baseUrl != null ) {
      resetLoadPageStatus(baseUrl);

      log.i(t, "loadPage: full reload: " + baseUrl + hash);

      loadUrl(baseUrl + hash);
    } else if ( isLoadPageFrameworkFinished ) {
      log.i(t,  "loadPage: delegate to gotoUrlHash: " + hash);
      gotoUrlHash(hash);
    } else {
      log.w(t, "loadPage: framework did not load -- cannot load anything!");
    }
  }

  public synchronized void clearPage() {
    log.i(t, "clearPage: current loadPageUrl: " + loadPageUrl);
    String baseUrl = activity.getUrlBaseLocation(false);

    if ( baseUrl != null ) {
      resetLoadPageStatus(baseUrl);
      log.i(t, "clearPage: full reload: " + baseUrl);
      loadUrl(baseUrl);
    } else {
      log.w(t, "clearPage: framework did not load -- cannot load anything!");
    }
  }

  synchronized void frameworkHasLoaded() {
    isLoadPageFrameworkFinished = true;
    if (!isLoadPageFinished && !isJavascriptFlushActive) {
      log.i(t, "loadPageFinished: BEGINNING FLUSH refId: " + activity.getRefId());
      isJavascriptFlushActive = true;
      while (isJavascriptFlushActive && !javascriptRequestsWaitingForPageLoad.isEmpty()) {
        String s = javascriptRequestsWaitingForPageLoad.removeFirst();
        log.i(t, "loadPageFinished: DISPATCHING javascriptUrl: " + s);
        loadJavascriptUrl(s);
      }
      isLoadPageFinished = true;
      isJavascriptFlushActive = false;
      isFirstPageLoad = false;
    } else {
      log.i(t, "loadPageFinished: IGNORING completion event refId: " + activity.getRefId());
    }
  }

  private synchronized void resetLoadPageStatus(String baseUrl) {
    isLoadPageFrameworkFinished = false;
    isLoadPageFinished = false;
    loadPageUrl = baseUrl;
    isJavascriptFlushActive = false;
    // do not purge the list of actions if this is the first page load.
    // keep them queued until they can be issued.
    if ( !isFirstPageLoad ) {
      while (!javascriptRequestsWaitingForPageLoad.isEmpty()) {
        String s = javascriptRequestsWaitingForPageLoad.removeFirst();
        log.i(t, "resetLoadPageStatus: DISCARDING javascriptUrl: " + s);
      }
    }
  }
}
