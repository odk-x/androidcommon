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

import org.opendatakit.common.android.activities.IAppAwareActivity;
import org.opendatakit.common.android.activities.IOdkCommonActivity;
import org.opendatakit.common.android.activities.IOdkDataActivity;
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
public abstract class ODKWebView extends WebView {

  private static final String t = "ODKWebView";
  private static final String BASE_STATE = "BASE_STATE";
  private static final String JAVASCRIPT_REQUESTS_WAITING_FOR_PAGE_LOAD = "JAVASCRIPT_REQUESTS_WAITING_FOR_PAGE_LOAD";

  protected WebLoggerIf log;
  private OdkCommon odkCommon;
  private OdkData odkData;
  private boolean isInactive = false;
  private String loadPageUrl = null;
  private boolean isLoadPageFrameworkFinished = false;
  private boolean isLoadPageFinished = false;
  private boolean isJavascriptFlushActive = false;
  private boolean isFirstPageLoad = true;
  private final LinkedList<String> javascriptRequestsWaitingForPageLoad = new LinkedList<String>();

  /**
   * @return if the webpage has a framework that will call back to notify that it has loaded,
   * then return true. Otherwise, when onPageLoad() completes, we assume the webpage is ready
   * for action.
   */
  public abstract boolean hasPageFramework();

  public abstract void loadPage();

  public abstract void reloadPage();

  /**
   * Return whether or not the webkit is active. It is active
   * if it has a page loaded, or being loaded, into it.
   *
   * @return
   */
  public boolean isInactive() {
    return isInactive;
  }

  /**
   * Stop all handling of calls from the Webkit Javascript
   * to any of the injected Java interfaces.
   */
  public void setInactive() {
    isInactive = true;
  }

  public void serviceChange(boolean ready) {
    if (ready) {
      loadPage();
    } else {
      resetLoadPageStatus(loadPageUrl);
    }
  }

  public String getLoadPageUrl() {
    return loadPageUrl;
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
    // Context is ALWAYS an IOdkDataActivity, IOdkCommonActivity, IAppAwareActivity, IInitResumeActivity...

    String appName = ((IAppAwareActivity) context).getAppName();
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
    odkCommon = new OdkCommon((IOdkCommonActivity) context, this);
    addJavascriptInterface(odkCommon.getJavascriptInterfaceWithWeakReference(), "odkCommonIf");

    odkData = new OdkData((IOdkDataActivity) context, this);
    addJavascriptInterface(odkData.getJavascriptInterfaceWithWeakReference(), "odkDataIf");
  }

  @Override public void destroy() {
    // bare minimum time to mark this as inactive.
    setInactive();
    super.destroy();
  }

  public final WebLoggerIf getLogger() {
    return log;
  }

  /**
   * Signals that a queued action (either the result of 
   * a doAction call or a Java-initiated Url change) is
   * available.
   * <p>The Javascript side should call 
   * odkCommon.getFirstQueuedAction() to retrieve the action.
   * If the returned value is a string, it is a Url change
   * request. if it is a struct, it is a doAction result.
   */
  public void signalQueuedActionAvailable() {
    // NOTE: this is asynchronous
    log.i(t, "signalQueuedActionAvailable()");
    loadJavascriptUrl("javascript:window.odkCommon.signalQueuedActionAvailable()");
  }

  public void signalResponseAvailable() {
    // NOTE: this is asynchronous
    log.i(t, "signalResponseAvailable()");
    loadJavascriptUrl("javascript:odkData.responseAvailable();");
  }

  // called to invoke a javascript method inside the webView
  private synchronized void loadJavascriptUrl(String javascriptUrl) {
    if ( isInactive() ) return; // no-op
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
    ((IOdkCommonActivity) getContext()).queueUrlChange(hash);
    signalQueuedActionAvailable();
  }

  public void pageFinished(String url) {
    if ( !hasPageFramework() ) {
      // if we get an onPageFinished() callback on the WebViewClient that matches our
      // intended load-page URL, then we should consider the page as having been loaded.
      String intendedPageToLoad = getLoadPageUrl();
      if (url != null && intendedPageToLoad != null) {
        int idxSlash = url.indexOf('/');// http:/
        if (idxSlash != -1) {
          idxSlash = url.indexOf('/', idxSlash + 1); // http://
          if (idxSlash != -1) {
            idxSlash = url.indexOf('/', idxSlash + 1); // http://localhost:8365/
            if (idxSlash != -1) {
              idxSlash = url.indexOf('/', idxSlash + 1); // http://localhost:8365/appname/
              String trimmedUrl = url.substring(idxSlash + 1);
              if (trimmedUrl.equals(intendedPageToLoad)) {
                frameworkHasLoaded();
              }
            }
          }
        }
      }
    }
    // otherwise, wait for the framework to tell us it has fully loaded.
  }

  protected boolean hasPageFrameworkFinishedLoading() {
    return isLoadPageFrameworkFinished;
  }

  public synchronized void frameworkHasLoaded() {
    isLoadPageFrameworkFinished = true;
    if (!isLoadPageFinished && !isJavascriptFlushActive) {
      log.i(t, "loadPageFinished: BEGINNING FLUSH");
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
      log.i(t, "loadPageFinished: IGNORING completion event");
    }
  }

  protected synchronized void resetLoadPageStatus(String baseUrl) {
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
