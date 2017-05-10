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
package org.opendatakit.views;

import android.annotation.SuppressLint;
import android.content.Context;
import android.os.Build;
import android.os.Bundle;
import android.os.Looper;
import android.os.Parcelable;
import android.util.AttributeSet;
import android.view.View;
import android.webkit.WebSettings;
import android.webkit.WebView;

import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.activities.IOdkCommonActivity;
import org.opendatakit.activities.IOdkDataActivity;
import org.opendatakit.application.CommonApplication;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;

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
  private String containerFragmentID = null;
  private boolean isLoadPageFrameworkFinished = false;
  private boolean isLoadPageFinished = false;
  private boolean isJavascriptFlushActive = false;
  private boolean isFirstPageLoad = true;
  private boolean shouldForceLoadDuringReload = false;
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
      resetLoadPageStatus(loadPageUrl, containerFragmentID);
    }
  }

  public String getLoadPageUrl() {
    return loadPageUrl;
  }

  public String getContainerFragmentID() {
    return containerFragmentID;
  }

  public void setContainerFragmentID(String containerFragmentID) {
    this.containerFragmentID = containerFragmentID;
  }

  @Override
  protected Parcelable onSaveInstanceState () {
    log.i(t, "[" + this.hashCode() + "] onSaveInstanceState()");
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
    log.i(t, "[" + this.hashCode() + "] onRestoreInstanceState()");
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

  @SuppressWarnings("deprecation")
  private void setGeoLocationCache(String appName, WebSettings ws) {
    if ( Build.VERSION.SDK_INT < 24 ) {
          ws.setGeolocationDatabasePath(ODKFileUtils.getGeoCacheFolder(appName));
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
    log.i(t, "[" + this.hashCode() + "] ODKWebView()");

    perhapsEnableDebugging();

    // for development -- always draw from source...
    WebSettings ws = getSettings();
    ws.setAllowFileAccess(true);
    ws.setAppCacheEnabled(true);
    ws.setAppCachePath(ODKFileUtils.getAppCacheFolder(appName));
    ws.setCacheMode(WebSettings.LOAD_DEFAULT);
    ws.setDatabaseEnabled(false);
    int fontSize = CommonToolProperties.getQuestionFontsize(context.getApplicationContext(), appName);
    ws.setDefaultFixedFontSize(fontSize);
    ws.setDefaultFontSize(fontSize);
    ws.setDomStorageEnabled(true);
    setGeoLocationCache(appName, ws);
    ws.setGeolocationEnabled(true);
    ws.setJavaScriptCanOpenWindowsAutomatically(true);
    ws.setJavaScriptEnabled(true);

    // disable to try to solve touch/mouse/swipe issues
    ws.setBuiltInZoomControls(true);
    ws.setSupportZoom(true);
    ws.setUseWideViewPort(false);

    setFocusable(true);
    setFocusableInTouchMode(true);

    // questionable value...
    setScrollBarStyle(View.SCROLLBARS_INSIDE_OVERLAY);
    setSaveEnabled(true);

    // set up the client...
    setWebChromeClient(new ODKWebChromeClient(this));
    setWebViewClient(new ODKWebViewClient(this));

    // set up the odkCommonIf
    odkCommon = new OdkCommon((IOdkCommonActivity) context, this);
    addJavascriptInterface(odkCommon.getJavascriptInterfaceWithWeakReference(),
        Constants.JavaScriptHandles.COMMON);

    odkData = new OdkData((IOdkDataActivity) context, this);
    addJavascriptInterface(odkData.getJavascriptInterfaceWithWeakReference(),
        Constants.JavaScriptHandles.DATA);
  }

  @Override public void destroy() {
    // bare minimum time to mark this as inactive.
    setInactive();
    if (odkData != null) {
      odkData.shutdownContext();
    }

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
    log.i(t, "[" + this.hashCode() + "] signalQueuedActionAvailable()");
    loadJavascriptUrl("javascript:window.odkCommon.signalQueuedActionAvailable()");
  }

  public void signalResponseAvailable() {
    // NOTE: this is asynchronous
    log.i(t, "[" + this.hashCode() + "] signalResponseAvailable()");
    loadJavascriptUrl("javascript:odkData.responseAvailable();");
  }

  // called to invoke a javascript method inside the webView
  private synchronized void loadJavascriptUrl(final String javascriptUrl) {
    if ( isInactive() ) return; // no-op
    if (isLoadPageFinished || isJavascriptFlushActive) {
      log.i(t, "[" + this.hashCode() + "] loadJavascriptUrl: IMMEDIATE: " + javascriptUrl);

      // Ensure that this is run on the UI thread
      if (Thread.currentThread() != Looper.getMainLooper().getThread()) {
        post(new Runnable() {
          public void run() {
            loadUrl(javascriptUrl);
          }
        });
      } else {
        loadUrl(javascriptUrl);
      }

    } else {
      log.i(t, "[" + this.hashCode() + "] loadJavascriptUrl: QUEUING: " + javascriptUrl);
      javascriptRequestsWaitingForPageLoad.add(javascriptUrl);
    }
  }

  public void gotoUrlHash(String hash) {
    log.i(t, "[" + this.hashCode() + "] gotoUrlHash: " + hash);
    ((IOdkCommonActivity) getContext()).queueUrlChange(hash);
    signalQueuedActionAvailable();
  }

  public void pageFinished(String url) {
    if ( !hasPageFramework() ) {
      // if we get an onPageFinished() callback on the WebViewClient that matches our
      // intended load-page URL, then we should consider the page as having been loaded.
      String intendedPageToLoad = getLoadPageUrl();
      if (url != null && intendedPageToLoad != null) {
        // Various versions of the browser append ? or # or ?# to a naked URL
        // Strip # of everything and ? off the query string is empty.
        //
        int idxHash = intendedPageToLoad.indexOf('#');
        if ( idxHash != -1 ) {
          intendedPageToLoad = intendedPageToLoad.substring(0, idxHash);
        }
        idxHash = url.indexOf('#');
        if ( idxHash != -1 ) {
          url = url.substring(0, idxHash);
        }
        int idxQuestion = intendedPageToLoad.indexOf('?');
        if ( idxQuestion == intendedPageToLoad.length()-1 ) {
          intendedPageToLoad = intendedPageToLoad.substring(0, idxQuestion);
        }
        idxQuestion = url.indexOf('?');
        if ( idxQuestion == url.length()-1 ) {
          url = url.substring(0, idxQuestion);
        }

        // finally, test the two URLs
        if ( url.equals(intendedPageToLoad) ) {
          frameworkHasLoaded();
        }
      }
    }
    // otherwise, wait for the framework to tell us it has fully loaded.
  }

  protected boolean hasPageFrameworkFinishedLoading() {
    return isLoadPageFrameworkFinished;
  }

  public void setForceLoadDuringReload() {
    shouldForceLoadDuringReload = true;
  }

  protected boolean shouldForceLoadDuringReload() {
    return shouldForceLoadDuringReload;
  }

  public synchronized void frameworkHasLoaded() {
    isLoadPageFrameworkFinished = true;
    if (!isLoadPageFinished && !isJavascriptFlushActive) {
      log.i(t, "[" + this.hashCode() + "] loadPageFinished: BEGINNING FLUSH");
      isJavascriptFlushActive = true;
      while (isJavascriptFlushActive && !javascriptRequestsWaitingForPageLoad.isEmpty()) {
        String s = javascriptRequestsWaitingForPageLoad.removeFirst();
        log.i(t, "[" + this.hashCode() + "] loadPageFinished: DISPATCHING javascriptUrl: " + s);
        loadJavascriptUrl(s);
      }
      isLoadPageFinished = true;
      isJavascriptFlushActive = false;
      isFirstPageLoad = false;
    } else {
      log.i(t, "[" + this.hashCode() + "] loadPageFinished: IGNORING completion event");
    }
  }

  protected synchronized void resetLoadPageStatus(String baseUrl, String containerFragmentID) {
    isLoadPageFrameworkFinished = false;
    isLoadPageFinished = false;
    loadPageUrl = baseUrl;
    this.containerFragmentID = containerFragmentID;
    isJavascriptFlushActive = false;
    shouldForceLoadDuringReload = false;

    // do not purge the list of actions if this is the first page load.
    // keep them queued until they can be issued.
    if ( !isFirstPageLoad ) {
      while (!javascriptRequestsWaitingForPageLoad.isEmpty()) {
        String s = javascriptRequestsWaitingForPageLoad.removeFirst();
        log.i(t, "resetLoadPageStatus: DISCARDING javascriptUrl: " + s);
      }
    }
  }

  protected synchronized void loadPageOnUiThread(final String url, final String containerFragmentID,
                                                 boolean reload) {
     String typeOfLoad = reload ? "reloadPage" : "loadPage";

     if (url != null) {

        if (!reload || (shouldForceLoadDuringReload() || hasPageFrameworkFinishedLoading() ||
            !url.equals(getLoadPageUrl())))
           {
              resetLoadPageStatus(url, containerFragmentID);

              log.i(t, typeOfLoad + ": load: " + url);

              // Ensure that this is run on the UI thread
              if (Thread.currentThread() != Looper.getMainLooper().getThread()) {
                 post(new Runnable() {
                    public void run() {
                       loadUrl(url);
                    }
                 });
              } else {
                 loadUrl(url);
              }
           } else {
              log.w(t, typeOfLoad + ": framework in process of loading -- ignoring request!");
           }

     } else {
        log.w(t, typeOfLoad + ": cannot load anything url is null!");
     }

  }

}
