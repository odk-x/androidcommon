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
import android.content.res.Configuration;
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
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.utilities.ODKFileUtils;

/**
 * NOTE: assumes that the Context implements IOdkSurveyActivity.
 * <p>
 * Wrapper for a raw WebView. The enclosing application should only call:
 * initialize(appName) addJavascriptInterface(class,name)
 * loadJavascriptUrl("javascript:...") and any View methods.
 * <p>
 * This class handles ensuring that the framework (index.html) is loaded before
 * executing the javascript URLs.
 *
 * @author mitchellsundt@gmail.com
 */
@SuppressLint("SetJavaScriptEnabled") public abstract class ODKWebView extends WebView
    implements IOdkWebView {

   private static final String t = "ODKWebView";
   private static final String BASE_STATE = "BASE_STATE";

   protected WebLoggerIf log;
   private OdkCommon odkCommon;
   private OdkData odkData;
   private String loadPageUrl = null;
   private String containerFragmentID = null;

   /**
    * isInactive == true -- when this View is being destroyed
    * shouldForceLoadDuringReload == true -- if true, always call loadUrl(loadPageUrl).
    * this should be set to true onCreate() and after onPause()
    * or when the database becomes unavailable so that the page will
    * load when the database becomes available (generally, during
    * the Activity.postResume() phase).
    * isLoadPageFrameworkFinished == false -- this will be set to true once the framework
    * has completely loaded.
    * <p>
    * The general state flow is:
    * on create               -> (false, true, false)
    * on pause                -> (false, true, false)
    * on database unavailable -> (false, true, false)
    * (false, true, false) -> on database available -> (false, false, false) invoke loadUrl(url)
    * (false, false, false) -> on framework loaded  -> (false, false, true)
    * <p>
    * on destroy -> (true, *, *)
    */
   private boolean isInactive = false;
   private boolean shouldForceLoadDuringReload = true;
   private boolean isLoadPageFrameworkFinished = false;
   private boolean shouldReloadAfterLoad = false;

   /**
    * @return if the webpage has a framework that will call back to notify that it has loaded,
    * then return true. Otherwise, when onPageLoad() completes, we assume the webpage is ready
    * for action.
    */
   public abstract boolean hasPageFramework();

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

   public String getLoadPageUrl() {
      return loadPageUrl;
   }

   public String getContainerFragmentID() {
      return containerFragmentID;
   }

   public Context getOdkContext() {
      return super.getContext();
   }

   public void setContainerFragmentID(String containerFragmentID) {
      this.containerFragmentID = containerFragmentID;
   }

   @Override protected Parcelable onSaveInstanceState() {
      log.i(t, "[" + this.hashCode() + "] onSaveInstanceState()");
      Parcelable baseState = super.onSaveInstanceState();
      Bundle savedState = new Bundle();
      if (baseState != null) {
         savedState.putParcelable(BASE_STATE, baseState);
      }
      return savedState;
   }

   @Override protected void onRestoreInstanceState(Parcelable state) {
      log.i(t, "[" + this.hashCode() + "] onRestoreInstanceState()");
      Bundle savedState = (Bundle) state;

      if (savedState.containsKey(BASE_STATE)) {
         Parcelable baseState = savedState.getParcelable(BASE_STATE);
         super.onRestoreInstanceState(baseState);
      }
   }

   @Override public void onResume() {
      super.onResume();
   }

   @Override public void onPause() {
      super.onPause();

      shouldForceLoadDuringReload = true;
      isLoadPageFrameworkFinished = false;
      containerFragmentID = getContainerFragmentID();
   }

   @Override public void destroy() {
      // bare minimum time to mark this as inactive.
      isInactive = true;
      if (odkData != null) {
         odkData.shutdownContext();
      }

      super.destroy();
   }

   @SuppressLint("NewApi") private void perhapsEnableDebugging() {
      if (Build.VERSION.SDK_INT >= 19) {
         WebView.setWebContentsDebuggingEnabled(true);
      }
   }

   @SuppressWarnings("deprecation") private void setGeoLocationCache(String appName,
       WebSettings ws) {
      if (Build.VERSION.SDK_INT < 24) {
         ws.setGeolocationDatabasePath(ODKFileUtils.getGeoCacheFolder(appName));
      }
   }

   public ODKWebView(Context context, AttributeSet attrs) {
      super(context, attrs);

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
      int fontSize = CommonToolProperties
          .getQuestionFontsize(context.getApplicationContext(), appName);
      ws.setDefaultFixedFontSize(fontSize);
      ws.setDefaultFontSize(fontSize);
      ws.setDomStorageEnabled(true);
      setGeoLocationCache(appName, ws);
      ws.setGeolocationEnabled(true);
      ws.setJavaScriptCanOpenWindowsAutomatically(true);
      ws.setJavaScriptEnabled(true);

      // disable to try to solve touch/mouse/swipe issues
      ws.setSupportZoom(true);
      ws.setUseWideViewPort(false);
      ws.setMediaPlaybackRequiresUserGesture(false);

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
    * If the page has not yet loaded, we suppress this
    * notification.
    */
   public void signalQueuedActionAvailable() {
      // NOTE: this is asynchronous
      log.i(t, "[" + this.hashCode() + "] signalQueuedActionAvailable()");
      loadJavascriptUrl("javascript:window.odkCommon.signalQueuedActionAvailable()", true);
      shouldReloadAfterLoad = true;
   }

   /**
    * Signals that a databse response is available.
    * This should be processed before the framework is fully loaded
    * since part of loading the framework will be fetching data from
    * the database.
    */
   public void signalResponseAvailable() {
      // NOTE: this is asynchronous
      log.i(t, "[" + this.hashCode() + "] signalResponseAvailable()");
      loadJavascriptUrl("javascript:odkData.responseAvailable();", false);
   }

   // called to invoke a javascript method inside the webView
   private synchronized void loadJavascriptUrl(final String javascriptUrl,
       boolean suppressIfFrameworkIsNotLoaded) {
      if (isInactive())
         return; // no-op
      if (isLoadPageFrameworkFinished || !suppressIfFrameworkIsNotLoaded) {
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
      }
   }

   public void pageFinished(String url) {
      if (!hasPageFramework()) {
         // if we get an onPageFinished() callback on the WebViewClient that matches our
         // intended load-page URL, then we should consider the page as having been loaded.
         String intendedPageToLoad = getLoadPageUrl();
         if (url != null && intendedPageToLoad != null) {
            // Various versions of the browser append ? or # or ?# to a naked URL
            // Strip # of everything and ? off the query string is empty.
            //
            int idxHash = intendedPageToLoad.indexOf('#');
            if (idxHash != -1) {
               intendedPageToLoad = intendedPageToLoad.substring(0, idxHash);
            }
            idxHash = url.indexOf('#');
            if (idxHash != -1) {
               url = url.substring(0, idxHash);
            }
            int idxQuestion = intendedPageToLoad.indexOf('?');
            if (idxQuestion == intendedPageToLoad.length() - 1) {
               intendedPageToLoad = intendedPageToLoad.substring(0, idxQuestion);
            }
            idxQuestion = url.indexOf('?');
            if (idxQuestion == url.length() - 1) {
               url = url.substring(0, idxQuestion);
            }
            if (url.charAt(url.length() - 1) == '/') {
               url = url.substring(0, url.length() - 1);
            }
            if (intendedPageToLoad.charAt(intendedPageToLoad.length() - 1) == '/') {
               intendedPageToLoad = intendedPageToLoad
                   .substring(0, intendedPageToLoad.length() - 1);
            }

            // finally, test the two URLs
            if (url.equals(intendedPageToLoad)) {
               frameworkHasLoaded();
            }
         }
      }
      // otherwise, wait for the framework to tell us it has fully loaded.
   }

   public synchronized void setForceLoadDuringReload() {
      shouldForceLoadDuringReload = true;
   }

   public synchronized void frameworkHasLoaded() {
      isLoadPageFrameworkFinished = true;
   }


   protected synchronized void loadPageOnUiThread(final String url,
       final String containerFragmentID) {
      if (isInactive()) {
         log.w(t, "LoadPageonUIThread: ignored -- webkit is inactive!");
         return;
      }

      if (url != null) {
         boolean isSameUrl = url.equals(getLoadPageUrl());

         if (shouldForceLoadDuringReload || !isSameUrl || shouldReloadAfterLoad) {

            // NOTE:
            // there is a potential race condition if there
            // is a page loading that hasn't yet had its framework
            // load and we are *forcing* a reload or loading a different
            // url. No easy way to guard against that since preventing a
            // reload if the prior load did not complete would prevent a reset
            // of the UI. And allowing the reload can cause a premature
            // transition into the framework-has-loaded status. In general,
            // we expect webkits to load a single URL and spawn new webkits
            // when launching different URLs, so this race condition is
            // largely prevented via our usage model: 1-url <=> 1-webkit
            // and only reload or load that URL once.

            // reset to a clean need-to-reload state
            isLoadPageFrameworkFinished = false;
            loadPageUrl = url;
            this.containerFragmentID = containerFragmentID;

            if (shouldForceLoadDuringReload || !isSameUrl) {
               log.i(t, "loadURL: " + url);
               // Ensure that this is run on the UI thread
               if (Thread.currentThread() != Looper.getMainLooper().getThread()) {
                  post(new Runnable() {
                     @Override public void run() {
                        loadUrl(url);
                     }
                  });
               } else {
                  loadUrl(url);
               }
            }

            if (shouldForceLoadDuringReload || shouldReloadAfterLoad) {
               log.i(t, "initatiate reload");
               // Ensure that this is run on the UI thread
               if (Thread.currentThread() != Looper.getMainLooper().getThread()) {
                  post(new Runnable() {
                     @Override public void run() {
                        reload();
                     }
                  });
               } else {
                  reload();
               }
            }

            // and signal that a load is commencing for url
            // if a subsequent load for url is issued, it will
            // be ignored until this one has completed or until
            // a load is forced by the caller (reload == false).
            shouldForceLoadDuringReload = false;
            shouldReloadAfterLoad = false;

         } else {
            log.w(t, "framework in process of loading -- ignoring request!");
         }
      } else {
         log.w(t, "cannot load anything -- url is null!");
      }
   }
}
