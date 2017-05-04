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

package org.opendatakit.views;

import android.annotation.TargetApi;
import android.os.Build;
import android.os.Message;
import android.view.KeyEvent;
import android.webkit.WebResourceError;
import android.webkit.WebResourceRequest;
import android.webkit.WebView;
import android.webkit.WebViewClient;

public class ODKWebViewClient extends WebViewClient {
  private static final String t = "ODKWebViewClient";
  private final ODKWebView wrappedView;

  ODKWebViewClient(ODKWebView wrappedView) {
	this.wrappedView = wrappedView;
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean shouldOverrideUrlLoading(WebView view, String url) {
    wrappedView.getLogger().i(t,
        "shouldOverrideUrlLoading: " + url + " ms: " + Long.toString(System.currentTimeMillis()));
    return super.shouldOverrideUrlLoading(view, url);
  }

  @TargetApi(24)
  @Override
  public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
    wrappedView.getLogger().i(t,
        "shouldOverrideUrlLoading: " + request.getUrl().toString() + " ms: " +
            Long.toString(System.currentTimeMillis()));
    return super.shouldOverrideUrlLoading(view, request);
  }

  @Override
  public void doUpdateVisitedHistory(WebView view, String url, boolean isReload) {
    wrappedView.getLogger().i(t, "doUpdateVisitedHistory: " + url + " ms: " + Long.toString(System.currentTimeMillis()));
  }

  @Override
  public void onLoadResource(WebView view, String url) {
    wrappedView.getLogger().i(t, "onLoadResource: " + url + " ms: " + Long.toString(System.currentTimeMillis()));
    super.onLoadResource(view, url);
  }

  @Override
  public void onPageFinished(WebView view, String url) {
    wrappedView.getLogger().i(t, "onPageFinished: " + url + " ms: " + Long.toString(System.currentTimeMillis()));
    wrappedView.pageFinished(url);
    super.onPageFinished(view, url);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void onReceivedError(WebView view, int errorCode, String description, String failingUrl) {
    wrappedView.getLogger().i(t, "onReceivedError: " + failingUrl + " ms: " + Long.toString(System.currentTimeMillis()));
    super.onReceivedError(view, errorCode, description, failingUrl);
  }


  @TargetApi(23)
  @Override
  public void onReceivedError(WebView view, WebResourceRequest request,
      WebResourceError error) {
    wrappedView.getLogger().i(t, "onReceivedError: " + request.getUrl().toString() + " ms: " + Long
        .toString(System
        .currentTimeMillis()));
    super.onReceivedError(view, request, error);
  }

  @Override
  public void onScaleChanged(WebView view, float oldScale, float newScale) {
    wrappedView.getLogger().i(t, "onScaleChanged: " + newScale);
    super.onScaleChanged(view, oldScale, newScale);
  }

  @Override
  public void onUnhandledKeyEvent(WebView view, KeyEvent event) {
    wrappedView.getLogger().i(t, "onUnhandledKeyEvent: " + event.toString());
    super.onUnhandledKeyEvent(view, event);
  }
}
