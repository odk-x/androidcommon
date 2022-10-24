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

import android.webkit.ConsoleMessage;
import android.webkit.GeolocationPermissions;
import android.webkit.JsResult;
import android.webkit.PermissionRequest;
import android.webkit.ValueCallback;
import android.webkit.WebChromeClient;
import android.webkit.WebView;

public class ODKWebChromeClient extends WebChromeClient {

  private static final String t = "ODKWebChromeClient";
  private ODKWebView wrappedWebView;

  public ODKWebChromeClient(ODKWebView wrappedWebView) {
    this.wrappedWebView = wrappedWebView;
  }

  @Override
  public void getVisitedHistory(ValueCallback<String[]> callback) {
    callback.onReceiveValue(new String[] {});
  }

  @Override
  public boolean onConsoleMessage(ConsoleMessage consoleMessage) {
    if (consoleMessage.sourceId() == null || consoleMessage.sourceId().length() == 0) {
      wrappedWebView.getLogger().e(t, "onConsoleMessage: Javascript exception: " + consoleMessage.message());
      return true;
    } else {
      if (consoleMessage.messageLevel() == ConsoleMessage.MessageLevel.DEBUG) {
        wrappedWebView.getLogger().d(t, consoleMessage.message());
      } else if (consoleMessage.messageLevel() == ConsoleMessage.MessageLevel.ERROR) {
        wrappedWebView.getLogger().e(t, consoleMessage.message());
      } else if (consoleMessage.messageLevel() == ConsoleMessage.MessageLevel.LOG) {
        wrappedWebView.getLogger().i(t, consoleMessage.message());
      } else if (consoleMessage.messageLevel() == ConsoleMessage.MessageLevel.TIP) {
        wrappedWebView.getLogger().t(t, consoleMessage.message());
      } else if (consoleMessage.messageLevel() == ConsoleMessage.MessageLevel.WARNING) {
        wrappedWebView.getLogger().w(t, consoleMessage.message());
      } else {
        wrappedWebView.getLogger().e(t, consoleMessage.message());
      }
      return true;
    }
  }

  @Override
  public boolean onJsAlert(WebView view, String url, String message, JsResult result) {
    wrappedWebView.getLogger().w(t, url + ": " + message);
    return false;
  }

  @Override
  public void onGeolocationPermissionsShowPrompt(String origin, GeolocationPermissions.Callback
      callback) {
    callback.invoke(origin, true, false);
  }

  @Override
  public void onPermissionRequest(final PermissionRequest request) {
    request.grant(request.getResources());
  }

}
