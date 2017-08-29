/*
 * Copyright (C) 2009-2013 University of Washington
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

package org.opendatakit.webkitserver.utilities;

import java.net.URI;
import java.net.URISyntaxException;

import org.opendatakit.consts.WebkitServerConsts;

import android.net.Uri;

/**
 * Utilities class for urls
 */
public class UrlUtils {

  /**
   * Do not instantiate this
   */
  private UrlUtils() {
  }

  /**
   * Get the most basic web view path. The appName should be applied to this
   * in order to reference content under ODKFileUtils.getAppFolder(appName)
   *
   * @return
   */
  public static Uri getWebViewContentUri() {
    return Uri.parse(WebkitServerConsts.SCHEME + "://" + WebkitServerConsts.HOSTNAME +
                    ":" + Integer.toString(WebkitServerConsts.PORT) + "/");
  }

  /**
   * Return the uri path portion of a uriFragment that might include query or hash
   * parts. The uriFragment is expected to be a valid string as might follow a hostname.
   * This method returns the path portion without hash or query parameters.
   * <p>
   * For example, <code>my/file/path.html#foo=2</code> would return
   * <code>my/file/path.html</code>.
   * <p>
   * Similarly, <code>a/different/file.html?foo=bar</code> would return
   * <code>a/different/file.html</code>.
   * @param uriFragment
   * @return
   */
  public static String getPathFromUriFragment(String uriFragment) {
    int parameterIndex = getIndexOfParameters(uriFragment);
    if (parameterIndex == -1) {
      return uriFragment;
    } else {
      return uriFragment.substring(0, parameterIndex);
    }
  }
  
  /**
   * Get the index into segment where the query parameters start in the
   * segment. For example, <code>my/file/path.html#foo</code> would return the
   * index of '#'. Similarly, <code>a/different/file.html?foo=bar</code> would
   * return the index of '?'. If both '#' and '?' are present, it will return
   * the index of the first.
   * <p>
   * Returns -1 if neither is present.
   * @param segment
   * @return
   */
  static int getIndexOfParameters(String segment) {
    int hashIndex = segment.indexOf('#');
    int qIndex = segment.indexOf('?');
    
    int notPresentFlag = -1;
    
    if (hashIndex == -1 && qIndex == -1) {
      // no hash or query param
      return notPresentFlag;
    } else if (hashIndex == -1 && qIndex != -1) {
      // only a query parameter
      return qIndex;
    } else if (hashIndex != -1 && qIndex == -1) {
      // only a hash
      return hashIndex;
    } else {
      // both hash and query param
      int firstSpecialIndex = Math.min(hashIndex, qIndex);
      return firstSpecialIndex;
    }
  }
  
  /**
   * Return the parameters from a uriFragment. For example,
   * <code>my/file/path.html#foo</code> would return "#foo".
   * Similarly, <code>a/different/file.html?foo=bar</code> would
   * return "?foo=bar".
   * <p>
   * Returns "" if there are no parameters.
   * @param uriFragment
   * @return
   */
  public static String getParametersFromUriFragment(String uriFragment) {
    int parameterIndex = getIndexOfParameters(uriFragment);
    
    String notPresentFlag = "";
    
    if (parameterIndex == -1) {
      return notPresentFlag;
    } else {
      return uriFragment.substring(parameterIndex);
    }
    
  }

  /**
   * The constructed URI may be invalid if it references a file that is in a
   * legacy directory or an inaccessible directory. Assumes the uriFragment is
   * a valid uri fragment that might include query or hash parameters.
   *
   * Strips off the query and/or hash parameter, constructs the full uri for this appName
   * and residual path portion, then appends the supplied query and/or hash parameter.
   *
   * Typical usage:
   *
   * File file;
   *
   * getAsWebViewUri(appName, ODKFileUtils.asUriFragment(appName, file));
   *
   * @param appName
   * @param uriFragment
   * @return
   */
  public static String getAsWebViewUri(String appName, String uriFragment) {
    int parameterIndex = getIndexOfParameters(uriFragment);

    String pathPart = uriFragment;
    String parameters = "";

    if (parameterIndex != -1) {
      pathPart = uriFragment.substring(0, parameterIndex);
      parameters = uriFragment.substring(parameterIndex);
    }

    Uri u = getWebViewContentUri();
    String fullPath = u.buildUpon().appendPath(appName).appendEncodedPath(pathPart).toString();
    // assume query and hash parts are properly escaped...
    return fullPath + parameters;
  }

  public static boolean isValidUrl(String url) {

    try {
      @SuppressWarnings("unused")
      URI uri = new URI(url);
      return true;
    } catch (URISyntaxException e) {
      return false;
    }

  }

}
