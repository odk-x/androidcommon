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

package org.opendatakit.common.android.utilities;

import java.net.URI;
import java.net.URISyntaxException;

import org.opendatakit.webkitserver.WebkitServerConsts;

import android.content.Context;
import android.net.Uri;

public class UrlUtils {

  private static final String SCHEME_HTTP = "http";

  public static Uri getWebViewContentUri(Context c) {
    return Uri.parse(SCHEME_HTTP + "://" + WebkitServerConsts.HOSTNAME + ":"
        + Integer.toString(WebkitServerConsts.PORT) + "/");
  }
  
  /**
   * Return the file name from a URI segment. The URI segment is expected to
   * be a valid segment as might follow a hostname. This method returns the
   * file name from that segment without hash or query parameters.
   * <p>
   * For example, <code>my/file/path.html#foo=2</code> would return
   * <code>my/file/path.html</code>.
   * <p>
   * Similarly, <code>a/different/file.html?foo=bar</code> would return
   * <code>a/different/file.html</code>.
   * @param segment
   * @return
   */
  public static String getFileNameFromUriSegment(String segment) {
    int parameterIndex = getIndexOfParameters(segment);
    if (parameterIndex == -1) {
      return segment;
    } else {
      return segment.substring(0, parameterIndex);
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
   * Return the parameters from a URL segment. For example,
   * <code>my/file/path.html#foo</code> would return "#foo".
   * Similarly, <code>a/different/file.html?foo=bar</code> would
   * return "?foo=bar".
   * <p>
   * Returns "" if there are no parameters.
   * @param segment
   * @return
   */
  public static String getParametersFromSegment(String segment) {
    int parameterIndex = getIndexOfParameters(segment);
    
    String notPresentFlag = "";
    
    if (parameterIndex == -1) {
      return notPresentFlag;
    } else {
      return segment.substring(parameterIndex);
    }
    
  }

  /**
   * Handles properly encoding a path segment of a URL for use over the wire.
   * Converts arbitrary characters into those appropriate for a segment in a
   * URL path.
   *
   * @param segment
   * @return encodedSegment
   */
  public static String encodeSegment(String segment) {
    // the segment can have URI-inappropriate characters. Encode it first...
    String encodedSegment = Uri.encode(segment, null);
//    try {
//      encodedSegment = UriUtils.encodePathSegment(segment, CharEncoding.US_ASCII);
//    } catch (UnsupportedEncodingException e) {
//      e.printStackTrace();
//      throw new IllegalStateException("Should be able to encode with ASCII");
//    }
    return encodedSegment;
  }

  /**
   * The constructed URI may be invalid if it references a file that is in a
   * legacy directory or an inaccessible directory.
   *
   * Typical usage:
   *
   * File file;
   *
   * getAsWebViewUri(this, appName, ODKFileUtils.asUriFragment(appName, file));
   *
   * @param context
   * @param appName
   * @param uriFragment
   * @return
   */
  public static String getAsWebViewUri(Context context, String appName, String uriFragment) {
    Uri u = UrlUtils.getWebViewContentUri(context);
    // we need to escape the segments.
    u = Uri.withAppendedPath(u, encodeSegment(appName));

    String pathPart;
    String queryPart;
    String hashPart;
    int idxQ = uriFragment.indexOf("?");
    int idxH = uriFragment.indexOf("#");
    if ( idxQ != -1 ) {
      if ( idxH != -1 ) {
        if ( idxH < idxQ ) {
          pathPart = uriFragment.substring(0,idxH);
          queryPart = "";
          hashPart = uriFragment.substring(idxH);
        } else {
          pathPart = uriFragment.substring(0,idxQ);
          queryPart = uriFragment.substring(idxQ, idxH);
          hashPart = uriFragment.substring(idxH);
        }
      } else {
        pathPart = uriFragment.substring(0,idxQ);
        queryPart = uriFragment.substring(idxQ);
        hashPart = "";
      }
    } else if ( idxH != -1 ) {
      pathPart = uriFragment.substring(0,idxH);
      queryPart = "";
      hashPart = uriFragment.substring(idxH);
    } else {
      pathPart = uriFragment;
      queryPart = "";
      hashPart = "";
    }

    String[] segments = pathPart.split("/");
    for (String s : segments) {
      u = Uri.withAppendedPath(u, encodeSegment(s));
    }
    // unclear what escaping is needed on query and hash parts...
    return u.toString() + queryPart + hashPart;
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
