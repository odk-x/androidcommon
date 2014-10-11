/*
 * Copyright (C) 2011-2013 University of Washington
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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.CharEncoding;
import org.kxml2.io.KXmlParser;
import org.kxml2.kdom.Document;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;
import org.opendatakit.httpclientandroidlib.Header;
import org.opendatakit.httpclientandroidlib.HttpEntity;
import org.opendatakit.httpclientandroidlib.HttpRequest;
import org.opendatakit.httpclientandroidlib.HttpResponse;
import org.opendatakit.httpclientandroidlib.auth.AuthScope;
import org.opendatakit.httpclientandroidlib.auth.Credentials;
import org.opendatakit.httpclientandroidlib.auth.UsernamePasswordCredentials;
import org.opendatakit.httpclientandroidlib.client.CookieStore;
import org.opendatakit.httpclientandroidlib.client.CredentialsProvider;
import org.opendatakit.httpclientandroidlib.client.HttpClient;
import org.opendatakit.httpclientandroidlib.client.methods.HttpGet;
import org.opendatakit.httpclientandroidlib.client.methods.HttpHead;
import org.opendatakit.httpclientandroidlib.client.methods.HttpPost;
import org.opendatakit.httpclientandroidlib.client.params.AuthPolicy;
import org.opendatakit.httpclientandroidlib.client.params.ClientPNames;
import org.opendatakit.httpclientandroidlib.client.params.HttpClientParams;
import org.opendatakit.httpclientandroidlib.client.protocol.ClientContext;
import org.opendatakit.httpclientandroidlib.conn.ClientConnectionManager;
import org.opendatakit.httpclientandroidlib.impl.client.BasicCookieStore;
import org.opendatakit.httpclientandroidlib.impl.client.DefaultHttpClient;
import org.opendatakit.httpclientandroidlib.params.BasicHttpParams;
import org.opendatakit.httpclientandroidlib.params.HttpConnectionParams;
import org.opendatakit.httpclientandroidlib.params.HttpParams;
import org.opendatakit.httpclientandroidlib.protocol.BasicHttpContext;
import org.opendatakit.httpclientandroidlib.protocol.HttpContext;
import org.xmlpull.v1.XmlPullParser;

import android.annotation.SuppressLint;
import android.text.format.DateFormat;
import android.util.Log;

/**
 * Common utility methods for managing the credentials associated with the
 * request context and constructing http context, client and request with the
 * proper parameters and OpenRosa headers.
 *
 * @author mitchellsundt@gmail.com
 */
public final class WebUtils {
  private static final String t = "WebUtils";

  public static final String HTTP_CONTENT_TYPE_TEXT_XML = "text/xml";
  public static final int CONNECTION_TIMEOUT = 45000;

  public static final String OPEN_ROSA_VERSION_HEADER = "X-OpenRosa-Version";
  public static final String OPEN_ROSA_VERSION = "1.0";

  private static final String DATE_HEADER = "Date";

  /**
   * Date format pattern used to parse HTTP date headers in RFC 1123 format.
   * copied from apache.commons.lang.DateUtils
   */
  private static final String PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz";

  /**
   * Date format pattern used to parse HTTP date headers in RFC 1036 format.
   * copied from apache.commons.lang.DateUtils
   */
  private static final String PATTERN_RFC1036 = "EEEE, dd-MMM-yy HH:mm:ss zzz";

  /**
   * Date format pattern used to parse HTTP date headers in ANSI C
   * <code>asctime()</code> format.
   * copied from apache.commons.lang.DateUtils
   */
  private static final String PATTERN_ASCTIME = "EEE MMM d HH:mm:ss yyyy";
  private static final String PATTERN_DATE_TOSTRING = "EEE MMM dd HH:mm:ss zzz yyyy";
  private static final String PATTERN_ISO8601_JAVAROSA = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  private static final String PATTERN_DATE_ONLY_JAVAROSA = "yyyy-MM-dd";
  private static final String PATTERN_TIME_ONLY_JAVAROSA = "HH:mm:ss.SSS'Z'";
  private static final String PATTERN_ISO8601 = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final String PATTERN_ISO8601_WITHOUT_ZONE = "yyyy-MM-dd'T'HH:mm:ss.SSS";
  private static final String PATTERN_ISO8601_DATE = "yyyy-MM-ddZ";
  private static final String PATTERN_ISO8601_TIME = "HH:mm:ss.SSSZ";
  private static final String PATTERN_YYYY_MM_DD_DATE_ONLY_NO_TIME_DASH = "yyyy-MM-dd";
  private static final String PATTERN_NO_DATE_TIME_ONLY = "HH:mm:ss.SSS";
  private static final String PATTERN_GOOGLE_DOCS = "MM/dd/yyyy HH:mm:ss.SSS";
  private static final String PATTERN_GOOGLE_DOCS_DATE_ONLY = "MM/dd/yyyy";

  private static final GregorianCalendar g = new GregorianCalendar(TimeZone.getTimeZone("GMT"));

  private static WebUtils webUtils = new WebUtils();
  
  static {
    // register a state-reset manipulator for 'webUtils' field.
    StaticStateManipulator.get().register(75, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        WebUtils utils = webUtils;
        webUtils = null;
        utils.httpConnectionManager.shutdown();
        webUtils = new WebUtils();
      }
      
    });
  }

  public static WebUtils get() { 
    return webUtils;
  }
  
  /**
   * For mocking -- supply a mocked object.
   * 
   * @param utils
   */
  public static void set(WebUtils utils) {
    webUtils = utils;
  }
  
  // share all session cookies across all sessions...
  private CookieStore cookieStore = new BasicCookieStore();
  // retain credentials for 7 minutes...
  private CredentialsProvider credsProvider = new AgingCredentialsProvider(7 * 60 * 1000);

  private ClientConnectionManager httpConnectionManager = null;

  protected WebUtils() {
  };

  @SuppressLint("SimpleDateFormat")
  private Date parseDateSubset( String value, String[] parsePatterns, Locale l, TimeZone tz) {
    // borrowed from apache.commons.lang.DateUtils...
    Date d = null;
    SimpleDateFormat parser = null;
    ParsePosition pos = new ParsePosition(0);
    for (int i = 0; i < parsePatterns.length; i++) {
      if (i == 0) {
        if ( l == null ) {
          parser = new SimpleDateFormat(parsePatterns[0]);
        } else {
          parser = new SimpleDateFormat(parsePatterns[0], l);
        }
      } else {
        parser.applyPattern(parsePatterns[i]);
      }
      parser.setTimeZone(tz); // enforce UTC for formats without timezones
      pos.setIndex(0);
      d = parser.parse(value, pos);
      if (d != null && pos.getIndex() == value.length()) {
        return d;
      }
    }
    return d;
  }
  /**
   * Parse a string into a datetime value. Tries the common Http formats, the
   * iso8601 format (used by Javarosa), the default formatting from
   * Date.toString(), and a time-only format.
   *
   * @param value
   * @return
   */
  public Date parseDate(String value) {
    if ( value == null || value.length() == 0 ) return null;

    String[] javaRosaPattern = new String[] {
        PATTERN_ISO8601_JAVAROSA,
        PATTERN_DATE_ONLY_JAVAROSA,
        PATTERN_TIME_ONLY_JAVAROSA };

    String[] iso8601Pattern = new String[] {
        PATTERN_ISO8601 };

    String[] localizedParsePatterns = new String[] {
        // try the common HTTP date formats that have time zones
        PATTERN_RFC1123,
        PATTERN_RFC1036,
        PATTERN_DATE_TOSTRING };

    String[] localizedNoTzParsePatterns = new String[] {
        // ones without timezones... (will assume UTC)
        PATTERN_ASCTIME };

    String[] tzParsePatterns = new String[] {
        PATTERN_ISO8601,
        PATTERN_ISO8601_DATE,
        PATTERN_ISO8601_TIME };

    String[] noTzParsePatterns = new String[] {
        // ones without timezones... (will assume UTC)
        PATTERN_ISO8601_WITHOUT_ZONE,
        PATTERN_NO_DATE_TIME_ONLY,
        PATTERN_YYYY_MM_DD_DATE_ONLY_NO_TIME_DASH,
        PATTERN_GOOGLE_DOCS };

    Date d = null;
    // iso8601 parsing is sometimes off-by-one when JR does it...
    d = parseDateSubset(value, iso8601Pattern, null, TimeZone.getTimeZone("GMT"));
    if ( d != null ) return d;
    // try to parse with the JavaRosa parsers (these are approximate -- timezone must be GMT)
    d = parseDateSubset(value, javaRosaPattern, null, TimeZone.getTimeZone("GMT"));
    if ( d != null ) return d;
    // try localized and english text parsers (for Web headers and interactive filter spec.)
    d = parseDateSubset(value, localizedParsePatterns, Locale.ENGLISH, TimeZone.getTimeZone("GMT"));
    if ( d != null ) return d;
    d = parseDateSubset(value, localizedParsePatterns, null, TimeZone.getTimeZone("GMT"));
    if ( d != null ) return d;
    d = parseDateSubset(value, localizedNoTzParsePatterns, Locale.ENGLISH, TimeZone.getTimeZone("GMT"));
    if ( d != null ) return d;
    d = parseDateSubset(value, localizedNoTzParsePatterns, null, TimeZone.getTimeZone("GMT"));
    if ( d != null ) return d;
    // try other common patterns that might not quite match JavaRosa parsers
    d = parseDateSubset(value, tzParsePatterns, null, TimeZone.getTimeZone("GMT"));
    if ( d != null ) return d;
    d = parseDateSubset(value, noTzParsePatterns, null, TimeZone.getTimeZone("GMT"));
    if ( d != null ) return d;
    throw new IllegalArgumentException("Unable to parse the date: " + value);
  }

  @SuppressLint("SimpleDateFormat")
  public String asSubmissionDateTimeString(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asJavarosaDateTime = new SimpleDateFormat(PATTERN_ISO8601_JAVAROSA);
    asJavarosaDateTime.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asJavarosaDateTime.format(d);
  }

  @SuppressLint("SimpleDateFormat")
  public String asSubmissionDateOnlyString(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asJavarosaDate = new SimpleDateFormat(PATTERN_DATE_ONLY_JAVAROSA);
    asJavarosaDate.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asJavarosaDate.format(d);
  }

  @SuppressLint("SimpleDateFormat")
  public String asSubmissionTimeOnlyString(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asJavarosaTime = new SimpleDateFormat(PATTERN_TIME_ONLY_JAVAROSA);
    asJavarosaTime.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asJavarosaTime.format(d);
  }

  /**
   * Return the GoogleDocs datetime string representation of a datetime.
   *
   * @param d
   * @return
   */
  @SuppressLint("SimpleDateFormat")
  public String googleDocsDateTime(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asGoogleDoc = new SimpleDateFormat(PATTERN_GOOGLE_DOCS);
    asGoogleDoc.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asGoogleDoc.format(d);
  }

  /**
   * Return the GoogleDocs date string representation of a date-only datetime.
   *
   * @param d
   * @return
   */
  @SuppressLint("SimpleDateFormat")
  public String googleDocsDateOnly(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asGoogleDocDateOnly = new SimpleDateFormat(PATTERN_GOOGLE_DOCS_DATE_ONLY);
    asGoogleDocDateOnly.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asGoogleDocDateOnly.format(d);
  }

  /**
   * Return the ISO8601 string representation of a date.
   *
   * @param d
   * @return
   */
  @SuppressLint("SimpleDateFormat")
  public String iso8601Date(Date d) {
    if (d == null)
      return null;
    // SDF is not thread-safe
    SimpleDateFormat asGMTiso8601 = new SimpleDateFormat(PATTERN_ISO8601); // with time zone
    asGMTiso8601.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asGMTiso8601.format(d);
  }

  /**
   * Return the RFC1123 string representation of a date.
   * @param d
   * @return
   */
  @SuppressLint("SimpleDateFormat")
  public String rfc1123Date(Date d) {
    if (d == null)
      return null;
    // SDF is not thread-safe
    SimpleDateFormat asGMTrfc1123 = new SimpleDateFormat(PATTERN_RFC1123); // with time zone
    asGMTrfc1123.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asGMTrfc1123.format(d);
  }

  /**
   * Construct the list of scopes (port + authProtocol) for a given host.
   * @param host
   * @return
   */
  public List<AuthScope> buildAuthScopes(String host) {
    List<AuthScope> asList = new ArrayList<AuthScope>();

    AuthScope a;
    // allow digest auth on any port...
    a = new AuthScope(host, -1, null, AuthPolicy.DIGEST);
    asList.add(a);
    // and allow basic auth on the standard TLS/SSL ports...
    a = new AuthScope(host, 443, null, AuthPolicy.BASIC);
    asList.add(a);
    a = new AuthScope(host, 8443, null, AuthPolicy.BASIC);
    asList.add(a);

    return asList;
  }

  public void clearAllCredentials() {
    HttpContext localContext = getHttpContext();
    CredentialsProvider credsProvider = (CredentialsProvider) localContext
        .getAttribute(ClientContext.CREDS_PROVIDER);
    Log.i(t, "clearAllCredentials");
    credsProvider.clear();
  }

  public boolean hasCredentials(String userEmail, String host) {
    HttpContext localContext = getHttpContext();
    CredentialsProvider credsProvider = (CredentialsProvider) localContext
        .getAttribute(ClientContext.CREDS_PROVIDER);

    List<AuthScope> asList = buildAuthScopes(host);
    boolean hasCreds = true;
    for (AuthScope a : asList) {
      Credentials c = credsProvider.getCredentials(a);
      if (c == null) {
        hasCreds = false;
        continue;
      }
    }
    return hasCreds;
  }

  /**
   * Remove all credentials for accessing the specified host.
   *
   * @param host
   */
  private void clearHostCredentials(String host) {
    HttpContext localContext = getHttpContext();
    CredentialsProvider credsProvider = (CredentialsProvider) localContext
        .getAttribute(ClientContext.CREDS_PROVIDER);
    Log.i(t, "clearHostCredentials: " + host);
    List<AuthScope> asList = buildAuthScopes(host);
    for (AuthScope a : asList) {
      credsProvider.setCredentials(a, null);
    }
  }

  /**
   * Remove all credentials for accessing the specified host and, if the
   * username is not null or blank then add a (username, password) credential
   * for accessing this host.
   *
   * @param username
   * @param password
   * @param host
   */
  public void addCredentials(String username, String password, String host) {
    HttpContext localContext = getHttpContext();
    // to ensure that this is the only authentication available for this
    // host...
    clearHostCredentials(host);
    if (username != null && username.trim().length() != 0) {
      Log.i(t, "adding credential for host: " + host + " username:" + username);
      Credentials c = new UsernamePasswordCredentials(username, password);
      addCredentials(localContext, c, host);
    }
  }

  private void addCredentials(HttpContext localContext, Credentials c, String host) {
    CredentialsProvider credsProvider = (CredentialsProvider) localContext
        .getAttribute(ClientContext.CREDS_PROVIDER);

    List<AuthScope> asList = buildAuthScopes(host);
    for (AuthScope a : asList) {
      credsProvider.setCredentials(a, c);
    }
  }

  private void setOpenRosaHeaders(HttpRequest req) {
    req.setHeader(OPEN_ROSA_VERSION_HEADER, OPEN_ROSA_VERSION);
    g.setTime(new Date());
    req.setHeader(DATE_HEADER, DateFormat.format("E, dd MMM yyyy hh:mm:ss zz", g).toString());
  }

  public HttpHead createOpenRosaHttpHead(URI uri) {
    HttpHead req = new HttpHead(uri);
    setOpenRosaHeaders(req);
    return req;
  }

  public HttpGet createOpenRosaHttpGet(URI uri) {
    return createOpenRosaHttpGet(uri, "");
  }

  public HttpGet createOpenRosaHttpGet(URI uri, String auth) {
    HttpGet req = new HttpGet();
    setOpenRosaHeaders(req);
    setGoogleHeaders(req, auth);
    req.setURI(uri);
    return req;
  }

  public void setGoogleHeaders(HttpRequest req, String auth) {
    if ((auth != null) && (auth.length() > 0)) {
      req.setHeader("Authorization", "GoogleLogin auth=" + auth);
    }
  }

  public HttpPost createOpenRosaHttpPost(URI uri) {
    return createOpenRosaHttpPost(uri, "");
  }

  public HttpPost createOpenRosaHttpPost(URI uri, String auth) {
    HttpPost req = new HttpPost(uri);
    setOpenRosaHeaders(req);
    setGoogleHeaders(req, auth);
    return req;
  }

  /**
   * Shared HttpContext so a user doesn't have to re-enter login information
   *
   * @return
   */
  public synchronized HttpContext getHttpContext() {

    // context holds authentication state machine, so it cannot be
    // shared across independent activities.
    HttpContext localContext = new BasicHttpContext();

    localContext.setAttribute(ClientContext.COOKIE_STORE, cookieStore);
    localContext.setAttribute(ClientContext.CREDS_PROVIDER, credsProvider);

    return localContext;
  }

  /**
   * Create an httpClient with connection timeouts and other parameters set.
   * Save and reuse the connection manager across invocations (this is what
   * requires synchronized access).
   *
   * @param timeout
   * @return HttpClient properly configured.
   */
  public synchronized HttpClient createHttpClient(int timeout) {
    return createHttpClient(timeout, 1);
  }

  public synchronized HttpClient createHttpClient(int timeout, int maxRedirects) {
    // configure connection
    HttpParams params = new BasicHttpParams();
    HttpConnectionParams.setConnectionTimeout(params, timeout);
    HttpConnectionParams.setSoTimeout(params, 2 * timeout);
    // support redirecting to handle http: => https: transition
    HttpClientParams.setRedirecting(params, true);
    // support authenticating
    HttpClientParams.setAuthenticating(params, true);
    // if possible, bias toward digest auth (may not be in 4.0 beta 2)
    List<String> authPref = new ArrayList<String>();
    authPref.add(AuthPolicy.DIGEST);
    authPref.add(AuthPolicy.BASIC);
    // does this work in Google's 4.0 beta 2 snapshot?
    params.setParameter("http.auth-target.scheme-pref", authPref);

    // setup client
    HttpClient httpclient;

    // reuse the connection manager across all clients this ODK Survey
    // creates.
    if (httpConnectionManager == null) {
      // let Apache stack create a connection manager.
      httpclient = new DefaultHttpClient(params);
      httpConnectionManager = httpclient.getConnectionManager();
    } else {
      // reuse the connection manager we already got.
      httpclient = new DefaultHttpClient(httpConnectionManager, params);
    }

    httpclient.getParams().setParameter(ClientPNames.MAX_REDIRECTS, maxRedirects);
    httpclient.getParams().setParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, true);

    return httpclient;
  }

  /**
   * Utility to ensure that the entity stream of a response is drained of bytes.
   *
   * @param response
   */
  public void discardEntityBytes(HttpResponse response) {
    // may be a server that does not handle
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      try {
        // have to read the stream in order to reuse the connection
        InputStream is = response.getEntity().getContent();
        // read to end of stream...
        final long count = 1024L;
        while (is.skip(count) == count)
          ;
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Common method for returning a parsed xml document given a url and the http
   * context and client objects involved in the web connection.
   *
   * @param urlString
   * @param localContext
   * @param httpclient
   * @return
   */
  public DocumentFetchResult getXmlDocument(String urlString, HttpContext localContext,
      HttpClient httpclient, String auth) {
    URI u = null;
    try {
      URL url = new URL(URLDecoder.decode(urlString, CharEncoding.UTF_8));
      u = url.toURI();
    } catch (Exception e) {
      e.printStackTrace();
      return new DocumentFetchResult(e.getLocalizedMessage()
      // + app.getString(R.string.while_accessing) + urlString);
          + ("while accessing") + urlString, 0);
    }

    // set up request...
    HttpGet req = createOpenRosaHttpGet(u, auth);

    HttpResponse response = null;
    try {
      response = httpclient.execute(req, localContext);
      int statusCode = response.getStatusLine().getStatusCode();

      HttpEntity entity = response.getEntity();

      if (statusCode != 200) {
        discardEntityBytes(response);
        String webError = response.getStatusLine().getReasonPhrase() + " (" + statusCode + ")";

        return new DocumentFetchResult(u.toString() + " responded with: " + webError, statusCode);
      }

      if (entity == null) {
        String error = "No entity body returned from: " + u.toString();
        Log.e(t, error);
        return new DocumentFetchResult(error, 0);
      }

      if (!entity.getContentType().getValue().toLowerCase(Locale.ENGLISH)
          .contains(WebUtils.HTTP_CONTENT_TYPE_TEXT_XML)) {
        discardEntityBytes(response);
        String error = "ContentType: "
            + entity.getContentType().getValue()
            + " returned from: "
            + u.toString()
            + " is not text/xml.  This is often caused a network proxy.  Do you need to login to your network?";
        Log.e(t, error);
        return new DocumentFetchResult(error, 0);
      }

      // parse response
      Document doc = null;
      try {
        InputStream is = null;
        InputStreamReader isr = null;
        try {
          is = entity.getContent();
          isr = new InputStreamReader(is, Charsets.UTF_8);
          doc = new Document();
          KXmlParser parser = new KXmlParser();
          parser.setInput(isr);
          parser.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);
          doc.parse(parser);
          isr.close();
          isr = null;
        } finally {
          if (isr != null) {
            try {
              // ensure stream is consumed...
              final long count = 1024L;
              while (isr.skip(count) == count)
                ;
            } catch (Exception e) {
              // no-op
            }
            try {
              isr.close();
            } catch (Exception e) {
              // no-op
            }
          }
          if (is != null) {
            try {
              is.close();
            } catch (Exception e) {
              // no-op
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        String error = "Parsing failed with " + e.getMessage() + "while accessing " + u.toString();
        Log.e(t, error);
        return new DocumentFetchResult(error, 0);
      }

      boolean isOR = false;
      Header[] fields = response.getHeaders(WebUtils.OPEN_ROSA_VERSION_HEADER);
      if (fields != null && fields.length >= 1) {
        isOR = true;
        boolean versionMatch = false;
        boolean first = true;
        StringBuilder b = new StringBuilder();
        for (Header h : fields) {
          if (WebUtils.OPEN_ROSA_VERSION.equals(h.getValue())) {
            versionMatch = true;
            break;
          }
          if (!first) {
            b.append("; ");
          }
          first = false;
          b.append(h.getValue());
        }
        if (!versionMatch) {
          Log.w(t, WebUtils.OPEN_ROSA_VERSION_HEADER + " unrecognized version(s): " + b.toString());
        }
      }
      return new DocumentFetchResult(doc, isOR);
    } catch (Exception e) {
      clearHttpConnectionManager();
      e.printStackTrace();
      String cause;
      if (e.getCause() != null) {
        cause = e.getCause().getMessage();
      } else {
        cause = e.getMessage();
      }
      String error = "Error: " + cause + " while accessing " + u.toString();

      Log.w(t, error);
      return new DocumentFetchResult(error, 0);
    }
  }

  public void clearHttpConnectionManager() {
    // If we get an unexpected exception, the safest thing is to close
    // all connections
    // so that if there is garbage on the connection we ensure it is
    // removed. This
    // is especially important if the connection times out.
    httpConnectionManager.shutdown();
    httpConnectionManager = null;
  }
}
