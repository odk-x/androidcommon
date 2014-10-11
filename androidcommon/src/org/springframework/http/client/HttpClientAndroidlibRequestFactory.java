/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.http.client;

import java.io.IOException;
import java.net.URI;

import org.opendatakit.common.android.utilities.ClientConnectionManagerFactory;
import org.opendatakit.httpclientandroidlib.client.HttpClient;
import org.opendatakit.httpclientandroidlib.client.methods.HttpDelete;
import org.opendatakit.httpclientandroidlib.client.methods.HttpGet;
import org.opendatakit.httpclientandroidlib.client.methods.HttpHead;
import org.opendatakit.httpclientandroidlib.client.methods.HttpOptions;
import org.opendatakit.httpclientandroidlib.client.methods.HttpPost;
import org.opendatakit.httpclientandroidlib.client.methods.HttpPut;
import org.opendatakit.httpclientandroidlib.client.methods.HttpTrace;
import org.opendatakit.httpclientandroidlib.client.methods.HttpUriRequest;
import org.opendatakit.httpclientandroidlib.conn.ClientConnectionManager;
import org.opendatakit.httpclientandroidlib.params.CoreConnectionPNames;
import org.opendatakit.httpclientandroidlib.params.HttpParams;
import org.opendatakit.httpclientandroidlib.params.HttpProtocolParams;
import org.opendatakit.httpclientandroidlib.protocol.HttpContext;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.http.HttpMethod;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.http.client.ClientHttpRequestFactory} implementation that uses <a
 * href="http://hc.apache.org/httpcomponents-client-ga/httpclient/">Http Components HttpClient</a> to create requests.
 *
 * <p>
 * Allows to use a pre-configured {@link HttpClient} instance - potentially with authentication, HTTP connection
 * pooling, etc.
 *
 * @author Oleg Kalnichevski
 * @author Roy Clarkson
 * @since 1.0
 */
public class HttpClientAndroidlibRequestFactory implements ClientHttpRequestFactory, DisposableBean {

	private static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 100;

	private static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 5;

	private static final int DEFAULT_READ_TIMEOUT_MILLISECONDS = (60 * 1000);

	private String appName;
	private HttpClient httpClient;

	/**
	 * Create a new instance of the HttpComponentsClientHttpRequestFactory
	 * with the given timeout and redirect limits.
    * 
	 * @param appName the AppName selects the connection manager.
	 * @param timeout
	 * @param maxRedirects
	 */
	public HttpClientAndroidlibRequestFactory(String appName, int timeout, int maxRedirects) {
     Assert.notNull(appName, "appName must not be null");
	  this.appName = appName;
	  this.httpClient = ClientConnectionManagerFactory.get(appName).createHttpClient(timeout, maxRedirects);
	}

	/**
	 * Return the {@code HttpClient} used by this factory.
	 */
	public HttpClient getHttpClient() {
		return this.httpClient;
	}

	/**
	 * Set the connection timeout for the underlying HttpClient. A timeout value of 0 specifies an infinite timeout.
	 * @param timeout the timeout value in milliseconds
	 */
	public void setConnectTimeout(int timeout) {
		Assert.isTrue(timeout >= 0, "Timeout must be a non-negative value");
		getHttpClient().getParams().setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, timeout);
	}

	/**
	 * Set the socket read timeout for the underlying HttpClient. A timeout value of 0 specifies an infinite timeout.
	 * @param timeout the timeout value in milliseconds
	 */
	public void setReadTimeout(int timeout) {
		Assert.isTrue(timeout >= 0, "Timeout must be a non-negative value");
		getHttpClient().getParams().setIntParameter(CoreConnectionPNames.SO_TIMEOUT, timeout);
	}

	public ClientHttpRequest createRequest(URI uri, HttpMethod httpMethod) throws IOException {
		HttpUriRequest httpRequest = createHttpRequest(httpMethod, uri);
		postProcessHttpRequest(httpRequest);
		return new HttpClientAndroidlibHttpRequest(getHttpClient(), httpRequest, createHttpContext(httpMethod, uri));
	}

	/**
	 * Create a HttpComponents HttpUriRequest object for the given HTTP method and URI specification.
	 *
	 * @param httpMethod the HTTP method
	 * @param uri the URI
	 * @return the HttpComponents HttpUriRequest object
	 */
	protected HttpUriRequest createHttpRequest(HttpMethod httpMethod, URI uri) {
		switch (httpMethod) {
			case GET:
				return new HttpGet(uri);
			case DELETE:
				return new HttpDelete(uri);
			case HEAD:
				return new HttpHead(uri);
			case OPTIONS:
				return new HttpOptions(uri);
			case POST:
				return new HttpPost(uri);
			case PUT:
				return new HttpPut(uri);
			case TRACE:
				return new HttpTrace(uri);
			default:
				throw new IllegalArgumentException("Invalid HTTP method: " + httpMethod);
		}
	}

	/**
	 * Template method that allows for manipulating the {@link HttpUriRequest} before it is returned as part of a
	 * {@link HttpClientAndroidlibHttpRequest}.
	 * <p>
	 * The default implementation is empty.
	 *
	 * @param httpRequest the HTTP request object to process
	 */
	protected void postProcessHttpRequest(HttpUriRequest httpRequest) {
		HttpParams params = httpRequest.getParams();
		HttpProtocolParams.setUseExpectContinue(params, false);
	}

	/**
	 * Template methods that creates a {@link HttpContext} for the given HTTP method and URI.
	 * <p>
	 * The default implementation returns {@code null}.
	 * @param httpMethod the HTTP method
	 * @param uri the URI
	 * @return the http context
	 */
	protected HttpContext createHttpContext(HttpMethod httpMethod, URI uri) {
		return ClientConnectionManagerFactory.get(appName).getHttpContext();
	}

	/**
	 * Shutdown hook that closes the underlying {@link ClientConnectionManager}'s connection pool, if any.
	 */
	public void destroy() {
	  ClientConnectionManagerFactory.get(appName).clearHttpConnectionManager();
	}

}
