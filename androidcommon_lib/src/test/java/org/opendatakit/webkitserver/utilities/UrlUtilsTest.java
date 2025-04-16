package org.opendatakit.webkitserver.utilities;

import org.junit.Test;

import static org.junit.Assert.*;

public class UrlUtilsTest {

    private static final String EMPTY_STRING = "";
    private static final String URL_SEGMENT1 = "test/";
    private static final String FILE_PATH_PREFIX = "this/test/file/path/";
    private static final String HTML_EXTENSION = ".html";
    private static final String QUERY_PARAM_PREFIX = "?";
    private static final String HASH_PREFIX = "#";
    private static final String TEST_PATH = "test/test";

    // File paths
    private static final String FILE_PATH_WITH_HTML = FILE_PATH_PREFIX + HTML_EXTENSION;

    // Query and hash parameter values
    private static final String HASH_VALUE_FOO = "foo";
    private static final String QUERY_VALUE_FOO_AND_BAR = "foo&bar=3";
    private static final String QUERY_VALUE_BAR_AND_BAZ = "bar=3&baz=55";
    private static final String QUERY_VALUE_FOO_BAR = "foo=bar";
    private static final String QUERY_VALUE_BAR_BAZ = "bar=baz";

    // Common path components
    private static final String TEST_HTML_PATH = URL_SEGMENT1 + TEST_PATH + HTML_EXTENSION;

    // Complete URL fragments with parameters
    private static final String FILE_WITH_HASH = URL_SEGMENT1 + "file" + HASH_PREFIX + HASH_VALUE_FOO;
    private static final String FILE_WITH_QUERY_PARAMS = "pretty/little/liar" + QUERY_PARAM_PREFIX + QUERY_VALUE_FOO_AND_BAR;
    private static final String FILE_WITH_HASH_AND_QUERY = TEST_HTML_PATH +
            HASH_PREFIX + HASH_VALUE_FOO +
            QUERY_PARAM_PREFIX + QUERY_VALUE_BAR_AND_BAZ;
    private static final String FILE_WITHOUT_PARAMS = TEST_HTML_PATH;
    private static final String FILE_WITH_HASH_ONLY = URL_SEGMENT1 + "test" + HTML_EXTENSION +
            HASH_PREFIX + HASH_VALUE_FOO;
    private static final String FILE_WITH_QUERY = "this/is/a/file/that/i/like" + HTML_EXTENSION +
            QUERY_PARAM_PREFIX + QUERY_VALUE_FOO_BAR;
    private static final String FILE_WITH_BOTH = "foo/bar" + HTML_EXTENSION +
            HASH_PREFIX + HASH_VALUE_FOO +
            QUERY_PARAM_PREFIX + QUERY_VALUE_BAR_BAZ;

    @Test
    public void testNoHashOrParameters() {
        assertRetrieveFileNameHelper(FILE_PATH_WITH_HTML, FILE_PATH_WITH_HTML);
    }

    @Test
    public void testEmptyString() {
        assertRetrieveFileNameHelper(EMPTY_STRING, EMPTY_STRING);
    }

    @Test
    public void testOnlyHash() {
        assertRetrieveFileNameHelper(URL_SEGMENT1 + "file", FILE_WITH_HASH);
    }

    @Test
    public void testOnlyQueryParams() {
        assertRetrieveFileNameHelper("pretty/little/liar", FILE_WITH_QUERY_PARAMS);
    }

    @Test
    public void testHashAndQueryParams() {
        assertRetrieveFileNameHelper(TEST_HTML_PATH, FILE_WITH_HASH_AND_QUERY);
    }

    @Test
    public void testGetIndexOfParamsNoParams() {
        assertGetIndexHelper(FILE_WITHOUT_PARAMS, -1);
    }

    @Test
    public void testGetIndexOfParamsHash() {
        assertGetIndexHelper(FILE_WITH_HASH_ONLY, 14);
    }

    @Test
    public void testGetIndexOfQueryHash() {
        assertGetIndexHelper(FILE_WITH_QUERY, 31);
    }

    @Test
    public void testGetIndexOfBoth() {
        assertGetIndexHelper(FILE_WITH_BOTH, 12);
    }

    @Test
    public void testGetParamsNone() {
        assertGetParamsHelper(FILE_PATH_WITH_HTML, EMPTY_STRING);
    }

    @Test
    public void testGetParamsHash() {
        assertGetParamsHelper(FILE_WITH_HASH, HASH_PREFIX + HASH_VALUE_FOO);
    }

    @Test
    public void testGetParamsQuery() {
        assertGetParamsHelper(FILE_WITH_QUERY_PARAMS, QUERY_PARAM_PREFIX + QUERY_VALUE_FOO_AND_BAR);
    }

    @Test
    public void testGetParamsBoth() {
        assertGetParamsHelper(FILE_WITH_HASH_AND_QUERY, HASH_PREFIX + HASH_VALUE_FOO + QUERY_PARAM_PREFIX + QUERY_VALUE_BAR_AND_BAZ);
    }

    /**
     * Take start, retrieve the file name, and assert that the result is equal to
     * expected.
     * @param expected Expected file path
     * @param start Input URL segment
     */
    protected void assertRetrieveFileNameHelper(String expected, String start) {
        String result = UrlUtils.getPathFromUriFragment(start);
        assertEquals(expected, result);
    }

    /**
     * Test the package-private getIndexOfParameters method directly
     * @param segment URL segment to test
     * @param expected Expected index of parameters
     */
    protected void assertGetIndexHelper(String segment, int expected) {
        int actual = UrlUtils.getIndexOfParameters(segment);
        assertEquals(expected, actual);
    }

    /**
     * Assert the parameters extracted from the URL segment
     * @param segment URL segment to test
     * @param expected Expected parameters string
     */
    protected void assertGetParamsHelper(String segment, String expected) {
        String actual = UrlUtils.getParametersFromUriFragment(segment);
        assertEquals(expected, actual);
    }

}

