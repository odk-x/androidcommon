package org.opendatakit.test;

import org.junit.Test;
import org.opendatakit.webkitserver.utilities.UrlUtils;

import static org.junit.Assert.*;

public class UrlUtilsTest {

    private static final String EMPTY_STRING = "";
    private static final String URL_SEGMENT1 = "test/";
    private static final String FILE_PATH_PREFIX = "this/test/file/path/";
    private static final String HTML_EXTENSION = ".html";
    private static final String QUERY_PARAM_PREFIX = "?";
    private static final String HASH_PREFIX = "#";
    
    private static final String FILE_PATH_WITH_HTML = FILE_PATH_PREFIX + HTML_EXTENSION;
    private static final String FILE_WITH_HASH = URL_SEGMENT1 + "file" + HASH_PREFIX + "foo";
    private static final String FILE_WITH_QUERY_PARAMS = "pretty/little/liar" + QUERY_PARAM_PREFIX + "foo&bar=3";
    private static final String FILE_WITH_HASH_AND_QUERY = URL_SEGMENT1 + "test/test" + HTML_EXTENSION + HASH_PREFIX + "foo" + QUERY_PARAM_PREFIX + "bar=3&baz=55";
    private static final String FILE_WITHOUT_PARAMS = URL_SEGMENT1 + "test/test" + HTML_EXTENSION;
    private static final String FILE_WITH_HASH_ONLY = URL_SEGMENT1 + "test" + HTML_EXTENSION + HASH_PREFIX + "foo";
    private static final String FILE_WITH_QUERY = "this/is/a/file/that/i/like" + HTML_EXTENSION + QUERY_PARAM_PREFIX + "foo=bar";
    private static final String FILE_WITH_BOTH = "foo/bar" + HTML_EXTENSION + HASH_PREFIX + "foo" + QUERY_PARAM_PREFIX + "bar=baz";

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
        assertRetrieveFileNameHelper(URL_SEGMENT1 + "test/test" + HTML_EXTENSION, FILE_WITH_HASH_AND_QUERY);
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
        assertGetParamsHelper(FILE_WITH_HASH, HASH_PREFIX + "foo");
    }

    @Test
    public void testGetParamsQuery() {
        assertGetParamsHelper(FILE_WITH_QUERY_PARAMS, QUERY_PARAM_PREFIX + "foo&bar=3");
    }

    @Test
    public void testGetParamsBoth() {
        assertGetParamsHelper(FILE_WITH_HASH_AND_QUERY, HASH_PREFIX + "foo" + QUERY_PARAM_PREFIX + "bar=3&baz=55");
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
     * Since getIndexOfParameters is package-private, we'll use reflection to access it
     * @param segment URL segment to test
     * @param expected Expected index of parameters
     */
    protected void assertGetIndexHelper(String segment, int expected) {
        try {
            java.lang.reflect.Method method = UrlUtils.class.getDeclaredMethod("getIndexOfParameters", String.class);
            method.setAccessible(true);
            int actual = (int) method.invoke(null, segment);
            assertEquals(expected, actual);
        } catch (Exception e) {
            fail("Could not access getIndexOfParameters method: " + e.getMessage());
        }
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