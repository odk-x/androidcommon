package org.opendatakit.test;

import org.junit.Test;
import org.opendatakit.webkitserver.utilities.UrlUtils;

import static org.junit.Assert.*;

public class UrlUtilsTest {

    @Test
    public void testNoHashOrParameters() {
        String fileName = "this/test/file/path/.html";
        assertRetrieveFileNameHelper(fileName, fileName);
    }

    @Test
    public void testEmptyString() {
        assertRetrieveFileNameHelper("", "");
    }

    @Test
    public void testOnlyHash() {
        String segment = "test/file#foo";
        assertRetrieveFileNameHelper("test/file", segment);
    }

    @Test
    public void testOnlyQueryParams() {
        String segment = "pretty/little/liar?foo&bar=3";
        assertRetrieveFileNameHelper("pretty/little/liar", segment);
    }

    @Test
    public void testHashAndQueryParams() {
        String segment = "test/test/test.html#foo?bar=3&baz=55";
        assertRetrieveFileNameHelper("test/test/test.html", segment);
    }

    @Test
    public void testGetIndexOfParamsNoParams() {
        String segment = "test/test/test.html";
        assertGetIndexHelper(segment, -1);
    }

    @Test
    public void testGetIndexOfParamsHash() {
        String segment = "test/test.html#foo";
        int expected = 14;
        assertGetIndexHelper(segment, expected);
    }

    @Test
    public void testGetIndexOfQueryHash() {
        String segment = "this/is/a/file/that/i/like.html?foo=bar";
        int expected = 31;
        assertGetIndexHelper(segment, expected);
    }

    @Test
    public void testGetIndexOfBoth() {
        String segment = "foo/bar.html#foo?bar=baz";
        int expected = 12;
        assertGetIndexHelper(segment, expected);
    }

    @Test
    public void testGetParamsNone() {
        String segment = "this/test/file/path/.html";
        assertGetParamsHelper(segment, "");
    }

    @Test
    public void testGetParamsHash() {
        String segment = "test/file#foo";
        assertGetParamsHelper(segment, "#foo");
    }

    @Test
    public void testGetParamsQuery() {
        String segment = "pretty/little/liar?foo&bar=3";
        assertGetParamsHelper(segment, "?foo&bar=3");
    }

    @Test
    public void testGetParamsBoth() {
        String segment = "test/test/test.html#foo?bar=3&baz=55";
        assertGetParamsHelper(segment, "#foo?bar=3&baz=55");
    }

    /**
     * Take start, retrieve the file name, and assert that the result is equal to
     * expected.
     * @param expected
     * @param start
     */
    protected void assertRetrieveFileNameHelper(String expected, String start) {
        String result = UrlUtils.getPathFromUriFragment(start);
        assertEquals(expected, result);
    }

    /**
     * Since getIndexOfParameters is package-private, we'll use reflection to access it
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

    protected void assertGetParamsHelper(String segment, String expected) {
        String actual = UrlUtils.getParametersFromUriFragment(segment);
        assertEquals(expected, actual);
    }
}