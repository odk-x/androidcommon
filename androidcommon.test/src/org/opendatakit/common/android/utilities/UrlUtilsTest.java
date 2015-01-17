package org.opendatakit.common.android.utilities;

import android.test.AndroidTestCase;

public class UrlUtilsTest extends AndroidTestCase {
  
  public void testNoHashOrParameters() {
    String fileName = "this/test/file/path/.html";
    this.assertRetrieveFileNameHelper(fileName, fileName);
  }
  
  public void testEmptyString() {
    this.assertRetrieveFileNameHelper("", ""); 
  }
  
  public void testOnlyHash() {
    String segment = "test/file#foo";
    this.assertRetrieveFileNameHelper("test/file", segment);
  }
  
  public void testOnlyQueryParams() {
    String segment = "pretty/little/liar?foo&bar=3";
    this.assertRetrieveFileNameHelper("pretty/little/liar", segment);
  }
  
  public void hashAndQueryParams() {
    String segment = "test/test/test.html#foo?bar=3&baz=55";
    this.assertRetrieveFileNameHelper("test/test/test.html", segment);
  }
  
  /**
   * Take start, retrieve the file name, and assert that the result is equal to
   * expected.
   * @param expected
   * @param start
   */
  protected void assertRetrieveFileNameHelper(String expected, String start) {
    String result = UrlUtils.getFileNameFromUriSegment(start);
    assertEquals(expected, result);
  }

}
