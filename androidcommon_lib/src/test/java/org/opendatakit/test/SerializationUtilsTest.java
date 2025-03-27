package org.opendatakit.test;

import static org.junit.Assert.*;

import android.os.Bundle;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.webkitserver.utilities.SerializationUtils;
import org.robolectric.RobolectricTestRunner;
import java.util.Arrays;

@RunWith(RobolectricTestRunner.class)
public class SerializationUtilsTest {

    private final String APP_NAME = "testApp";

    @Test
    public void testConvertFromBundle_withPrimitiveValues() throws JSONException {
        Bundle bundle = new Bundle();
        bundle.putString("key1", "value1");
        bundle.putInt("key2", 123);
        bundle.putBoolean("key3", true);
        bundle.putDouble("key4", 45.67);

        JSONObject json = SerializationUtils.convertFromBundle(APP_NAME, bundle);

        assertEquals("value1", json.getString("key1"));
        assertEquals(123, json.getInt("key2"));
        assertTrue(json.getBoolean("key3"));
        assertEquals(45.67, json.getDouble("key4"), 0.001);
    }

    @Test
    public void testConvertFromBundle_withArrayValues() throws JSONException {
        Bundle bundle = new Bundle();
        bundle.putIntArray("intArray", new int[]{1, 2, 3});
        bundle.putStringArray("stringArray", new String[]{"a", "b", "c"});

        JSONObject json = SerializationUtils.convertFromBundle(APP_NAME, bundle);

        JSONArray intArray = json.getJSONArray("intArray");
        assertEquals(3, intArray.length());
        assertEquals(1, intArray.getInt(0));
        assertEquals(2, intArray.getInt(1));
        assertEquals(3, intArray.getInt(2));

        JSONArray stringArray = json.getJSONArray("stringArray");
        assertEquals(3, stringArray.length());
        assertEquals("a", stringArray.getString(0));
        assertEquals("b", stringArray.getString(1));
        assertEquals("c", stringArray.getString(2));
    }

    @Test
    public void testConvertFromBundle_withNestedBundle() throws JSONException {
        Bundle innerBundle = new Bundle();
        innerBundle.putString("innerKey", "innerValue");

        Bundle outerBundle = new Bundle();
        outerBundle.putBundle("nestedBundle", innerBundle);

        JSONObject json = SerializationUtils.convertFromBundle(APP_NAME, outerBundle);
        JSONObject nestedJson = json.getJSONObject("nestedBundle");

        assertEquals("innerValue", nestedJson.getString("innerKey"));
    }

    @Test
    public void testConvertToBundle_withPrimitiveValues() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("key1", "value1");
        json.put("key2", 123);
        json.put("key3", true);
        json.put("key4", 45.67);

        Bundle bundle = SerializationUtils.convertToBundle(json, null);

        assertEquals("value1", bundle.getString("key1"));
        assertEquals(123, bundle.getInt("key2"));
        assertTrue(bundle.getBoolean("key3"));
        assertEquals(45.67, bundle.getDouble("key4"), 0.001);
    }

    @Test
    public void testConvertToBundle_withArrayValues() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("intArray", new JSONArray(Arrays.asList(1, 2, 3)));
        json.put("stringArray", new JSONArray(Arrays.asList("a", "b", "c")));

        Bundle bundle = SerializationUtils.convertToBundle(json, null);

        int[] intArray = bundle.getIntArray("intArray");
        assertNotNull(intArray);
        assertArrayEquals(new int[]{1, 2, 3}, intArray);

        String[] stringArray = bundle.getStringArray("stringArray");
        assertNotNull(stringArray);
        assertArrayEquals(new String[]{"a", "b", "c"}, stringArray);
    }

    @Test
    public void testConvertToBundle_withNestedJSONObject() throws JSONException {
        JSONObject nestedJson = new JSONObject();
        nestedJson.put("innerKey", "innerValue");

        JSONObject json = new JSONObject();
        json.put("nestedBundle", nestedJson);

        Bundle bundle = SerializationUtils.convertToBundle(json, null);
        Bundle nestedBundle = bundle.getBundle("nestedBundle");

        assertNotNull(nestedBundle);
        assertEquals("innerValue", nestedBundle.getString("innerKey"));
    }

    @Test
    public void testConvertFromBundle_withNullValues() throws JSONException {
        Bundle bundle = new Bundle();
        bundle.putString("nullKey", null);

        JSONObject json = SerializationUtils.convertFromBundle(APP_NAME, bundle);

        assertTrue(json.has("nullKey"));
        assertTrue(json.isNull("nullKey"));
    }

    @Test
    public void testConvertToBundle_withNullValues() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("nullKey", JSONObject.NULL);

        Bundle bundle = SerializationUtils.convertToBundle(json, null);

        assertNull(bundle.getString("nullKey"));
    }

    @Test
    public void testConvertToBundle_withMacroStringExpander() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("expandableKey", "Hello, ${name}");

        SerializationUtils.MacroStringExpander expander = value -> value.replace("${name}", "World");

        Bundle bundle = SerializationUtils.convertToBundle(json, expander);

        assertEquals("Hello, World", bundle.getString("expandableKey"));
    }

    @Test(expected = JSONException.class)
    public void testConvertToBundle_withNestedJSONArray_throwsException() throws JSONException {
        JSONObject json = new JSONObject();
        JSONArray nestedArray = new JSONArray();
        nestedArray.put(new JSONArray(Arrays.asList(1, 2, 3)));  // Nested array

        json.put("nestedArray", nestedArray);

        SerializationUtils.convertToBundle(json, null);
    }
}
