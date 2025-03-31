package org.opendatakit.webkitserver.utilities;

import android.os.Bundle;
import android.os.Parcelable;

import androidx.test.ext.junit.runners.AndroidJUnit4;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.webkitserver.utilities.SerializationUtils.MacroStringExpander;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(AndroidJUnit4.class)
public class SerializationUtilsTest {

    private static final String TEST_APP_NAME = "testApp";
    private static final String KEY_STRING = "stringKey";
    private static final String KEY_INT = "intKey";
    private static final String KEY_LONG = "longKey";
    private static final String KEY_DOUBLE = "doubleKey";
    private static final String KEY_BOOLEAN = "booleanKey";
    private static final String KEY_NULL = "nullKey";
    private static final String KEY_BUNDLE = "bundleKey";
    private static final String KEY_STRING_ARRAY = "stringArrayKey";
    private static final String KEY_INT_ARRAY = "intArrayKey";
    private static final String KEY_LONG_ARRAY = "longArrayKey";
    private static final String KEY_DOUBLE_ARRAY = "doubleArrayKey";
    private static final String KEY_BOOLEAN_ARRAY = "booleanArrayKey";
    private static final String KEY_BUNDLE_ARRAY = "bundleArrayKey";

    @Test
    public void testConvertFromBundleSimpleTypes() throws JSONException {
        // Create a bundle with various simple types
        Bundle bundle = new Bundle();
        bundle.putString(KEY_STRING, "test string");
        bundle.putInt(KEY_INT, 42);
        bundle.putLong(KEY_LONG, 9999999999L);
        bundle.putDouble(KEY_DOUBLE, 3.14159);
        bundle.putBoolean(KEY_BOOLEAN, true);
        bundle.putString(KEY_NULL, null);

        // Convert to JSON
        JSONObject json = SerializationUtils.convertFromBundle(TEST_APP_NAME, bundle);

        // Verify conversion was correct
        assertEquals("test string", json.getString(KEY_STRING));
        assertEquals(42, json.getInt(KEY_INT));
        assertEquals(9999999999L, json.getLong(KEY_LONG));
        assertEquals(3.14159, json.getDouble(KEY_DOUBLE), 0.0000001);
        assertTrue(json.getBoolean(KEY_BOOLEAN));
        assertTrue(json.has(KEY_NULL));
        assertTrue(json.isNull(KEY_NULL));
    }

    @Test
    public void testConvertFromBundleArrayTypes() throws JSONException {
        // Create a bundle with various array types
        Bundle bundle = new Bundle();
        bundle.putStringArray(KEY_STRING_ARRAY, new String[]{"one", null, "three"});
        bundle.putIntArray(KEY_INT_ARRAY, new int[]{1, 2, 3});
        bundle.putLongArray(KEY_LONG_ARRAY, new long[]{100L, 200L, 300L});
        bundle.putDoubleArray(KEY_DOUBLE_ARRAY, new double[]{1.1, 2.2, 3.3});
        bundle.putBooleanArray(KEY_BOOLEAN_ARRAY, new boolean[]{true, false, true});

        // Convert to JSON
        JSONObject json = SerializationUtils.convertFromBundle(TEST_APP_NAME, bundle);

        // Verify string array
        JSONArray stringArray = json.getJSONArray(KEY_STRING_ARRAY);
        assertEquals(3, stringArray.length());
        assertEquals("one", stringArray.getString(0));
        assertTrue(stringArray.isNull(1));
        assertEquals("three", stringArray.getString(2));

        // Verify int array
        JSONArray intArray = json.getJSONArray(KEY_INT_ARRAY);
        assertEquals(3, intArray.length());
        assertEquals(1, intArray.getInt(0));
        assertEquals(2, intArray.getInt(1));
        assertEquals(3, intArray.getInt(2));

        // Verify long array
        JSONArray longArray = json.getJSONArray(KEY_LONG_ARRAY);
        assertEquals(3, longArray.length());
        assertEquals(100L, longArray.getLong(0));
        assertEquals(200L, longArray.getLong(1));
        assertEquals(300L, longArray.getLong(2));

        // Verify double array
        JSONArray doubleArray = json.getJSONArray(KEY_DOUBLE_ARRAY);
        assertEquals(3, doubleArray.length());
        assertEquals(1.1, doubleArray.getDouble(0), 0.0001);
        assertEquals(2.2, doubleArray.getDouble(1), 0.0001);
        assertEquals(3.3, doubleArray.getDouble(2), 0.0001);

        // Verify boolean array
        JSONArray booleanArray = json.getJSONArray(KEY_BOOLEAN_ARRAY);
        assertEquals(3, booleanArray.length());
        assertTrue(booleanArray.getBoolean(0));
        assertFalse(booleanArray.getBoolean(1));
        assertTrue(booleanArray.getBoolean(2));
    }

    @Test
    public void testConvertFromBundleNestedBundles() throws JSONException {
        // Create nested bundles
        Bundle childBundle = new Bundle();
        childBundle.putString(KEY_STRING, "child string");
        childBundle.putInt(KEY_INT, 99);

        Bundle parentBundle = new Bundle();
        parentBundle.putBundle(KEY_BUNDLE, childBundle);

        // Create bundle array
        Bundle[] bundleArray = new Bundle[2];
        bundleArray[0] = new Bundle();
        bundleArray[0].putString(KEY_STRING, "array bundle 1");
        bundleArray[1] = new Bundle();
        bundleArray[1].putString(KEY_STRING, "array bundle 2");

        parentBundle.putParcelableArray(KEY_BUNDLE_ARRAY, bundleArray);

        // Convert to JSON
        JSONObject json = SerializationUtils.convertFromBundle(TEST_APP_NAME, parentBundle);

        // Verify nested bundle
        JSONObject nestedJson = json.getJSONObject(KEY_BUNDLE);
        assertEquals("child string", nestedJson.getString(KEY_STRING));
        assertEquals(99, nestedJson.getInt(KEY_INT));

        // Verify bundle array
        JSONArray bundleJsonArray = json.getJSONArray(KEY_BUNDLE_ARRAY);
        assertEquals(2, bundleJsonArray.length());
        assertEquals("array bundle 1", bundleJsonArray.getJSONObject(0).getString(KEY_STRING));
        assertEquals("array bundle 2", bundleJsonArray.getJSONObject(1).getString(KEY_STRING));
    }

    @Test
    public void testConvertToBundleSimpleTypes() throws JSONException {
        // Create JSON with simple types
        JSONObject json = new JSONObject();
        json.put(KEY_STRING, "test string");
        json.put(KEY_INT, 42);
        json.put(KEY_LONG, 9999999999L);
        json.put(KEY_DOUBLE, 3.14159);
        json.put(KEY_BOOLEAN, true);
        json.put(KEY_NULL, JSONObject.NULL);

        // Define a simple MacroStringExpander
        MacroStringExpander expander = new MacroStringExpander() {
            @Override
            public String expandString(String value) {
                // Simple implementation: just append a suffix to demonstrate expansion
                return value + "_expanded";
            }
        };

        // Convert to Bundle
        Bundle bundle = SerializationUtils.convertToBundle(json, expander);

        // Verify conversion was correct
        assertEquals("test string_expanded", bundle.getString(KEY_STRING));
        assertEquals(42, bundle.getInt(KEY_INT));
        assertEquals(9999999999L, bundle.getLong(KEY_LONG));
        assertEquals(3.14159, bundle.getDouble(KEY_DOUBLE), 0.0000001);
        assertTrue(bundle.getBoolean(KEY_BOOLEAN));
        assertFalse(bundle.containsKey(KEY_NULL)); // NULL values aren't stored in the bundle
    }

    @Test
    public void testConvertToBundleArrayTypes() throws JSONException {
        // Create JSON with various array types
        JSONObject json = new JSONObject();

        // String array
        JSONArray stringArray = new JSONArray();
        stringArray.put("one");
        stringArray.put(JSONObject.NULL);
        stringArray.put("three");
        json.put(KEY_STRING_ARRAY, stringArray);

        // Int array
        JSONArray intArray = new JSONArray();
        intArray.put(1);
        intArray.put(2);
        intArray.put(3);
        json.put(KEY_INT_ARRAY, intArray);

        // Long array
        JSONArray longArray = new JSONArray();
        longArray.put(100L);
        longArray.put(200L);
        longArray.put(300L);
        json.put(KEY_LONG_ARRAY, longArray);

        // Double array
        JSONArray doubleArray = new JSONArray();
        doubleArray.put(1.1);
        doubleArray.put(2.2);
        doubleArray.put(3.3);
        json.put(KEY_DOUBLE_ARRAY, doubleArray);

        // Boolean array
        JSONArray booleanArray = new JSONArray();
        booleanArray.put(true);
        booleanArray.put(false);
        booleanArray.put(true);
        json.put(KEY_BOOLEAN_ARRAY, booleanArray);

        // Convert to Bundle with null expander (no string expansion)
        Bundle bundle = SerializationUtils.convertToBundle(json, null);

        // Verify string array
        String[] bundleStringArray = bundle.getStringArray(KEY_STRING_ARRAY);
        assert bundleStringArray != null;
        assertEquals(3, bundleStringArray.length);
        assertEquals("one", bundleStringArray[0]);
        assertNull(bundleStringArray[1]);
        assertEquals("three", bundleStringArray[2]);

        // Verify int array
        int[] bundleIntArray = bundle.getIntArray(KEY_INT_ARRAY);
        assert bundleIntArray != null;
        assertEquals(3, bundleIntArray.length);
        assertEquals(1, bundleIntArray[0]);
        assertEquals(2, bundleIntArray[1]);
        assertEquals(3, bundleIntArray[2]);

        // Verify long array
        long[] bundleLongArray = bundle.getLongArray(KEY_LONG_ARRAY);
        assert bundleLongArray != null;
        assertEquals(3, bundleLongArray.length);
        assertEquals(100L, bundleLongArray[0]);
        assertEquals(200L, bundleLongArray[1]);
        assertEquals(300L, bundleLongArray[2]);

        // Verify double array
        double[] bundleDoubleArray = bundle.getDoubleArray(KEY_DOUBLE_ARRAY);
        assert bundleDoubleArray != null;
        assertEquals(3, bundleDoubleArray.length);
        assertEquals(1.1, bundleDoubleArray[0], 0.0001);
        assertEquals(2.2, bundleDoubleArray[1], 0.0001);
        assertEquals(3.3, bundleDoubleArray[2], 0.0001);

        // Verify boolean array
        boolean[] bundleBooleanArray = bundle.getBooleanArray(KEY_BOOLEAN_ARRAY);
        assert bundleBooleanArray != null;
        assertEquals(3, bundleBooleanArray.length);
        assertTrue(bundleBooleanArray[0]);
        assertFalse(bundleBooleanArray[1]);
        assertTrue(bundleBooleanArray[2]);
    }

    @Test
    public void testConvertToBundleNestedObjects() throws JSONException {
        // Create nested JSON objects
        JSONObject childJson = new JSONObject();
        childJson.put(KEY_STRING, "child string");
        childJson.put(KEY_INT, 99);

        JSONObject parentJson = new JSONObject();
        parentJson.put(KEY_BUNDLE, childJson);

        // Create JSON array of objects
        JSONArray jsonArray = new JSONArray();
        JSONObject arrayObj1 = new JSONObject();
        arrayObj1.put(KEY_STRING, "array object 1");
        JSONObject arrayObj2 = new JSONObject();
        arrayObj2.put(KEY_STRING, "array object 2");
        jsonArray.put(arrayObj1);
        jsonArray.put(arrayObj2);

        parentJson.put(KEY_BUNDLE_ARRAY, jsonArray);

        // Convert to Bundle with null expander
        Bundle bundle = SerializationUtils.convertToBundle(parentJson, null);

        // Verify nested bundle
        Bundle nestedBundle = bundle.getBundle(KEY_BUNDLE);
        assertNotNull(nestedBundle);
        assertEquals("child string", nestedBundle.getString(KEY_STRING));
        assertEquals(99, nestedBundle.getInt(KEY_INT));

        // Verify bundle array
        Parcelable[] bundleArray = bundle.getParcelableArray(KEY_BUNDLE_ARRAY);
        assert bundleArray != null;
        assertEquals(2, bundleArray.length);
        Bundle bundle1 = (Bundle) bundleArray[0];
        Bundle bundle2 = (Bundle) bundleArray[1];
        assertEquals("array object 1", bundle1.getString(KEY_STRING));
        assertEquals("array object 2", bundle2.getString(KEY_STRING));
    }

    @Test
    public void testMacroExpansionInArrays() throws JSONException {
        // Create JSON with string array that needs expansion
        JSONObject json = new JSONObject();
        JSONArray stringArray = new JSONArray();
        stringArray.put("expand_me");
        stringArray.put("expand_me_too");
        json.put(KEY_STRING_ARRAY, stringArray);

        // Define a simple MacroStringExpander
        MacroStringExpander expander = new MacroStringExpander() {
            @Override
            public String expandString(String value) {
                return value + "_expanded";
            }
        };

        // Convert to Bundle
        Bundle bundle = SerializationUtils.convertToBundle(json, expander);

        // Verify the string array values - note: the expander is NOT applied to array elements
        // based on the SerializationUtils implementation
        String[] bundleStringArray = bundle.getStringArray(KEY_STRING_ARRAY);
        assert bundleStringArray != null;
        assertEquals(2, bundleStringArray.length);
        assertEquals("expand_me", bundleStringArray[0]);  // Should NOT be expanded
        assertEquals("expand_me_too", bundleStringArray[1]);  // Should NOT be expanded
    }

    @Test
    public void testRoundTripConversion() throws JSONException {
        // Create a complex bundle with various types
        Bundle originalBundle = new Bundle();
        originalBundle.putString(KEY_STRING, "test string");
        originalBundle.putInt(KEY_INT, 42);
        originalBundle.putLong(KEY_LONG, 9999999999L);
        originalBundle.putDouble(KEY_DOUBLE, 3.14159);
        originalBundle.putBoolean(KEY_BOOLEAN, true);

        // Add arrays
        originalBundle.putStringArray(KEY_STRING_ARRAY, new String[]{"one", null, "three"});
        originalBundle.putIntArray(KEY_INT_ARRAY, new int[]{1, 2, 3});

        // Add nested bundle
        Bundle nestedBundle = new Bundle();
        nestedBundle.putString(KEY_STRING, "nested string");
        nestedBundle.putInt(KEY_INT, 99);
        originalBundle.putBundle(KEY_BUNDLE, nestedBundle);

        // Convert Bundle to JSON
        JSONObject json = SerializationUtils.convertFromBundle(TEST_APP_NAME, originalBundle);

        // Convert JSON back to Bundle with null expander (no string expansion)
        Bundle resultBundle = SerializationUtils.convertToBundle(json, null);

        // Verify round-trip conversion for simple types
        assertEquals(originalBundle.getString(KEY_STRING), resultBundle.getString(KEY_STRING));
        assertEquals(originalBundle.getInt(KEY_INT), resultBundle.getInt(KEY_INT));
        assertEquals(originalBundle.getLong(KEY_LONG), resultBundle.getLong(KEY_LONG));
        assertEquals(originalBundle.getDouble(KEY_DOUBLE), resultBundle.getDouble(KEY_DOUBLE), 0.0000001);
        assertEquals(originalBundle.getBoolean(KEY_BOOLEAN), resultBundle.getBoolean(KEY_BOOLEAN));

        // Verify string array
        String[] originalStringArray = originalBundle.getStringArray(KEY_STRING_ARRAY);
        String[] resultStringArray = resultBundle.getStringArray(KEY_STRING_ARRAY);
        assert resultStringArray != null;
        assert originalStringArray != null;
        assertEquals(originalStringArray.length, resultStringArray.length);
        for (int i = 0; i < originalStringArray.length; i++) {
            assertEquals(originalStringArray[i], resultStringArray[i]);
        }

        // Verify int array
        int[] originalIntArray = originalBundle.getIntArray(KEY_INT_ARRAY);
        int[] resultIntArray = resultBundle.getIntArray(KEY_INT_ARRAY);
        assert resultIntArray != null;
        assert originalIntArray != null;
        assertEquals(originalIntArray.length, resultIntArray.length);
        for (int i = 0; i < originalIntArray.length; i++) {
            assertEquals(originalIntArray[i], resultIntArray[i]);
        }

        // Verify nested bundle
        Bundle originalNestedBundle = originalBundle.getBundle(KEY_BUNDLE);
        Bundle resultNestedBundle = resultBundle.getBundle(KEY_BUNDLE);
        assert originalNestedBundle != null;
        assert resultNestedBundle != null;
        assertEquals(originalNestedBundle.getString(KEY_STRING), resultNestedBundle.getString(KEY_STRING));
        assertEquals(originalNestedBundle.getInt(KEY_INT), resultNestedBundle.getInt(KEY_INT));
    }
}