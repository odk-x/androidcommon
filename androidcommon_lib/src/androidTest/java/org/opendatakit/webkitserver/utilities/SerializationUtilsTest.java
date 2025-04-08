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

    // Test values for simple types
    private static final String TEST_STRING_VALUE = "test string";
    private static final int TEST_INT_VALUE = 42;
    private static final long TEST_LONG_VALUE = 9999999999L;
    private static final double TEST_DOUBLE_VALUE = 3.14159;
    private static final double TEST_DOUBLE_DELTA = 0.0000001;
    private static final boolean TEST_BOOLEAN_VALUE = true;

    // Test values for nested bundle
    private static final String TEST_CHILD_STRING_VALUE = "child string";
    private static final int TEST_CHILD_INT_VALUE = 99;

    // Test values for array types
    private static final String[] TEST_STRING_ARRAY = {"one", null, "three"};
    private static final int[] TEST_INT_ARRAY = {1, 2, 3};
    private static final long[] TEST_LONG_ARRAY = {100L, 200L, 300L};
    private static final double[] TEST_DOUBLE_ARRAY = {1.1, 2.2, 3.3};
    private static final boolean[] TEST_BOOLEAN_ARRAY = {true, false, true};

    // Test values for array bundles
    private static final String TEST_ARRAY_BUNDLE_1_STRING = "array bundle 1";
    private static final String TEST_ARRAY_BUNDLE_2_STRING = "array bundle 2";

    // Test values for array JSON objects
    private static final String TEST_ARRAY_OBJECT_1_STRING = "array object 1";
    private static final String TEST_ARRAY_OBJECT_2_STRING = "array object 2";

    // Test values for string expansion
    private static final String TEST_EXPANSION_SUFFIX = "_expanded";
    private static final String TEST_EXPAND_STRING_1 = "expand_me";
    private static final String TEST_EXPAND_STRING_2 = "expand_me_too";

    @Test
    public void testConvertFromBundleSimpleTypes() throws JSONException {
        // Create a bundle with various simple types
        Bundle bundle = new Bundle();
        bundle.putString(KEY_STRING, TEST_STRING_VALUE);
        bundle.putInt(KEY_INT, TEST_INT_VALUE);
        bundle.putLong(KEY_LONG, TEST_LONG_VALUE);
        bundle.putDouble(KEY_DOUBLE, TEST_DOUBLE_VALUE);
        bundle.putBoolean(KEY_BOOLEAN, TEST_BOOLEAN_VALUE);
        bundle.putString(KEY_NULL, null);

        // Convert to JSON
        JSONObject json = SerializationUtils.convertFromBundle(TEST_APP_NAME, bundle);

        // Verify conversion was correct
        assertEquals(TEST_STRING_VALUE, json.getString(KEY_STRING));
        assertEquals(TEST_INT_VALUE, json.getInt(KEY_INT));
        assertEquals(TEST_LONG_VALUE, json.getLong(KEY_LONG));
        assertEquals(TEST_DOUBLE_VALUE, json.getDouble(KEY_DOUBLE), TEST_DOUBLE_DELTA);
        assertTrue(json.getBoolean(KEY_BOOLEAN));
        assertTrue(json.has(KEY_NULL));
        assertTrue(json.isNull(KEY_NULL));
    }

    @Test
    public void testConvertFromBundleArrayTypes() throws JSONException {
        // Create a bundle with various array types
        Bundle bundle = new Bundle();
        bundle.putStringArray(KEY_STRING_ARRAY, TEST_STRING_ARRAY);
        bundle.putIntArray(KEY_INT_ARRAY, TEST_INT_ARRAY);
        bundle.putLongArray(KEY_LONG_ARRAY, TEST_LONG_ARRAY);
        bundle.putDoubleArray(KEY_DOUBLE_ARRAY, TEST_DOUBLE_ARRAY);
        bundle.putBooleanArray(KEY_BOOLEAN_ARRAY, TEST_BOOLEAN_ARRAY);

        // Convert to JSON
        JSONObject json = SerializationUtils.convertFromBundle(TEST_APP_NAME, bundle);

        // Verify string array
        JSONArray stringArray = json.getJSONArray(KEY_STRING_ARRAY);
        assertEquals(TEST_STRING_ARRAY.length, stringArray.length());
        assertEquals(TEST_STRING_ARRAY[0], stringArray.getString(0));
        assertTrue(stringArray.isNull(1));
        assertEquals(TEST_STRING_ARRAY[2], stringArray.getString(2));

        // Verify int array
        JSONArray intArray = json.getJSONArray(KEY_INT_ARRAY);
        assertEquals(TEST_INT_ARRAY.length, intArray.length());
        assertEquals(TEST_INT_ARRAY[0], intArray.getInt(0));
        assertEquals(TEST_INT_ARRAY[1], intArray.getInt(1));
        assertEquals(TEST_INT_ARRAY[2], intArray.getInt(2));

        // Verify long array
        JSONArray longArray = json.getJSONArray(KEY_LONG_ARRAY);
        assertEquals(TEST_LONG_ARRAY.length, longArray.length());
        assertEquals(TEST_LONG_ARRAY[0], longArray.getLong(0));
        assertEquals(TEST_LONG_ARRAY[1], longArray.getLong(1));
        assertEquals(TEST_LONG_ARRAY[2], longArray.getLong(2));

        // Verify double array
        JSONArray doubleArray = json.getJSONArray(KEY_DOUBLE_ARRAY);
        assertEquals(TEST_DOUBLE_ARRAY.length, doubleArray.length());
        assertEquals(TEST_DOUBLE_ARRAY[0], doubleArray.getDouble(0), TEST_DOUBLE_DELTA);
        assertEquals(TEST_DOUBLE_ARRAY[1], doubleArray.getDouble(1), TEST_DOUBLE_DELTA);
        assertEquals(TEST_DOUBLE_ARRAY[2], doubleArray.getDouble(2), TEST_DOUBLE_DELTA);

        // Verify boolean array
        JSONArray booleanArray = json.getJSONArray(KEY_BOOLEAN_ARRAY);
        assertEquals(TEST_BOOLEAN_ARRAY.length, booleanArray.length());
        assertEquals(TEST_BOOLEAN_ARRAY[0], booleanArray.getBoolean(0));
        assertEquals(TEST_BOOLEAN_ARRAY[1], booleanArray.getBoolean(1));
        assertEquals(TEST_BOOLEAN_ARRAY[2], booleanArray.getBoolean(2));
    }

    @Test
    public void testConvertFromBundleNestedBundles() throws JSONException {
        // Create nested bundles
        Bundle childBundle = new Bundle();
        childBundle.putString(KEY_STRING, TEST_CHILD_STRING_VALUE);
        childBundle.putInt(KEY_INT, TEST_CHILD_INT_VALUE);

        Bundle parentBundle = new Bundle();
        parentBundle.putBundle(KEY_BUNDLE, childBundle);

        // Create bundle array
        Bundle[] bundleArray = new Bundle[2];
        bundleArray[0] = new Bundle();
        bundleArray[0].putString(KEY_STRING, TEST_ARRAY_BUNDLE_1_STRING);
        bundleArray[1] = new Bundle();
        bundleArray[1].putString(KEY_STRING, TEST_ARRAY_BUNDLE_2_STRING);

        parentBundle.putParcelableArray(KEY_BUNDLE_ARRAY, bundleArray);

        // Convert to JSON
        JSONObject json = SerializationUtils.convertFromBundle(TEST_APP_NAME, parentBundle);

        // Verify nested bundle
        JSONObject nestedJson = json.getJSONObject(KEY_BUNDLE);
        assertEquals(TEST_CHILD_STRING_VALUE, nestedJson.getString(KEY_STRING));
        assertEquals(TEST_CHILD_INT_VALUE, nestedJson.getInt(KEY_INT));

        // Verify bundle array
        JSONArray bundleJsonArray = json.getJSONArray(KEY_BUNDLE_ARRAY);
        assertEquals(2, bundleJsonArray.length());
        assertEquals(TEST_ARRAY_BUNDLE_1_STRING, bundleJsonArray.getJSONObject(0).getString(KEY_STRING));
        assertEquals(TEST_ARRAY_BUNDLE_2_STRING, bundleJsonArray.getJSONObject(1).getString(KEY_STRING));
    }

    @Test
    public void testConvertToBundleSimpleTypes() throws JSONException {
        // Create JSON with simple types
        JSONObject json = new JSONObject();
        json.put(KEY_STRING, TEST_STRING_VALUE);
        json.put(KEY_INT, TEST_INT_VALUE);
        json.put(KEY_LONG, TEST_LONG_VALUE);
        json.put(KEY_DOUBLE, TEST_DOUBLE_VALUE);
        json.put(KEY_BOOLEAN, TEST_BOOLEAN_VALUE);
        json.put(KEY_NULL, JSONObject.NULL);

        // Define a simple MacroStringExpander
        MacroStringExpander expander = new MacroStringExpander() {
            @Override
            public String expandString(String value) {
                // Simple implementation: just append a suffix to demonstrate expansion
                return value + TEST_EXPANSION_SUFFIX;
            }
        };

        // Convert to Bundle
        Bundle bundle = SerializationUtils.convertToBundle(json, expander);

        // Verify conversion was correct
        assertEquals(TEST_STRING_VALUE + TEST_EXPANSION_SUFFIX, bundle.getString(KEY_STRING));
        assertEquals(TEST_INT_VALUE, bundle.getInt(KEY_INT));
        assertEquals(TEST_LONG_VALUE, bundle.getLong(KEY_LONG));
        assertEquals(TEST_DOUBLE_VALUE, bundle.getDouble(KEY_DOUBLE), TEST_DOUBLE_DELTA);
        assertTrue(bundle.getBoolean(KEY_BOOLEAN));
        assertFalse(bundle.containsKey(KEY_NULL)); // NULL values aren't stored in the bundle
    }

    @Test
    public void testConvertToBundleArrayTypes() throws JSONException {
        // Create JSON with various array types
        JSONObject json = new JSONObject();

        // String array
        JSONArray stringArray = new JSONArray();
        stringArray.put(TEST_STRING_ARRAY[0]);
        stringArray.put(JSONObject.NULL);
        stringArray.put(TEST_STRING_ARRAY[2]);
        json.put(KEY_STRING_ARRAY, stringArray);

        // Int array
        JSONArray intArray = new JSONArray();
        for (int value : TEST_INT_ARRAY) {
            intArray.put(value);
        }
        json.put(KEY_INT_ARRAY, intArray);

        // Long array
        JSONArray longArray = new JSONArray();
        for (long value : TEST_LONG_ARRAY) {
            longArray.put(value);
        }
        json.put(KEY_LONG_ARRAY, longArray);

        // Double array
        JSONArray doubleArray = new JSONArray();
        for (double value : TEST_DOUBLE_ARRAY) {
            doubleArray.put(value);
        }
        json.put(KEY_DOUBLE_ARRAY, doubleArray);

        // Boolean array
        JSONArray booleanArray = new JSONArray();
        for (boolean value : TEST_BOOLEAN_ARRAY) {
            booleanArray.put(value);
        }
        json.put(KEY_BOOLEAN_ARRAY, booleanArray);

        // Convert to Bundle with null expander (no string expansion)
        Bundle bundle = SerializationUtils.convertToBundle(json, null);

        // Verify string array
        String[] bundleStringArray = bundle.getStringArray(KEY_STRING_ARRAY);
        assert bundleStringArray != null;
        assertEquals(TEST_STRING_ARRAY.length, bundleStringArray.length);
        assertEquals(TEST_STRING_ARRAY[0], bundleStringArray[0]);
        assertNull(bundleStringArray[1]);
        assertEquals(TEST_STRING_ARRAY[2], bundleStringArray[2]);

        // Verify int array
        int[] bundleIntArray = bundle.getIntArray(KEY_INT_ARRAY);
        assert bundleIntArray != null;
        assertEquals(TEST_INT_ARRAY.length, bundleIntArray.length);
        for (int i = 0; i < TEST_INT_ARRAY.length; i++) {
            assertEquals(TEST_INT_ARRAY[i], bundleIntArray[i]);
        }

        // Verify long array
        long[] bundleLongArray = bundle.getLongArray(KEY_LONG_ARRAY);
        assert bundleLongArray != null;
        assertEquals(TEST_LONG_ARRAY.length, bundleLongArray.length);
        for (int i = 0; i < TEST_LONG_ARRAY.length; i++) {
            assertEquals(TEST_LONG_ARRAY[i], bundleLongArray[i]);
        }

        // Verify double array
        double[] bundleDoubleArray = bundle.getDoubleArray(KEY_DOUBLE_ARRAY);
        assert bundleDoubleArray != null;
        assertEquals(TEST_DOUBLE_ARRAY.length, bundleDoubleArray.length);
        for (int i = 0; i < TEST_DOUBLE_ARRAY.length; i++) {
            assertEquals(TEST_DOUBLE_ARRAY[i], bundleDoubleArray[i], TEST_DOUBLE_DELTA);
        }

        // Verify boolean array
        boolean[] bundleBooleanArray = bundle.getBooleanArray(KEY_BOOLEAN_ARRAY);
        assert bundleBooleanArray != null;
        assertEquals(TEST_BOOLEAN_ARRAY.length, bundleBooleanArray.length);
        for (int i = 0; i < TEST_BOOLEAN_ARRAY.length; i++) {
            assertEquals(TEST_BOOLEAN_ARRAY[i], bundleBooleanArray[i]);
        }
    }

    @Test
    public void testConvertToBundleNestedObjects() throws JSONException {
        // Create nested JSON objects
        JSONObject childJson = new JSONObject();
        childJson.put(KEY_STRING, TEST_CHILD_STRING_VALUE);
        childJson.put(KEY_INT, TEST_CHILD_INT_VALUE);

        JSONObject parentJson = new JSONObject();
        parentJson.put(KEY_BUNDLE, childJson);

        // Create JSON array of objects
        JSONArray jsonArray = new JSONArray();
        JSONObject arrayObj1 = new JSONObject();
        arrayObj1.put(KEY_STRING, TEST_ARRAY_OBJECT_1_STRING);
        JSONObject arrayObj2 = new JSONObject();
        arrayObj2.put(KEY_STRING, TEST_ARRAY_OBJECT_2_STRING);
        jsonArray.put(arrayObj1);
        jsonArray.put(arrayObj2);

        parentJson.put(KEY_BUNDLE_ARRAY, jsonArray);

        // Convert to Bundle with null expander
        Bundle bundle = SerializationUtils.convertToBundle(parentJson, null);

        // Verify nested bundle
        Bundle nestedBundle = bundle.getBundle(KEY_BUNDLE);
        assertNotNull(nestedBundle);
        assertEquals(TEST_CHILD_STRING_VALUE, nestedBundle.getString(KEY_STRING));
        assertEquals(TEST_CHILD_INT_VALUE, nestedBundle.getInt(KEY_INT));

        // Verify bundle array
        Parcelable[] bundleArray = bundle.getParcelableArray(KEY_BUNDLE_ARRAY);
        assert bundleArray != null;
        assertEquals(2, bundleArray.length);
        Bundle bundle1 = (Bundle) bundleArray[0];
        Bundle bundle2 = (Bundle) bundleArray[1];
        assertEquals(TEST_ARRAY_OBJECT_1_STRING, bundle1.getString(KEY_STRING));
        assertEquals(TEST_ARRAY_OBJECT_2_STRING, bundle2.getString(KEY_STRING));
    }

    @Test
    public void testMacroExpansionInArrays() throws JSONException {
        // Create JSON with string array that needs expansion
        JSONObject json = new JSONObject();
        JSONArray stringArray = new JSONArray();
        stringArray.put(TEST_EXPAND_STRING_1);
        stringArray.put(TEST_EXPAND_STRING_2);
        json.put(KEY_STRING_ARRAY, stringArray);

        // Define a simple MacroStringExpander
        MacroStringExpander expander = new MacroStringExpander() {
            @Override
            public String expandString(String value) {
                return value + TEST_EXPANSION_SUFFIX;
            }
        };

        // Convert to Bundle
        Bundle bundle = SerializationUtils.convertToBundle(json, expander);

        // Verify the string array values - note: the expander is NOT applied to array elements
        // based on the SerializationUtils implementation
        String[] bundleStringArray = bundle.getStringArray(KEY_STRING_ARRAY);
        assert bundleStringArray != null;
        assertEquals(2, bundleStringArray.length);
        assertEquals(TEST_EXPAND_STRING_1, bundleStringArray[0]);  // Should NOT be expanded
        assertEquals(TEST_EXPAND_STRING_2, bundleStringArray[1]);  // Should NOT be expanded
    }

    @Test
    public void testRoundTripConversion() throws JSONException {
        // Create a complex bundle with various types
        Bundle originalBundle = new Bundle();
        originalBundle.putString(KEY_STRING, TEST_STRING_VALUE);
        originalBundle.putInt(KEY_INT, TEST_INT_VALUE);
        originalBundle.putLong(KEY_LONG, TEST_LONG_VALUE);
        originalBundle.putDouble(KEY_DOUBLE, TEST_DOUBLE_VALUE);
        originalBundle.putBoolean(KEY_BOOLEAN, TEST_BOOLEAN_VALUE);

        // Add arrays
        originalBundle.putStringArray(KEY_STRING_ARRAY, TEST_STRING_ARRAY);
        originalBundle.putIntArray(KEY_INT_ARRAY, TEST_INT_ARRAY);

        // Add nested bundle
        Bundle nestedBundle = new Bundle();
        nestedBundle.putString(KEY_STRING, TEST_CHILD_STRING_VALUE);
        nestedBundle.putInt(KEY_INT, TEST_CHILD_INT_VALUE);
        originalBundle.putBundle(KEY_BUNDLE, nestedBundle);

        // Convert Bundle to JSON
        JSONObject json = SerializationUtils.convertFromBundle(TEST_APP_NAME, originalBundle);

        // Convert JSON back to Bundle with null expander (no string expansion)
        Bundle resultBundle = SerializationUtils.convertToBundle(json, null);

        // Verify round-trip conversion for simple types
        assertEquals(originalBundle.getString(KEY_STRING), resultBundle.getString(KEY_STRING));
        assertEquals(originalBundle.getInt(KEY_INT), resultBundle.getInt(KEY_INT));
        assertEquals(originalBundle.getLong(KEY_LONG), resultBundle.getLong(KEY_LONG));
        assertEquals(originalBundle.getDouble(KEY_DOUBLE), resultBundle.getDouble(KEY_DOUBLE), TEST_DOUBLE_DELTA);
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
