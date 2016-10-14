/*
 * Copyright (C) 2013 University of Washington
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

package org.opendatakit.webkitserver.utilities;

import java.util.Iterator;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import android.os.Bundle;
import org.opendatakit.logging.WebLogger;

/**
 * Utilities for converting a Bundle to and from a JSON serialization for
 * transmission to javascript inside a WebKit.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class SerializationUtils {
  private static final String tag = "SerializationUtils";

  public static interface MacroStringExpander {
    public String expandString(String value);
  }

  // no constructor
  private SerializationUtils() {
  };

  public static JSONObject convertFromBundle(String appName, Bundle b) throws JSONException {
    JSONObject jo = new JSONObject();
    Set<String> keys = b.keySet();
    for (String key : keys) {
      Object o = b.get(key);
      if (o == null) {
        jo.put(key, JSONObject.NULL);
      } else if (o.getClass().isArray()) {
        JSONArray ja = new JSONArray();
        Class<?> t = o.getClass().getComponentType();
        if (t.equals(long.class)) {
          long[] a = (long[]) o;
          for (int i = 0; i < a.length; ++i) {
            ja.put(a[i]);
          }
          jo.put(key, ja);
        } else if (t.equals(int.class)) {
          int[] a = (int[]) o;
          for (int i = 0; i < a.length; ++i) {
            ja.put(a[i]);
          }
          jo.put(key, ja);
        } else if (t.equals(double.class)) {
          double[] a = (double[]) o;
          for (int i = 0; i < a.length; ++i) {
            ja.put(a[i]);
          }
          jo.put(key, ja);
        } else if (t.equals(boolean.class)) {
          boolean[] a = (boolean[]) o;
          for (int i = 0; i < a.length; ++i) {
            ja.put(a[i]);
          }
          jo.put(key, ja);
        } else if (t.equals(Long.class)) {
          Long[] a = (Long[]) o;
          for (int i = 0; i < a.length; ++i) {
            ja.put((a[i] == null) ? JSONObject.NULL : a[i]);
          }
        } else if (t.equals(Integer.class)) {
          Integer[] a = (Integer[]) o;
          for (int i = 0; i < a.length; ++i) {
            ja.put((a[i] == null) ? JSONObject.NULL : a[i]);
          }
          jo.put(key, ja);
        } else if (t.equals(Double.class)) {
          Double[] a = (Double[]) o;
          for (int i = 0; i < a.length; ++i) {
            ja.put((a[i] == null) ? JSONObject.NULL : a[i]);
          }
          jo.put(key, ja);
        } else if (t.equals(Boolean.class)) {
          Boolean[] a = (Boolean[]) o;
          for (int i = 0; i < a.length; ++i) {
            ja.put((a[i] == null) ? JSONObject.NULL : a[i]);
          }
          jo.put(key, ja);
        } else if (t.equals(String.class)) {
            String[] a = (String[]) o;
            for (int i = 0; i < a.length; ++i) {
              ja.put((a[i] == null) ? JSONObject.NULL : a[i]);
            }
            jo.put(key, ja);
        } else if (t.equals(Bundle.class) || Bundle.class.isAssignableFrom(t)) {
          Bundle[] a = (Bundle[]) o;
          for (int i = 0; i < a.length; ++i) {
            ja.put((a[i] == null) ? JSONObject.NULL : convertFromBundle(appName, a[i]));
          }
          jo.put(key, ja);
        } else if (t.equals(byte.class)) {
       	  WebLogger.getLogger(appName).w(tag, "byte array returned -- ignoring");
        } else {
          throw new JSONException("unrecognized class");
        }
      } else if (o instanceof Bundle) {
        jo.put(key, convertFromBundle(appName, (Bundle) o));
      } else if (o instanceof String) {
        jo.put(key, b.getString(key));
      } else if (o instanceof Boolean) {
        jo.put(key, b.getBoolean(key));
      } else if (o instanceof Integer) {
        jo.put(key, b.getInt(key));
      } else if (o instanceof Long) {
        jo.put(key, b.getLong(key));
      } else if (o instanceof Double) {
        jo.put(key, b.getDouble(key));
      }
    }
    return jo;
  }

  public static Bundle convertToBundle(JSONObject valueMap, final MacroStringExpander expander)
      throws JSONException {
    Bundle b = new Bundle();
    @SuppressWarnings("unchecked")
    Iterator<String> cur = valueMap.keys();
    while (cur.hasNext()) {
      String key = cur.next();
      if (!valueMap.isNull(key)) {
        Object o = valueMap.get(key);
        if (o instanceof JSONObject) {
          Bundle be = convertToBundle((JSONObject) o, expander);
          b.putBundle(key, be);
        } else if (o instanceof JSONArray) {
          JSONArray a = (JSONArray) o;
          // only non-empty arrays are written into the Bundle
          // first non-null element defines data type
          // for the array
          Object oe = null;
          for (int j = 0; j < a.length(); ++j) {
            if (!a.isNull(j)) {
              oe = a.get(j);
              break;
            }
          }
          if (oe != null) {
            if (oe instanceof JSONObject) {
              Bundle[] va = new Bundle[a.length()];
              for (int j = 0; j < a.length(); ++j) {
                if (a.isNull(j)) {
                  va[j] = null;
                } else {
                  va[j] = convertToBundle((JSONObject) a.getJSONObject(j), expander);
                }
              }
              b.putParcelableArray(key, va);
            } else if (oe instanceof JSONArray) {
              throw new JSONException("Unable to convert nested arrays");
            } else if (oe instanceof String) {
              String[] va = new String[a.length()];
              for (int j = 0; j < a.length(); ++j) {
                if (a.isNull(j)) {
                  va[j] = null;
                } else {
                  va[j] = a.getString(j);
                }
              }
              b.putStringArray(key, va);
            } else if (oe instanceof Boolean) {
              boolean[] va = new boolean[a.length()];
              for (int j = 0; j < a.length(); ++j) {
                if (a.isNull(j)) {
                  va[j] = false;
                } else {
                  va[j] = a.getBoolean(j);
                }
              }
              b.putBooleanArray(key, va);
            } else if (oe instanceof Integer) {
              int[] va = new int[a.length()];
              for (int j = 0; j < a.length(); ++j) {
                if (a.isNull(j)) {
                  va[j] = 0;
                } else {
                  va[j] = a.getInt(j);
                }
              }
              b.putIntArray(key, va);
            } else if (oe instanceof Long) {
              long[] va = new long[a.length()];
              for (int j = 0; j < a.length(); ++j) {
                if (a.isNull(j)) {
                  va[j] = 0;
                } else {
                  va[j] = a.getLong(j);
                }
              }
              b.putLongArray(key, va);
            } else if (oe instanceof Double) {
              double[] va = new double[a.length()];
              for (int j = 0; j < a.length(); ++j) {
                if (a.isNull(j)) {
                  va[j] = Double.NaN;
                } else {
                  va[j] = a.getDouble(j);
                }
              }
              b.putDoubleArray(key, va);
            }
          }
        } else if (o instanceof String) {
          String v = valueMap.getString(key);
          if (expander != null) {
            v = expander.expandString(v);
          }
          b.putString(key, v);
        } else if (o instanceof Boolean) {
          b.putBoolean(key, valueMap.getBoolean(key));
        } else if (o instanceof Integer) {
          b.putInt(key, valueMap.getInt(key));
        } else if (o instanceof Long) {
          b.putLong(key, valueMap.getLong(key));
        } else if (o instanceof Double) {
          b.putDouble(key, valueMap.getDouble(key));
        }
      }
    }
    return b;
  }

}
