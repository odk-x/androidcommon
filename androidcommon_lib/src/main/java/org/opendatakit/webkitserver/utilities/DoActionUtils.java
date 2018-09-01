package org.opendatakit.webkitserver.utilities;

import android.content.ComponentName;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.opendatakit.activities.IOdkCommonActivity;
import org.opendatakit.application.IToolAware;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.DynamicPropertiesCallback;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.properties.PropertyManager;
import org.opendatakit.views.ODKWebView;

/**
 * Consolidate processing for doAction implementation and onActivityResult
 *
 * @author mitchellsundt@gmail.com
 */

public class DoActionUtils {
   private static final String t = "DoActionUtils";


   /**
    * Build the intent for a doAction request.
    * Invoked from within Javascript to launch an activity.
    *
    * @param activity         The IOdkCommonActivity from which this is invoked.
    *                         ApplicationContext should derive from IToolAware.
    *
    * @param propertyManager  The property manager for this tool.
    *
    * @param dispatchStructAsJSONstring  JSON.stringify(anything) -- typically identifies prompt and
    *                                    user action. If this is null, then the Javascript layer
    *                                    is not notified of the result of this action. It just
    *                                    transparently happens and the webkit might reload as a
    *                                    result of the activity swapping out.
    *
    * @param action    The intent to be launched. e.g.,
    *                   org.opendatakit.survey.activities.MediaCaptureImageActivity
    *
    * @param intentObject  an object with the following structure:
    *                   {
    *                         "uri" : intent.setData(value)
    *                         "data" : intent.setData(value)  (preferred over "uri")
    *                         "package" : intent.setPackage(value)
    *                         "type" : intent.setType(value)
    *                         "action" : intent.setAction(value)
    *                         "category" : either a single string or a list of strings
    *                                      for intent.addCategory(item)
    *                         "flags" : the integer code for the values to store
    *                         "extras" : { key-value map describing extras bundle }
    *                   }
    *
    *                  Within the extras, if a value is of the form:
    *                     opendatakit-macro(name)
    *                  then substitute this with the result of getProperty(name)
    *
    *                  If the action begins with "org.opendatakit." then we also
    *                  add an "appName" property into the intent extras if it was
    *                  not specified.
    *
    * @return the intent to launch or null if there is an error.
    */
   public static Intent buildIntent(final IOdkCommonActivity activity,
       final PropertyManager propertyManager,
       String dispatchStructAsJSONstring,
       String action,
       JSONObject intentObject) {

      Intent i;
      boolean isCurrentApp = false;
      String currentApp = "org.opendatakit." + ((IToolAware) (activity
          .getApplicationContext()))
          .getToolName();

      boolean isOpendatakitApp = false;
      if (action.startsWith(currentApp)) {
         Class<?> clazz;
         try {
            clazz = Class.forName(action);
            i = new Intent(activity.getApplicationContext(), clazz);
            isCurrentApp = true;
         } catch (ClassNotFoundException e) {
            WebLogger.getLogger(activity.getAppName()).printStackTrace(e);
            i = new Intent(action);
         }
      } else {
         i = new Intent(action);
      }

      if (action.startsWith("org.opendatakit.")) {
         isOpendatakitApp = true;
      }

      try {

         String uriKey = "uri";
         String extrasKey = "extras";
         String packageKey = "package";
         String typeKey = "type";
         String dataKey = "data";
         String actionKey = "action";
         String categoriesKey = "category";
         String flagsKey = "flags";
         String componentPackageKey = "componentPackage";
         String componentActivityKey = "componentActivity";

         JSONObject valueMap = null;
         if (intentObject != null) {

            // do type first, as it says in the spec this call deletes any other
            // data (eg by setData()) on the intent.
            String type = null;
            if (intentObject.has(typeKey)) {
               type = intentObject.getString(typeKey);
               i.setType(type);
            }

            if (intentObject.has(uriKey) || intentObject.has(dataKey)) {
               // as it currently stands, the data property can be in either the uri
               // or data keys.
               String uriValueStr = null;
               if (intentObject.has(uriKey)) {
                  uriValueStr = intentObject.getString(uriKey);
               }
               // go ahead and overwrite with data if it's present.
               if (intentObject.has(dataKey)) {
                  uriValueStr = intentObject.getString(dataKey);
               }
               if (uriValueStr != null) {
                  Uri uri = Uri.parse(uriValueStr);
                  if ( type != null ) {
                     i.setDataAndType(uri, type);
                  } else {
                     i.setData(uri);
                  }
               }
            }

            if (intentObject.has(extrasKey)) {
               valueMap = intentObject.getJSONObject(extrasKey);
            }

            if (intentObject.has(packageKey)) {
               String packageStr = intentObject.getString(packageKey);
               i.setPackage(packageStr);
            }

            if (intentObject.has(actionKey)) {
               String actionStr = intentObject.getString(actionKey);
               i.setAction(actionStr);
            }

            if (intentObject.has(categoriesKey)) {
               try {
                  JSONArray categoriesList = intentObject.getJSONArray(categoriesKey);
                  for (int k = 0; k < categoriesList.length(); ++k) {
                     String categoryStr = categoriesList.getString(k);
                     i.addCategory(categoryStr);
                  }
               } catch ( Exception e ) {
                  String category = intentObject.getString(categoriesKey);
                  i.addCategory(category);
               }
            }

            if (intentObject.has(flagsKey)) {
               int flags = intentObject.getInt(flagsKey);
               i.addFlags(flags);
            }
            if (intentObject.has(componentPackageKey) && intentObject.has(componentActivityKey)) {
               String componentPackage = intentObject.getString(componentPackageKey);
               String componentActivity = intentObject.getString(componentActivityKey);
               i.setComponent(new ComponentName(componentPackage, componentActivity));
            }
         }

         if (valueMap != null) {
            Bundle b;
            PropertiesSingleton props = CommonToolProperties.get(activity.getApplicationContext(), activity.getAppName());

            final DynamicPropertiesCallback cb = new DynamicPropertiesCallback(activity.getAppName(),
                activity.getTableId(), activity.getInstanceId(),
                activity.getActiveUser(), props.getUserSelectedDefaultLocale());

            b = SerializationUtils.convertToBundle(valueMap, new SerializationUtils.MacroStringExpander() {

               @Override
               public String expandString(String value) {
                  if (value != null && value.startsWith("opendatakit-macro(") && value.endsWith(")")) {
                     String term = value.substring("opendatakit-macro(".length(), value.length() - 1)
                         .trim();
                     String v = propertyManager.getSingularProperty(term, cb);
                     if (v != null) {
                        return v;
                     } else {
                        WebLogger.getLogger(activity.getAppName()).e(t, "Unable to process opendatakit-macro: " + value);
                        throw new IllegalArgumentException(
                            "Unable to process opendatakit-macro expression: " + value);
                     }
                  } else {
                     return value;
                  }
               }
            });

            i.putExtras(b);
         }

         if (isOpendatakitApp) {
            // ensure that we supply our appName...
            if (!i.hasExtra(IntentConsts.INTENT_KEY_APP_NAME)) {
               i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, activity.getAppName());
               WebLogger.getLogger(activity.getAppName()).w(t, "doAction into Survey or Tables does not supply an appName. Adding: "
                   + activity.getAppName());
            }
         }
         return i;
      } catch (Exception ex) {
         WebLogger.getLogger(activity.getAppName()).e(t, "JSONException: " + ex.toString());
         WebLogger.getLogger(activity.getAppName()).printStackTrace(ex);
         return null;
      }
   }

   /**
    * Caller should wrap this in a try...finally block with the finally clearing the
    * values for the dispatchStringWaitingForData and actionWaitingForData.
    *
    * @param activity
    * @param view
    * @param resultCode
    * @param intent
    * @param dispatchStructJSONstring
    * @param actionWaitingForData
    */
   public static void processActivityResult(IOdkCommonActivity activity, ODKWebView view,
       int resultCode,
       Intent intent, String dispatchStructJSONstring, String actionWaitingForData) {
      WebLogger.getLogger(activity.getAppName()).i(t, "processActivityResult");

      try {
         if ( dispatchStructJSONstring == null || dispatchStructJSONstring.length()
             == 0 ) {
            // This is a special case -- if no dispatchStruct was supplied (
            // dispatchStructJSONstring is null), then we don't notify the WebKit
            // of the response from the request. Otherwise, we do.
            return;
         }
            Bundle b = (intent == null) ? null : intent.getExtras();
         JSONObject val = (b == null) ? null : SerializationUtils.convertFromBundle(activity.getAppName(), b);
         JSONObject jsonValue = new JSONObject();
         jsonValue.put("status", resultCode);
         if ( val != null ) {
            jsonValue.put("result", val);
         }
         JSONObject result = new JSONObject();
         // de-tokenize the dispatchStruct and store it in the result
         // this means there is less parsing in JS
         try {
            JSONTokener tokener = new JSONTokener(dispatchStructJSONstring);
            result.put("dispatchStruct", tokener.nextValue());
         } catch ( JSONException e ) {
            WebLogger.getLogger(activity.getAppName()).printStackTrace(e);
            result.put("dispatchStruct", JSONObject.NULL);
         }
         result.put("action",  actionWaitingForData);
         result.put("jsonValue", jsonValue);

         String actionOutcome = result.toString();
         activity.queueActionOutcome(actionOutcome);

         if (view != null) {
            view.signalQueuedActionAvailable();
         }
      } catch (Exception e) {
         WebLogger.getLogger(activity.getAppName()).printStackTrace(e);
         try {
            JSONObject jsonValue = new JSONObject();
            jsonValue.put("status", 0);
            jsonValue.put("result", e.toString());
            JSONObject result = new JSONObject();
            // de-tokenize the dispatchStruct and store it in the result.
            // this means there is less parsing in JS
            try {
               JSONTokener tokener = new JSONTokener(dispatchStructJSONstring);
               result.put("dispatchStruct", tokener.nextValue());
            } catch ( JSONException ex ) {
               result.put("dispatchStruct", JSONObject.NULL);
            }
            result.put("action",  actionWaitingForData);
            result.put("jsonValue", jsonValue);
            activity.queueActionOutcome(result.toString());

            if (view != null) {
               view.signalQueuedActionAvailable();
            }
         } catch (Exception ex) {
            WebLogger.getLogger(activity.getAppName()).printStackTrace(ex);
         }
      }
   }

}
