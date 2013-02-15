/*
 * Copyright (C) 2009-2013 University of Washington
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

package org.opendatakit.common.android.logic;

import java.util.HashMap;
import java.util.Locale;

import android.content.Context;
import android.net.wifi.WifiInfo;
import android.net.wifi.WifiManager;
import android.provider.Settings;
import android.telephony.TelephonyManager;
import android.util.Log;

/**
 * Used to return device properties to JavaRosa
 *
 * @author Yaw Anokwa (yanokwa@gmail.com)
 * @author mitchellsundt@gmail.com
 */

public class PropertyManagerImpl {

	public interface DynamicPropertiesInterface {
		String getUsername(Context c);
		String getUserEmail(Context c);
	};

	private String t = "PropertyManager";

	private Context mContext;
	private DynamicPropertiesInterface mCallback;

	private HashMap<String, String> mProperties;

	public final static String DEVICE_ID_PROPERTY = "deviceid"; // imei
	private final static String SUBSCRIBER_ID_PROPERTY = "subscriberid"; // imsi
	private final static String SIM_SERIAL_PROPERTY = "simserial";
	private final static String PHONE_NUMBER_PROPERTY = "phonenumber";
	private final static String USERNAME = "username";
	private final static String EMAIL = "email";

	public final static String OR_DEVICE_ID_PROPERTY = "uri:deviceid"; // imei
	public final static String OR_SUBSCRIBER_ID_PROPERTY = "uri:subscriberid"; // imsi
	public final static String OR_SIM_SERIAL_PROPERTY = "uri:simserial";
	public final static String OR_PHONE_NUMBER_PROPERTY = "uri:phonenumber";
	public final static String OR_USERNAME = "uri:username";
	public final static String OR_EMAIL = "uri:email";

	/**
	 * Constructor used within the Application object to create a singleton of
	 * the property manager. Access it through
	 * Survey.getInstance().getPropertyManager()
	 *
	 * @param context
	 */
	public PropertyManagerImpl(Context context, DynamicPropertiesInterface callback) {
		Log.i(t, "calling constructor");

		mContext = context;
		mCallback = callback;

		mProperties = new HashMap<String, String>();
		TelephonyManager mTelephonyManager = (TelephonyManager) mContext
				.getSystemService(Context.TELEPHONY_SERVICE);

		String deviceId = mTelephonyManager.getDeviceId();
		String orDeviceId = null;
		if (deviceId != null) {
			if ((deviceId.contains("*") || deviceId.contains("000000000000000"))) {
				deviceId = Settings.Secure.getString(
						mContext.getContentResolver(),
						Settings.Secure.ANDROID_ID);
				orDeviceId = Settings.Secure.ANDROID_ID + ":" + deviceId;
			} else {
				orDeviceId = "imei:" + deviceId;
			}
		}

		if (deviceId == null) {
			// no SIM -- WiFi only
			// Retrieve WiFiManager
			WifiManager wifi = (WifiManager) mContext
					.getSystemService(Context.WIFI_SERVICE);

			// Get WiFi status
			WifiInfo info = wifi.getConnectionInfo();
			if (info != null) {
				deviceId = info.getMacAddress();
				orDeviceId = "mac:" + deviceId;
			}
		}

		// if it is still null, use ANDROID_ID
		if (deviceId == null) {
			deviceId = Settings.Secure.getString(mContext.getContentResolver(),
					Settings.Secure.ANDROID_ID);
			orDeviceId = Settings.Secure.ANDROID_ID + ":" + deviceId;
		}

		mProperties.put(DEVICE_ID_PROPERTY, deviceId);
		mProperties.put(OR_DEVICE_ID_PROPERTY, orDeviceId);

		String value;

		value = mTelephonyManager.getSubscriberId();
		if (value != null) {
			mProperties.put(SUBSCRIBER_ID_PROPERTY, value);
			mProperties.put(OR_SUBSCRIBER_ID_PROPERTY, "imsi:" + value);
		}
		value = mTelephonyManager.getSimSerialNumber();
		if (value != null) {
			mProperties.put(SIM_SERIAL_PROPERTY, value);
			mProperties.put(OR_SIM_SERIAL_PROPERTY, "simserial:" + value);
		}
		value = mTelephonyManager.getLine1Number();
		if (value != null) {
			mProperties.put(PHONE_NUMBER_PROPERTY, value);
			mProperties.put(OR_PHONE_NUMBER_PROPERTY, "tel:" + value);
		}
	}

	public String getSingularProperty(String rawPropertyName) {

		String propertyName = rawPropertyName.toLowerCase(Locale.ENGLISH);

		// retrieve the dynamic values via the callback...
		if ( USERNAME.equals(propertyName) ) {
			return mCallback.getUsername(mContext);
		} else if (	OR_USERNAME.equals(propertyName) ) {
			String value = mCallback.getUsername(mContext);
			if ( value == null ) return null;
			return "username:" + value;
		} else if ( EMAIL.equals(propertyName) ) {
			return mCallback.getUserEmail(mContext);
		} else if ( OR_EMAIL.equals(propertyName) ) {
			String value = mCallback.getUserEmail(mContext);
			if ( value == null ) return null;
			return "mailto:" + value;
		} else {
			return mProperties.get(propertyName);
		}
	}
}
