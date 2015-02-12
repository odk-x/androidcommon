/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.webkitserver;

import java.util.concurrent.atomic.AtomicBoolean;

import org.opendatakit.webkitserver.service.OdkWebkitServerInterface;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;

public class OdkWebkitServerProxy implements ServiceConnection {

	private final static String LOGTAG = OdkWebkitServerProxy.class.getSimpleName();

	private OdkWebkitServerInterface webkitfilesService;

	protected Context componentContext;
	protected final AtomicBoolean isBoundToService = new AtomicBoolean(false);

	public OdkWebkitServerProxy(Context context) {
		componentContext = context;
		Intent bind_intent = new Intent();
		bind_intent.setClassName(WebkitServerConsts.WEBKITSERVER_SERVICE_PACKAGE, WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS);
		componentContext.bindService(bind_intent, this,
				Context.BIND_AUTO_CREATE);
	}

	public void shutdown() {
		Log.d(LOGTAG, "Application shutdown - unbinding from WebkitFile Server");
		if (isBoundToService.get()) {
			try {
				componentContext.unbindService(this);
				isBoundToService.set(false);
				Log.d(LOGTAG, "unbound to service");
			} catch (Exception ex) {
				Log.e(LOGTAG, "service shutdown threw exception");
				ex.printStackTrace();
			}
		}
	}

	@Override
	public void onServiceConnected(ComponentName className, IBinder service) {
		Log.d(LOGTAG, "Bound to service");
		webkitfilesService = OdkWebkitServerInterface.Stub.asInterface(service);
		isBoundToService.set(true);
	}

	@Override
	public void onServiceDisconnected(ComponentName arg0) {
		Log.d(LOGTAG, "unbound to service");
		isBoundToService.set(false);
	}



   public boolean restart () throws RemoteException {
      try {
         return webkitfilesService.restart();
      } catch (RemoteException rex) {
         rex.printStackTrace();
         throw rex;
      }
   }

	public boolean isBoundToService() {
		return isBoundToService.get();
	}

}