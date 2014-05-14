package org.opendatakit.sync;

import java.util.concurrent.atomic.AtomicBoolean;

import org.opendatakit.sync.service.OdkSyncServiceInterface;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;

public class OdkSyncServiceProxy implements ServiceConnection {

	private final static String LOGTAG = OdkSyncServiceProxy.class
			.getSimpleName();

	private OdkSyncServiceInterface sensorSvcProxy;

	protected Context componentContext;
	protected final AtomicBoolean isBoundToService = new AtomicBoolean(false);

	public OdkSyncServiceProxy(Context context) {
		componentContext = context;
		Intent bind_intent = new Intent();
		bind_intent.setClassName(SyncConsts.SYNC_SERVICE_PACKAGE,
				SyncConsts.SYNC_SERVICE_CLASS);
		componentContext.bindService(bind_intent, this,
				Context.BIND_AUTO_CREATE);
	}

	public void shutdown() {
		Log.d(LOGTAG, "Application shutdown - unbinding from Syncervice");
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
		sensorSvcProxy = OdkSyncServiceInterface.Stub.asInterface(service);
		isBoundToService.set(true);
	}

	@Override
	public void onServiceDisconnected(ComponentName arg0) {
		Log.d(LOGTAG, "unbound to service");
		isBoundToService.set(false);
	}

	public String getSyncStatus(String appName) throws RemoteException {
		if(appName == null)
			throw new IllegalArgumentException("App Name cannot be null");
		try {
			return sensorSvcProxy.getSyncStatus(appName);
		} catch (RemoteException rex) {
			rex.printStackTrace();
			throw rex;
		}
	}

	public boolean pushToServer(String appName) throws RemoteException {
		if(appName == null)
			throw new IllegalArgumentException("App Name cannot be null");

		try {
			return sensorSvcProxy.push(appName);
		} catch (RemoteException rex) {
			rex.printStackTrace();
			throw rex;
		}
	}

	public boolean synchronizeFromServer(String appName) throws RemoteException {
		if(appName == null)
			throw new IllegalArgumentException("App Name cannot be null");

		try {
			return sensorSvcProxy.synchronize(appName);
		} catch (RemoteException rex) {
			rex.printStackTrace();
			throw rex;
		}
	}

	public boolean isBoundToService() {
		return isBoundToService.get();
	}

}
