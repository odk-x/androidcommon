package org.opendatakit.sync.service;

import org.opendatakit.sync.service.SyncStatus;
import org.opendatakit.sync.service.SyncProgressState;

interface OdkSyncServiceInterface {

	SyncStatus getSyncStatus(in String appName);
	
	boolean synchronize(in String appName);
	
	boolean push(in String appName);
	
	SyncProgressState getSyncProgress(in String appName);
	
	String getSyncUpdateMessage(in String appName);
}
