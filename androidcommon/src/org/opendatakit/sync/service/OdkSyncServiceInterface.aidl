package org.opendatakit.sync.service;

interface OdkSyncServiceInterface {

	String getSyncStatus(in String appName);
	
	boolean synchronize(in String appName);
	
	boolean push(in String appName);

}
