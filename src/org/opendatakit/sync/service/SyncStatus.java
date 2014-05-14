package org.opendatakit.sync.service;

public enum SyncStatus {
	INIT,
	SYNCING,
	NETWORK_ERROR,
	AUTH_RESOLUTION,
	CONFLICT_RESOLUTION,
	SYNC_COMPLETE;
}
