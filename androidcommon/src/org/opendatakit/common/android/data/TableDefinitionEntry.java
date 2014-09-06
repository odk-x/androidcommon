package org.opendatakit.common.android.data;

import org.opendatakit.aggregate.odktables.rest.SyncState;

/**
 * A simple struct to hold the contents of a 
 * table definition entry.
 * 
 * @author mitchellsundt@gmail.com
 *
 */
public class TableDefinitionEntry {
  
  public String tableId;
  
  public String syncTag;
  
  public String schemaETag;

  public String dataEtag;
  
  public String lastSyncTime;
  
  public SyncState syncState;
  
  public int transactioning;
}
