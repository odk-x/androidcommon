package org.opendatakit.common.android.utilities.test;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.utilities.ODKDatabaseUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.StaticStateManipulator;

import android.database.sqlite.SQLiteDatabase;
import android.test.AndroidTestCase;

public class ODKDatabaseUtilsMoreTests extends AndroidTestCase {

  private static final String APP_NAME = "androidCommonTest";
  
  SQLiteDatabase db = null;
  
  /*
   * Set up the database for the tests(non-Javadoc)
   * 
   * @see android.test.AndroidTestCase#setUp()
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();

    // wait for a context to exist
    while (null == getContext()) {
      
      try {
        Thread.sleep(500);
      } catch ( InterruptedException e ) {
        // ignore
      }
    }
    
//    RenamingDelegatingContext context = new RenamingDelegatingContext(getContext(),
//        APP_NAME);
//    setContext(context);

    StaticStateManipulator.get().reset();
    
    FileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(APP_NAME)));

    db = DatabaseFactory.get().getDatabase(getContext(), APP_NAME);
  }

  /*
   * Destroy all test data once tests are done(non-Javadoc)
   * 
   * @see android.test.AndroidTestCase#tearDown()
   */
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();

    if (db != null) {
      db.close();
    }
    
    FileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(APP_NAME)));
  }


  /*
   * Test query when there is no data
   */
  public void testQueryWithNoData_ExpectFail() {
    String tableId = "badTable";
    boolean thrown = false;

    try {
      ODKDatabaseUtils.get().query(db, false, tableId, null, null, null, null, null, null, null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);
  }


}
