/*
 * Copyright (C) 2015 University of Washington
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

package org.opendatakit.common.android.application;

import java.util.ArrayList;

import org.opendatakit.androidcommon.R;
import org.opendatakit.common.android.listener.DatabaseConnectionListener;
import org.opendatakit.common.android.listener.InitializationListener;
import org.opendatakit.common.android.listener.LicenseReaderListener;
import org.opendatakit.common.android.logic.CommonToolProperties;
import org.opendatakit.common.android.logic.PropertiesSingleton;
import org.opendatakit.common.android.task.InitializationTask;
import org.opendatakit.common.android.task.LicenseReaderTask;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.android.views.ODKWebView;
import org.opendatakit.database.DatabaseConsts;
import org.opendatakit.database.service.OdkDbInterface;
import org.opendatakit.dbshim.DbShimConsts;
import org.opendatakit.dbshim.service.OdkDbShimInterface;
import org.opendatakit.webkitserver.WebkitServerConsts;
import org.opendatakit.webkitserver.service.OdkWebkitServerInterface;

import android.annotation.SuppressLint;
import android.app.Activity;
import android.app.Application;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.content.pm.PackageManager.NameNotFoundException;
import android.content.res.Configuration;
import android.os.AsyncTask;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.util.Log;
import android.view.View;
import android.webkit.WebView;
import android.widget.Toast;

public abstract class CommonApplication extends Application  implements LicenseReaderListener, InitializationListener {

  private static final String t = "CommonApplication";
  
  public static final String PERMISSION_WEBSERVER = "org.opendatakit.webkitserver.RUN_WEBSERVER";
  public static final String PERMISSION_DATABASE = "org.opendatakit.database.RUN_DATABASE";
  public static final String PERMISSION_DBSHIM = "org.opendatakit.dbshim.RUN_DBSHIM";

  // Support for mocking the remote interfaces that are actually accessed
  // vs. the WebKit service, which is merely started.
  private static boolean isMocked = false;
  
  // Hack to determine whether or not to cascade to the initialize task
  private static boolean disableInitializeCascade = true;
  
  // Hack for handling mock interfaces...
  private static OdkDbInterface mockDatabaseService = null;
  private static OdkDbShimInterface mockShimService = null;
  private static OdkWebkitServerInterface mockWebkitServerService = null;
  
  public static void setMocked() {
    isMocked = true;
  }
  
  public static boolean isMocked() {
    return isMocked;
  }
  
  public static boolean isDisableInitializeCascade() {
    return disableInitializeCascade;
  }
  
  public static void setEnableInitializeCascade() {
    disableInitializeCascade = false;
  }
  
  public static void setDisableInitializeCascade() {
    disableInitializeCascade = true;
  }
  
  public static void setMockDatabase(OdkDbInterface mock) {
    CommonApplication.mockDatabaseService = mock;
  }

  public static void setMockDbShim(OdkDbShimInterface mock) {
    CommonApplication.mockShimService = mock;
  }

  public static void setMockWebkitServer(OdkWebkitServerInterface mock) {
    CommonApplication.mockWebkitServerService = mock;
  }

  public static void mockServiceConnected(CommonApplication app, String name) {
    ComponentName className = null;
    if (name.equals(WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS)) {
      className = new ComponentName(WebkitServerConsts.WEBKITSERVER_SERVICE_PACKAGE,
          WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS);
    }

    if (name.equals(DbShimConsts.DBSHIM_SERVICE_CLASS)) {
      className = new ComponentName(DbShimConsts.DBSHIM_SERVICE_PACKAGE,
          DbShimConsts.DBSHIM_SERVICE_CLASS);
    }

    if (name.equals(DatabaseConsts.DATABASE_SERVICE_CLASS)) {
      className = new ComponentName(DatabaseConsts.DATABASE_SERVICE_PACKAGE,
          DatabaseConsts.DATABASE_SERVICE_CLASS);
    }
    
    if ( className == null ) {
      throw new IllegalStateException("unrecognized mockService");
    }
    
    app.doServiceConnected(className, null);
  }

  public static void mockServiceDisconnected(CommonApplication app, String name) {
    ComponentName className = null;
    if (name.equals(WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS)) {
      className = new ComponentName(WebkitServerConsts.WEBKITSERVER_SERVICE_PACKAGE,
          WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS);
    }

    if (name.equals(DbShimConsts.DBSHIM_SERVICE_CLASS)) {
      className = new ComponentName(DbShimConsts.DBSHIM_SERVICE_PACKAGE,
          DbShimConsts.DBSHIM_SERVICE_CLASS);
    }

    if (name.equals(DatabaseConsts.DATABASE_SERVICE_CLASS)) {
      className = new ComponentName(DatabaseConsts.DATABASE_SERVICE_PACKAGE,
          DatabaseConsts.DATABASE_SERVICE_CLASS);
    }
    
    if ( className == null ) {
      throw new IllegalStateException("unrecognized mockService");
    }
    
    app.doServiceDisconnected(className);
  }
  
  /**
   * Wrapper class for service activation management.
   * 
   * @author mitchellsundt@gmail.com
   *
   */
  private final class ServiceConnectionWrapper implements ServiceConnection {

    @Override
    public void onServiceConnected(ComponentName name, IBinder service) {
      CommonApplication.this.doServiceConnected(name, service);
    }

    @Override
    public void onServiceDisconnected(ComponentName name) {
      CommonApplication.this.doServiceDisconnected(name);
    }
  }

  /**
   * Task instances that are preserved until the application dies.
   * 
   * @author mitchellsundt@gmail.com
   *
   */
  private static final class BackgroundTasks {
    LicenseReaderTask mLicenseReaderTask = null;
    InitializationTask mInitializationTask = null;

    BackgroundTasks() {
    };
  }

  /**
   * Service connections that are preserved until the application dies.
   * 
   * @author mitchellsundt@gmail.com
   *
   */
  private static final class BackgroundServices {

    private ServiceConnectionWrapper webkitfilesServiceConnection = null;
    private OdkWebkitServerInterface webkitfilesService = null;
    private ServiceConnectionWrapper dbShimServiceConnection = null;
    private OdkDbShimInterface dbShimService = null;
    private ServiceConnectionWrapper databaseServiceConnection = null;
    private OdkDbInterface databaseService = null;
    private boolean isDestroying = false;

    BackgroundServices() {
    };
  }

  /**
   * Creates required directories on the SDCard (or other external storage)
   *
   * @return true if there are tables present
   * @throws RuntimeException
   *           if there is no SDCard or the directory exists as a non directory
   */
  public static void createODKDirs(String appName) throws RuntimeException {

    ODKFileUtils.verifyExternalStorageAvailability();

    ODKFileUtils.assertDirectoryStructure(appName);
  }

  // handed across orientation changes
  private final BackgroundTasks mBackgroundTasks = new BackgroundTasks(); 

  // handed across orientation changes
  private final BackgroundServices mBackgroundServices = new BackgroundServices(); 

  // These are expected to be broken down and set up during orientation changes.
  private LicenseReaderListener mLicenseReaderListener = null;
  private InitializationListener mInitializationListener = null;

  private boolean shuttingDown = false;
  
  public CommonApplication() {
    super();
  }
  
  @SuppressLint("NewApi")
  @Override
  public void onCreate() {
    shuttingDown = false;
    super.onCreate();

    if (Build.VERSION.SDK_INT >= 19) {
      WebView.setWebContentsDebuggingEnabled(true);
    }
  }

  @Override
  public void onConfigurationChanged(Configuration newConfig) {
    super.onConfigurationChanged(newConfig);
    Log.i(t, "onConfigurationChanged");
  }

  @Override
  public void onTerminate() {
    cleanShutdown();
    super.onTerminate();
    Log.i(t, "onTerminate");
  }

  public int getQuestionFontsize(String appName) {
    PropertiesSingleton props = CommonToolProperties.get(this, appName);
    Integer question_font = props.getIntegerProperty(CommonToolProperties.KEY_FONT_SIZE);
    int questionFontsize = (question_font == null) ? CommonToolProperties.DEFAULT_FONT_SIZE : question_font;
    return questionFontsize;
  }

  /**
   * The tool name is the name of the package after the org.opendatakit. prefix.
   * 
   * @return the tool name.
   */
  public String getToolName() {
    String packageName = getPackageName();
    String[] parts = packageName.split("\\.");
    return parts[2];
  }

  public String getVersionCodeString() {
    try {
      PackageInfo pinfo;
      pinfo = getPackageManager().getPackageInfo(getPackageName(), 0);
      int versionNumber = pinfo.versionCode;
      return Integer.toString(versionNumber);
    } catch (NameNotFoundException e) {
      e.printStackTrace();
      return "";
    }
  }

  public String getVersionDetail() {
    String versionDetail = "";
    try {
      PackageInfo pinfo;
      pinfo = getPackageManager().getPackageInfo(getPackageName(), 0);
      int versionNumber = pinfo.versionCode;
      String versionName = pinfo.versionName;
      versionDetail = " " + versionName + " (rev " + versionNumber + ")";
    } catch (NameNotFoundException e) {
      e.printStackTrace();
    }
    return versionDetail;
  }

  public String getVersionedAppName() {
    String versionDetail = this.getVersionDetail();
    return getString(getApkDisplayNameResourceId()) + versionDetail;
  }

  public abstract int getApkDisplayNameResourceId();
  
  public abstract int getAssetZipResourceId();
  
  public abstract int getFrameworkZipResourceId();
  
  public abstract int getWebKitResourceId();
  
  public boolean shouldRunInitializationTask(String appName) {
    if ( isMocked() ) {
      if ( isDisableInitializeCascade() ) {
        return false;
      }
    }
    PropertiesSingleton props = CommonToolProperties.get(this, appName);
    return props.shouldRunInitializationTask(this.getToolName());
  }

  public void clearRunInitializationTask(String appName) {
    PropertiesSingleton props = CommonToolProperties.get(this, appName);
    props.clearRunInitializationTask(this.getToolName());
  }

  public void setRunInitializationTask(String appName) {
    PropertiesSingleton props = CommonToolProperties.get(this, appName);
    props.setRunInitializationTask(this.getToolName());
  }

  private <T> void executeTask(AsyncTask<T, ?, ?> task, T... args) {

    int androidVersion = android.os.Build.VERSION.SDK_INT;
    if (androidVersion < 11) {
      task.execute(args);
    } else {
      // TODO: execute on serial executor in version 11 onward...
      task.execute(args);
      // task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, (Void[]) null);
    }

  }

  private Activity activeActivity = null;
  private Activity databaseListenerActivity = null;
  
  public void onActivityPause(Activity activity) {
    if ( activeActivity == activity ) {
      mLicenseReaderListener = null;
      mInitializationListener = null;
  
      if (mBackgroundTasks.mLicenseReaderTask != null) {
        mBackgroundTasks.mLicenseReaderTask.setLicenseReaderListener(null);
      }
      if (mBackgroundTasks.mInitializationTask != null) {
        mBackgroundTasks.mInitializationTask.setInitializationListener(null);
      }
    }
  }
  
  public void onActivityDestroy(Activity activity) {
    if ( activeActivity == activity ) {
      activeActivity = null;
      
      mLicenseReaderListener = null;
      mInitializationListener = null;
  
      if (mBackgroundTasks.mLicenseReaderTask != null) {
        mBackgroundTasks.mLicenseReaderTask.setLicenseReaderListener(null);
      }
      if (mBackgroundTasks.mInitializationTask != null) {
        mBackgroundTasks.mInitializationTask.setInitializationListener(null);
      }

      final Handler handler = new Handler();
      handler.postDelayed(new Runnable() {
        @Override
        public void run() {
          CommonApplication.this.testForShutdown();
        }
      }, 500);
    }
  }

  private void cleanShutdown() {
    try {
      shuttingDown = true;
      
      shutdownServices();
      WebLogger.closeAll();
    } finally {
      shuttingDown = false;
    }
  }
  
  private void testForShutdown() {
    // no other activity has been started -- shut down
    if ( activeActivity == null ) {
      cleanShutdown();
    }
  }

  public void onActivityResume(Activity activity) {
    databaseListenerActivity = null;
    activeActivity = activity;
    
    if (mBackgroundTasks.mLicenseReaderTask != null) {
      mBackgroundTasks.mLicenseReaderTask.setLicenseReaderListener(this);
    }
    if (mBackgroundTasks.mInitializationTask != null) {
      mBackgroundTasks.mInitializationTask.setInitializationListener(this);
    }
    
    // be sure the services are connected...
    mBackgroundServices.isDestroying = false;

    configureView();

    // failsafe -- ensure that the services are active...
    bindToService();
  }

  // /////////////////////////////////////////////////////////////////////////
  // service interactions
  private void unbindWebkitfilesServiceWrapper() {
    try {
      ServiceConnectionWrapper tmp = mBackgroundServices.webkitfilesServiceConnection;
      mBackgroundServices.webkitfilesServiceConnection = null;
      if ( tmp != null ) {
        unbindService(tmp);
      }
    } catch ( Exception e ) {
      // ignore
      e.printStackTrace();
    }
  }
  
  private void unbindDatabaseBinderWrapper() {
    try {
      ServiceConnectionWrapper tmp = mBackgroundServices.databaseServiceConnection;
      mBackgroundServices.databaseServiceConnection = null;
      if ( tmp != null ) {
        unbindService(tmp);
        triggerDatabaseEvent(false);
      }
    } catch ( Exception e ) {
      // ignore
      e.printStackTrace();
    }
  }
  
  private void unbindDbShimBinderWrapper() {
    try {
      ServiceConnectionWrapper tmp = mBackgroundServices.dbShimServiceConnection;
      mBackgroundServices.dbShimServiceConnection = null;
      if ( tmp != null ) {
        unbindService(tmp);
      }
    } catch ( Exception e ) {
      // ignore
      e.printStackTrace();
    }
  }
  
  private void shutdownServices() {
    Log.i(t, "shutdownServices - Releasing WebServer and DbShim service");
    mBackgroundServices.isDestroying = true;
    mBackgroundServices.webkitfilesService = null;
    mBackgroundServices.dbShimService = null;
    mBackgroundServices.databaseService = null;
    // release interfaces held by the view
    configureView();
    // release the webkitfilesService
    unbindWebkitfilesServiceWrapper();
    unbindDatabaseBinderWrapper();
    unbindDbShimBinderWrapper();
  }
  
  private void bindToService() {
    if ( isMocked ) {
      // we directly control all the service binding interactions if we are mocked
      return;
    }
    if (!shuttingDown && !mBackgroundServices.isDestroying) {
      PackageManager pm = getPackageManager();
      boolean useWebServer = (pm.checkPermission(PERMISSION_WEBSERVER, getPackageName()) == PackageManager.PERMISSION_GRANTED);
      boolean useDatabase = (pm.checkPermission(PERMISSION_DATABASE, getPackageName()) == PackageManager.PERMISSION_GRANTED);
      boolean useDbShim = (pm.checkPermission(PERMISSION_DBSHIM, getPackageName()) == PackageManager.PERMISSION_GRANTED);
      
          // do something
      
      if (useWebServer && mBackgroundServices.webkitfilesService == null && 
          mBackgroundServices.webkitfilesServiceConnection == null) {
        Log.i(t, "Attempting bind to WebServer service");
        mBackgroundServices.webkitfilesServiceConnection = new ServiceConnectionWrapper();
        Intent bind_intent = new Intent();
        bind_intent.setClassName(WebkitServerConsts.WEBKITSERVER_SERVICE_PACKAGE,
            WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS);
        bindService(
            bind_intent,
            mBackgroundServices.webkitfilesServiceConnection,
            Context.BIND_AUTO_CREATE
                | ((Build.VERSION.SDK_INT >= 14) ? Context.BIND_ADJUST_WITH_ACTIVITY : 0));
      }

      if (useDbShim && mBackgroundServices.dbShimService == null && 
          mBackgroundServices.dbShimServiceConnection == null) {
        Log.i(t, "Attempting bind to DbShim service");
        mBackgroundServices.dbShimServiceConnection = new ServiceConnectionWrapper();
        Intent bind_intent = new Intent();
        bind_intent.setClassName(DbShimConsts.DBSHIM_SERVICE_PACKAGE,
            DbShimConsts.DBSHIM_SERVICE_CLASS);
        bindService(
            bind_intent,
            mBackgroundServices.dbShimServiceConnection,
            Context.BIND_AUTO_CREATE
                | ((Build.VERSION.SDK_INT >= 14) ? Context.BIND_ADJUST_WITH_ACTIVITY : 0));
      }

      if (useDatabase && mBackgroundServices.databaseService == null && 
          mBackgroundServices.databaseServiceConnection == null) {
        Log.i(t, "Attempting bind to Database service");
        mBackgroundServices.databaseServiceConnection = new ServiceConnectionWrapper();
        Intent bind_intent = new Intent();
        bind_intent.setClassName(DatabaseConsts.DATABASE_SERVICE_PACKAGE,
            DatabaseConsts.DATABASE_SERVICE_CLASS);
        bindService(
            bind_intent,
            mBackgroundServices.databaseServiceConnection,
            Context.BIND_AUTO_CREATE
                | ((Build.VERSION.SDK_INT >= 14) ? Context.BIND_ADJUST_WITH_ACTIVITY : 0));
      }
    }
  }

  /**
   * 
   * @param className
   * @param service can be null if we are mocked
   */
  private void doServiceConnected(ComponentName className, IBinder service) {
    if (className.getClassName().equals(WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS)) {
      Log.i(t, "Bound to WebServer service");
      mBackgroundServices.webkitfilesService = (service == null) ? null : OdkWebkitServerInterface.Stub.asInterface(service);
    }

    if (className.getClassName().equals(DbShimConsts.DBSHIM_SERVICE_CLASS)) {
      Log.i(t, "Bound to DbShim service");
      mBackgroundServices.dbShimService = (service == null) ? null : OdkDbShimInterface.Stub.asInterface(service);
    }

    if (className.getClassName().equals(DatabaseConsts.DATABASE_SERVICE_CLASS)) {
      Log.i(t, "Bound to Database service");
      mBackgroundServices.databaseService = (service == null) ? null : OdkDbInterface.Stub.asInterface(service);
      
      triggerDatabaseEvent(false);
    }

    configureView();
  }
  
  public OdkDbInterface getDatabase() {
    if ( isMocked ) {
      return mockDatabaseService;
    } else {
      return mBackgroundServices.databaseService;
    }
  }
  
  private OdkDbShimInterface getDbShim() {
    if ( isMocked ) {
      return mockShimService;
    } else {
      return mBackgroundServices.dbShimService;
    }
  }
  
  private OdkWebkitServerInterface getWebkitServer() {
    if ( isMocked ) {
      return mockWebkitServerService;
    } else {
      return mBackgroundServices.webkitfilesService;
    }
  }
  
  public void configureView() {
    if ( activeActivity != null ) {
      Log.i(t, "configureView - possibly updating service information within ODKWebView");
      if ( getWebKitResourceId() != -1 ) {
        View v = activeActivity.findViewById(getWebKitResourceId());
        if (v != null && v instanceof ODKWebView) {
          ODKWebView wv = (ODKWebView) v;
          if (mBackgroundServices.isDestroying) {
            wv.serviceChange(false, null);
          } else if (activeActivity != null) {
            OdkWebkitServerInterface webkitServerIf = getWebkitServer();
            OdkDbShimInterface dbShimIf = getDbShim();
            wv.serviceChange(webkitServerIf != null && 
                dbShimIf != null, dbShimIf);
          }
        }
      }
    }
  }

  private void doServiceDisconnected(ComponentName className) {
    if (className.getClassName().equals(WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS)) {
      if (mBackgroundServices.isDestroying) {
        Log.i(t, "Unbound from WebServer service (intentionally)");
      } else {
        Log.w(t, "Unbound from WebServer service (unexpected)");
      }
      mBackgroundServices.webkitfilesService = null;
      unbindWebkitfilesServiceWrapper();
    }

    if (className.getClassName().equals(DbShimConsts.DBSHIM_SERVICE_CLASS)) {
      if (mBackgroundServices.isDestroying) {
        Log.i(t, "Unbound from DbShim service (intentionally)");
      } else {
        Log.w(t, "Unbound from DbShim service (unexpected)");
      }
      mBackgroundServices.dbShimService = null;
      unbindDbShimBinderWrapper();
    }

    if (className.getClassName().equals(DatabaseConsts.DATABASE_SERVICE_CLASS)) {
      if (mBackgroundServices.isDestroying) {
        Log.i(t, "Unbound from Database service (intentionally)");
      } else {
        Log.w(t, "Unbound from Database service (unexpected)");
      }
      mBackgroundServices.databaseService = null;
      unbindDatabaseBinderWrapper();
    }

    configureView();

    // the bindToService() method decides whether to connect or not...
    bindToService();
  }

  // /////////////////////////////////////////////////////////////////////////
  // registrations

  public void establishReadLicenseListener(LicenseReaderListener listener) {
    mLicenseReaderListener = listener;
    // async task may have completed while we were reorienting...
    if (mBackgroundTasks.mLicenseReaderTask != null
        && mBackgroundTasks.mLicenseReaderTask.getStatus() == AsyncTask.Status.FINISHED) {
      this.readLicenseComplete(mBackgroundTasks.mLicenseReaderTask.getResult());
    }
  }
  
  /**
   * Called by an activity when it has been sufficiently initialized so
   * that it can handle a databaseAvailable() call.
   * 
   * @param activity
   */
  public void establishDatabaseConnectionListener(Activity activity) {
    databaseListenerActivity = activity;
    triggerDatabaseEvent(true);
  }

  /**
   * If the given activity is active, then fire the callback based upon 
   * the availability of the database.
   * 
   * @param activity
   * @param listener
   */
  public void possiblyFireDatabaseCallback(Activity activity, DatabaseConnectionListener listener) {
    if (  activeActivity != null &&
        activeActivity == databaseListenerActivity &&
        databaseListenerActivity == activity ) {
      if ( this.getDatabase() == null ) {
        listener.databaseUnavailable();
      } else {
        listener.databaseAvailable();
      }
    }
  }
  
  private void triggerDatabaseEvent(boolean availableOnly) {
    if ( activeActivity != null &&
        activeActivity == databaseListenerActivity &&
        activeActivity instanceof DatabaseConnectionListener ) {
      if ( !availableOnly && this.getDatabase() == null ) {
        ((DatabaseConnectionListener) activeActivity).databaseUnavailable();
      } else {
        ((DatabaseConnectionListener) activeActivity).databaseAvailable();
      }
    }
  }
  
  public void establishInitializationListener(InitializationListener listener) {
    mInitializationListener = listener;
    // async task may have completed while we were reorienting...
    if (mBackgroundTasks.mInitializationTask != null
        && mBackgroundTasks.mInitializationTask.getStatus() == AsyncTask.Status.FINISHED) {
      this.initializationComplete(mBackgroundTasks.mInitializationTask.getOverallSuccess(),
          mBackgroundTasks.mInitializationTask.getResult());
    }
  }

  // ///////////////////////////////////////////////////
  // actions

  public synchronized void readLicenseFile(String appName, LicenseReaderListener listener) {
    mLicenseReaderListener = listener;
    if (mBackgroundTasks.mLicenseReaderTask != null
        && mBackgroundTasks.mLicenseReaderTask.getStatus() != AsyncTask.Status.FINISHED) {
      Toast.makeText(this, getString(R.string.still_reading_license_file), Toast.LENGTH_LONG).show();
    } else {
      LicenseReaderTask lrt = new LicenseReaderTask();
      lrt.setApplication(this);
      lrt.setAppName(appName);
      lrt.setLicenseReaderListener(this);
      mBackgroundTasks.mLicenseReaderTask = lrt;
      executeTask(mBackgroundTasks.mLicenseReaderTask);
    }
  }

  public synchronized boolean initializeAppName(String appName, InitializationListener listener) {
    mInitializationListener = listener;
    if (mBackgroundTasks.mInitializationTask != null
        && mBackgroundTasks.mInitializationTask.getStatus() != AsyncTask.Status.FINISHED) {
      // Toast.makeText(this.getActivity(),
      // getString(R.string.expansion_in_progress),
      // Toast.LENGTH_LONG).show();
      return true;
    } else if ( getDatabase() != null ) {
      InitializationTask cf = new InitializationTask();
      cf.setApplication(this);
      cf.setAppName(appName);
      cf.setInitializationListener(this);
      mBackgroundTasks.mInitializationTask = cf;
      executeTask(mBackgroundTasks.mInitializationTask, (Void) null);
      return true;
    } else {
      return false;
    }
  }

  // /////////////////////////////////////////////////////////////////////////
  // clearing tasks
  //
  // NOTE: clearing these makes us forget that they are running, but it is
  // up to the task itself to eventually shutdown. i.e., we don't quite
  // know when they actually stop.


  public synchronized void clearInitializationTask() {
    mInitializationListener = null;
    if (mBackgroundTasks.mInitializationTask != null) {
      mBackgroundTasks.mInitializationTask.setInitializationListener(null);
      if (mBackgroundTasks.mInitializationTask.getStatus() != AsyncTask.Status.FINISHED) {
        mBackgroundTasks.mInitializationTask.cancel(true);
      }
    }
    mBackgroundTasks.mInitializationTask = null;
  }

  // /////////////////////////////////////////////////////////////////////////
  // cancel requests
  //
  // These maintain communications paths, so that we get a failure
  // completion callback eventually.


  public synchronized void cancelInitializationTask() {
    if (mBackgroundTasks.mInitializationTask != null) {
      if (mBackgroundTasks.mInitializationTask.getStatus() != AsyncTask.Status.FINISHED) {
        mBackgroundTasks.mInitializationTask.cancel(true);
      }
    }
  }

  // /////////////////////////////////////////////////////////////////////////
  // callbacks

  @Override
  public void readLicenseComplete(String result) {
    if (mLicenseReaderListener != null) {
      mLicenseReaderListener.readLicenseComplete(result);
    }
    mBackgroundTasks.mLicenseReaderTask = null;
  }

  
  @Override
  public void initializationComplete(boolean overallSuccess, ArrayList<String> result) {
    if (mInitializationListener != null) {
      mInitializationListener.initializationComplete(overallSuccess, result);
    }
  }

  @Override
  public void initializationProgressUpdate(String status) {
    if (mInitializationListener != null) {
      mInitializationListener.initializationProgressUpdate(status);
    }
  }

}
