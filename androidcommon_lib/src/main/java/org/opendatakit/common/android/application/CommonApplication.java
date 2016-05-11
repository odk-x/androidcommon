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

import android.annotation.SuppressLint;
import android.app.Activity;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.pm.PackageManager;
import android.content.res.Configuration;
import android.os.AsyncTask;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.util.Log;
import android.view.View;
import android.webkit.WebView;
import org.opendatakit.common.android.listener.DatabaseConnectionListener;
import org.opendatakit.common.android.listener.InitializationListener;
import org.opendatakit.common.android.logic.CommonToolProperties;
import org.opendatakit.common.android.logic.PropertiesSingleton;
import org.opendatakit.common.android.task.InitializationTask;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.views.ODKWebView;
import org.opendatakit.database.DatabaseConsts;
import org.opendatakit.database.service.OdkDbInterface;
import org.opendatakit.webkitserver.WebkitServerConsts;
import org.opendatakit.webkitserver.service.OdkWebkitServerInterface;

import java.util.ArrayList;

public abstract class CommonApplication extends AppAwareApplication implements
    InitializationListener {

  private static final String t = "CommonApplication";
  
  public static final String PERMISSION_WEBSERVER = "org.opendatakit.webkitserver.RUN_WEBSERVER";
  public static final String PERMISSION_DATABASE = "org.opendatakit.database.RUN_DATABASE";

  // Support for mocking the remote interfaces that are actually accessed
  // vs. the WebKit service, which is merely started.
  private static boolean isMocked = false;
  
  // Hack to determine whether or not to cascade to the initialize task
  private static boolean disableInitializeCascade = true;
  
  // Hack for handling mock interfaces...
  private static OdkDbInterface mockDatabaseService = null;
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

  public static void setMockWebkitServer(OdkWebkitServerInterface mock) {
    CommonApplication.mockWebkitServerService = mock;
  }

  public static void mockServiceConnected(CommonApplication app, String name) {
    ComponentName className = null;
    if (name.equals(WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS)) {
      className = new ComponentName(WebkitServerConsts.WEBKITSERVER_SERVICE_PACKAGE,
          WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS);
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

  public abstract int getConfigZipResourceId();
  
  public abstract int getSystemZipResourceId();
  
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

  private Activity activeActivity = null;
  private Activity databaseListenerActivity = null;
  
  public void onActivityPause(Activity activity) {
    if ( activeActivity == activity ) {
      mInitializationListener = null;
  
      if (mBackgroundTasks.mInitializationTask != null) {
        mBackgroundTasks.mInitializationTask.setInitializationListener(null);
      }
    }
  }
  
  public void onActivityDestroy(Activity activity) {
    if ( activeActivity == activity ) {
      activeActivity = null;
      
      mInitializationListener = null;
  
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

  private void shutdownServices() {
    Log.i(t, "shutdownServices - Releasing WebServer and database service");
    mBackgroundServices.isDestroying = true;
    mBackgroundServices.webkitfilesService = null;
    mBackgroundServices.databaseService = null;
    // release interfaces held by the view
    configureView();
    // release the webkitfilesService
    unbindWebkitfilesServiceWrapper();
    unbindDatabaseBinderWrapper();
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
            wv.serviceChange(false);
          } else {
            OdkWebkitServerInterface webkitServerIf = getWebkitServer();
            OdkDbInterface dbIf = getDatabase();
            wv.serviceChange(webkitServerIf != null && dbIf != null);
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

  public void establishDoNotFireDatabaseConnectionListener(Activity activity) {
    databaseListenerActivity = activity;
  }

  public void fireDatabaseConnectionListener() {
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
      mBackgroundTasks.mInitializationTask.execute((Void) null);
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
