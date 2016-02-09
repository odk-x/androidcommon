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

package org.opendatakit.common.android.views;

import android.os.RemoteException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opendatakit.common.android.activities.IOdkDataActivity;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.listener.DatabaseConnectionListener;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.database.service.OdkDbInterface;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author mitchellsundt@gmail.com
 */
public class ExecutorContext implements DatabaseConnectionListener {
  private static final String TAG = "ExecutorContext";
    private static ExecutorContext currentContext = null;

    private static void updateCurrentContext(ExecutorContext ctxt) {
        if ( currentContext != null ) {
            ctxt.queueRequest(new ExecutorRequest(currentContext));
        }
        currentContext = ctxt;
        // register for database connection status changes
        ctxt.activity.registerDatabaseConnectionBackgroundListener(ctxt);
    }

    /**
     * The activity containing the web view.
     * Specifically, the API we need to access.
     */
    private final IOdkDataActivity activity;

  /**
   * The mutex used to guard all of the private data structures:
   *   worker, workQueue, activeConnection, mCacheOrderedDefns
   */
  private final Object mutex = new Object();

   /**
     * Our use of an executor is a bit odd:
     *
     * We need to handle database service disconnections.
     *
     * That requires direct management of the work queue.
     *
     * We still queue actions, but those actions need to pull
     * the request definitions off a work queue that is explicitly
     * managed by the ExecutorContext.
     *
     * The processors effectively record that there is (likely) work
     * to be processed. The work is held here.
     */
    private final ExecutorService worker = Executors.newSingleThreadExecutor();

    /**
     * workQueue should only be accessed by synchronized methods, as it may be
     * accessed in multiple threads.
     */
    private final LinkedList<ExecutorRequest> workQueue = new LinkedList<ExecutorRequest>();

  /**
   * Because all service requests are atomic, we can share a connection
   * across all uses. Only when we are shutting down do we then close
   * that database connection.
   *
   * The current active connection.
   */
  private OdkDbHandle activeConnection = null;

    private Map<String, OrderedColumns> mCachedOrderedDefns = new HashMap<String, OrderedColumns>();

    private ExecutorContext(IOdkDataActivity fragment) {
        this.activity = fragment;
        updateCurrentContext(this);
    }

    public static synchronized ExecutorContext getContext(IOdkDataActivity fragment) {
      if ( currentContext != null && (currentContext.activity == fragment)) {
        return currentContext;
      } else {
        return new ExecutorContext(fragment);
      }
    }

  /**
   * if we are not shutting down and there is work to be done then fire an ExecutorProcessor.
   */
  private void triggerExecutorProcessor() {
      // processor is most often NOT discarded
      ExecutorProcessor processor = activity.newExecutorProcessor(this);
      synchronized (mutex) {
        // we might have drained the queue -- or not.
        if ( !worker.isShutdown() && !worker.isTerminated() && !workQueue.isEmpty() ) {
          worker.execute(processor);
        }
      }
    }

  /**
   * if we are not shutting down then queue a request and fire an ExecutorProcessor.
   * @param request
   */
  public void queueRequest(ExecutorRequest request) {
      // processor is most often NOT discarded
      ExecutorProcessor processor = activity.newExecutorProcessor(this);
      synchronized (mutex) {
        if ( !worker.isShutdown() && !worker.isTerminated()) {
          // push the request
          workQueue.add(request);
          worker.execute(processor);
        }
      }
    }

  /**
   * @return the next ExecutorRequest or null if the queue is empty
   */
  public ExecutorRequest peekRequest() {
      synchronized (mutex) {
        if (workQueue.isEmpty()) {
          return null;
        } else {
          return workQueue.peekFirst();
        }
      }
    }

  /**
   * Remove the current item from the top of the work queue.
   *
   * @param trigger true if we should fire an ExecutorProcessor.
   */
  public void popRequest(boolean trigger) {
    // processor is most often NOT discarded
    ExecutorProcessor processor = (trigger ? activity.newExecutorProcessor(this) : null);
    synchronized (mutex) {
      if ( !workQueue.isEmpty() ) {
        workQueue.removeFirst();
      }
      if ( !worker.isShutdown() && !worker.isTerminated() && trigger && !workQueue.isEmpty() ) {
        // signal that we have work...
        worker.execute(processor);
      }
    }
  }

  /**
   * shutdown the worker. This is done within the mutex to ensure that the above methods
   * never throw an unexpected state exception.
   */
    private void shutdownWorker() {
      WebLogger.getLogger(currentContext.getAppName()).i(TAG, "shutdownWorker - shutting down odkDataIf Executor");
      Throwable t = null;
      synchronized (mutex) {
        if ( !worker.isShutdown() && !worker.isTerminated() ) {
          worker.shutdown();
        }
        try {
          worker.awaitTermination(3000L, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
          t = th;
        }
      }

      if ( t != null ) {
        WebLogger.getLogger(currentContext.getAppName()).w(TAG,
                "shutdownWorker - odkDataIf Executor threw exception while shutting down");
        WebLogger.getLogger(currentContext.getAppName()).printStackTrace(t);
      }
      WebLogger.getLogger(currentContext.getAppName()).i(TAG, "shutdownWorker - odkDataIf Executor has been shut down.");
    }

  /**
   * Get a new connection.
   *
   * @return OdkDbHandle
   */
  public OdkDbHandle getActiveConnection() {
    synchronized (mutex) {
      OdkDbInterface dbInterface;
      if ( activeConnection == null ) {
        dbInterface = activity.getDatabase();
        if ( dbInterface == null ) {
          WebLogger.getLogger(getAppName()).w(TAG,
              "failed openDatabase -- unable to access database service");
          return null;
        }
        try {
          activeConnection = dbInterface.openDatabase(getAppName());
        } catch (RemoteException e) {
          WebLogger.getLogger(getAppName()).w(TAG,
              "failed openDatabase -- " + e.getMessage());
        }
      }
      return activeConnection;
    }
  }

  public void terminateActiveConnection() {
    synchronized (mutex) {
      OdkDbInterface dbInterface;
      if ( activeConnection != null ) {
        try {
          dbInterface = activity.getDatabase();
          if (dbInterface == null) {
            // if the interface is down, the service itself will reclaim the OdkDbHandle.
            WebLogger.getLogger(getAppName())
                .w(TAG, "terminateActiveConnection -- unable to access database");
            return;
          }
          try {
            dbInterface.closeDatabase(getAppName(), activeConnection);
          } catch (RemoteException e) {
            WebLogger.getLogger(getAppName()).w(TAG,
                "resetting connection (disconnect/reconnect?) -- unable to release "
                    + activeConnection.getDatabaseHandle());
          }
        } finally {
          activeConnection = null;
        }
      }
    }
  }

  public OrderedColumns getOrderedColumns(String tableId) {
    synchronized (mutex) {
      return mCachedOrderedDefns.get(tableId);
    }
  }

  public void putOrderedColumns(String tableId, OrderedColumns orderedColumns) {
    synchronized (mutex) {
      mCachedOrderedDefns.put(tableId, orderedColumns);
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // No direct access to data structures below this point

  /**
   * @return
   */
    public OdkDbInterface getDatabase() {
        return activity.getDatabase();
    }

    public String getAppName() {
        return activity.getAppName();
    }

    public void releaseResources(String reason) {
      shutdownWorker();

  	  String errorMessage = "releaseResources - shutting down worker (" + reason +
                   ") -- rolling back all transactions and releasing all connections";
      for(;;) {
        ExecutorRequest req = peekRequest();
        if ( req == null ) {
          break;
        }
        try {
           reportError(req.callbackJSON, null, errorMessage);
        } catch(Exception e) {
           WebLogger.getLogger(getAppName()).w(TAG, "releaseResources - exception while "
               + "cancelling outstanding requests");
        } finally {
           popRequest(false);
        }
      }

      WebLogger.getLogger(currentContext.getAppName()).i(TAG, "releaseResources - workQueue has been purged.");

      terminateActiveConnection();

      WebLogger.getLogger(currentContext.getAppName()).w(TAG,
              "releaseResources - closed all associated dbHandles");
    }

    public void reportError(String callbackJSON, String transId, String errorMessage) {
      if ( callbackJSON != null ) {
        Map<String, Object> response = new HashMap<String, Object>();
        response.put("callbackJSON", callbackJSON);
        response.put("error", errorMessage);
        if (transId != null) {
          response.put("transId", transId);
        }
        String responseStr = null;
        try {
          responseStr = ODKFileUtils.mapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
          WebLogger.getLogger(currentContext.getAppName()).e(TAG, "should never have a conversion error");
          WebLogger.getLogger(currentContext.getAppName()).printStackTrace(e);
          throw new IllegalStateException("should never have a conversion error");
        }
        activity.signalResponseAvailable(responseStr);
      }
    }


    public void reportSuccess(String callbackJSON, String transId, ArrayList<List<Object>> data, Map<String,Object> metadata) {
        Map<String,Object> response = new HashMap<String,Object>();
        response.put("callbackJSON", callbackJSON);
        if ( transId != null ) {
            response.put("transId", transId);
        }
        if ( data != null ) {
            response.put("data", data);
        }
        if ( metadata != null ) {
            response.put("metadata", metadata);
        }
        String responseStr = null;
        try {
            responseStr = ODKFileUtils.mapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
          WebLogger.getLogger(currentContext.getAppName()).e(TAG, "should never have a conversion error");
          WebLogger.getLogger(currentContext.getAppName()).printStackTrace(e);
          throw new IllegalStateException("should never have a conversion error");
        }
        activity.signalResponseAvailable(responseStr);
    }

    @Override
    public void databaseAvailable() {
      // this may be called multiple times
      triggerExecutorProcessor();
    }

    @Override
    public void databaseUnavailable() {
      // if the service connection drops, the OdkDbHandles associated with that
      // service interface are closed and released.
      activeConnection = null;
      new ExecutorContext(activity);
    }

   public synchronized boolean isAlive() {
      return !(worker.isShutdown() || worker.isTerminated());
   }
}
