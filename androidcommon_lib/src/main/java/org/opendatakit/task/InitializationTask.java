/*
 * Copyright (C) 2013 University of Washington
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

package org.opendatakit.task;

import android.os.AsyncTask;
import org.opendatakit.application.CommonApplication;
import org.opendatakit.builder.InitializationOutcome;
import org.opendatakit.builder.InitializationSupervisor;
import org.opendatakit.builder.InitializationUtil;
import org.opendatakit.database.service.UserDbInterface;
import org.opendatakit.listener.InitializationListener;

import java.util.ArrayList;

/**
 * Background task for exploding the built-in zipfile resource into the
 * framework directory of the application and doing forms discovery on this
 * appName.
 *
 * @author mitchellsundt@gmail.com
 */
public class InitializationTask extends AsyncTask<Void, String, InitializationOutcome> {

  private static final String t = "InitializationTask";

  private CommonApplication appContext;
  private InitializationListener mStateListener;
  private String appName;

  private boolean mSuccess = false;
  private ArrayList<String> mResult = new ArrayList<String>();

  @Override
  protected InitializationOutcome doInBackground(Void... values) {
    InitializationUtil util = new InitializationUtil(appContext, appName,
        new InitializationSupervisor() {
          @Override public UserDbInterface getDatabase() {
            return appContext.getDatabase();
          }

          @Override public void publishProgress(String progress, String detail) {
            InitializationTask.this.publishProgress(progress, detail);
          }

          @Override public boolean isCancelled() {
            return InitializationTask.this.isCancelled();
          }

          @Override public String getToolName() {
            return appContext.getToolName();
          }

          @Override public String getVersionCodeString() {
            return appContext.getVersionCodeString();
          }

          @Override public int getSystemZipResourceId() {
            return appContext.getSystemZipResourceId();
          }

          @Override public int getConfigZipResourceId() {
            return appContext.getConfigZipResourceId();
          }
        } );

    InitializationOutcome pendingOutcome = util.initialize();

    return pendingOutcome;
  }

  @Override
  protected void onPostExecute(InitializationOutcome pendingOutcome) {
    synchronized (this) {
      mResult = pendingOutcome.outcomeLineItems;
      mSuccess = !pendingOutcome.problemExtractingToolZipContent &&
          !pendingOutcome.problemDefiningTables &&
          !pendingOutcome.problemDefiningForms &&
          !pendingOutcome.problemImportingAssetCsvContent &&
          pendingOutcome.assetsCsvFileNotFoundSet.isEmpty();

      if (mStateListener != null) {
        mStateListener.initializationComplete(mSuccess, mResult);
      }
    }
  }

  @Override
  protected void onCancelled(InitializationOutcome pendingOutcome) {
    synchronized (this) {
      // can be null if cancelled before task executes
      mResult = (pendingOutcome == null) ? new ArrayList<String>() : pendingOutcome.outcomeLineItems;
      mSuccess = false;
      if (mStateListener != null) {
        mStateListener.initializationComplete(mSuccess, mResult);
      }
    }
  }

  @Override
  protected void onProgressUpdate(String... values) {
    synchronized (this) {
      if (mStateListener != null) {
        // update progress and total
        mStateListener.initializationProgressUpdate(
            values[0] + ((values[1] != null) ? "\n(" + values[1] + ")" : ""));
      }
    }

  }

  public boolean getOverallSuccess() {
    return mSuccess;
  }

  public ArrayList<String> getResult() {
    return mResult;
  }

  public void setInitializationListener(InitializationListener sl) {
    synchronized (this) {
      mStateListener = sl;
    }
  }

  public void setAppName(String appName) {
    synchronized (this) {
      this.appName = appName;
    }
  }

  public String getAppName() {
    return appName;
  }

  public void setApplication(CommonApplication appContext) {
    synchronized (this) {
      this.appContext = appContext;
    }
  }

  public CommonApplication getApplication() {
    return appContext;
  }

}
