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

package org.opendatakit.activities;

import android.app.Application;
import android.preference.PreferenceActivity;
import org.opendatakit.application.CommonApplication;
import org.opendatakit.database.service.UserDbInterface;

public abstract class BasePreferenceActivity extends PreferenceActivity
    implements IAppAwareActivity {

  @Override
  protected void onResume() {
    super.onResume();
    ((CommonApplication) getApplication()).onActivityResume(this);
  }

  @Override
  protected void onPause() {
    ((CommonApplication) getApplication()).onActivityPause(this);
    super.onPause();
  }

  @Override
  protected void onDestroy() {
    ((CommonApplication) getApplication()).onActivityDestroy(this);
    super.onDestroy();
  }

  /**
   * Direct copy paste of {@link BaseActivity#getDatabase()} ()}
   *
   * @return the database interface
   */
  public UserDbInterface getDatabase() {
    return getCommonApplication().getDatabase();
  }

  /**
   * Direct copy paste of {@link BaseActivity#getCommonApplication()}
   *
   * @return the common application
   */
  public CommonApplication getCommonApplication() {
    Application app = getApplication();
    if (app instanceof CommonApplication) {
      return (CommonApplication) app;
    }
    throw new IllegalStateException("Bad app");
  }

}
