package org.opendatakit.common.android.activities;

import org.opendatakit.common.android.application.CommonApplication;
import org.opendatakit.common.android.listener.DatabaseConnectionListener;

import android.app.Activity;

public abstract class BaseActivity extends Activity implements DatabaseConnectionListener, IAppAwareActivity {

  @Override
  protected void onResume() {
    super.onResume();
    ((CommonApplication) getApplication()).onActivityResume(this);
  }

  @Override
  protected void onPause() {
    ((CommonApplication) getApplication()).onActivityPause(this);
    super.onDestroy();
  }

  @Override
  protected void onDestroy() {
    ((CommonApplication) getApplication()).onActivityDestroy(this);
    super.onDestroy();
  }

}
