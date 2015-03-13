package org.opendatakit.common.android.activities;

import org.opendatakit.common.android.application.CommonApplication;

import android.app.ListActivity;

public abstract class BaseListActivity extends ListActivity implements IAppAwareActivity {

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
