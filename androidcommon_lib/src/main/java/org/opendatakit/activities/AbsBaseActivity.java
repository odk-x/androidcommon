package org.opendatakit.activities;

import android.app.Activity;
import org.opendatakit.application.CommonApplication;
import org.opendatakit.listener.DatabaseConnectionListener;

/**
 * Created by Niles on 6/28/17.
 */

public abstract class AbsBaseActivity extends Activity
    implements DatabaseConnectionListener, IAppAwareActivity {

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
}
