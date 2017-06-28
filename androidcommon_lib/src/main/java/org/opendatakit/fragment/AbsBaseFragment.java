package org.opendatakit.fragment;

import android.app.Activity;
import android.app.Application;
import android.app.Fragment;
import org.opendatakit.activities.BaseActivity;
import org.opendatakit.application.CommonApplication;
import org.opendatakit.listener.DatabaseConnectionListener;

/**
 * Created by Niles on 6/28/17.
 */

public abstract class AbsBaseFragment extends Fragment implements DatabaseConnectionListener {
  public BaseActivity getBaseActivity() {
    Activity act = getActivity();
    if (act instanceof BaseActivity) {
      return (BaseActivity) act;
    }
    throw new IllegalStateException("Bad activity");
  }
  public CommonApplication getCommonApplication() {
    Application app = getActivity().getApplication();
    if (app instanceof CommonApplication) {
      return (CommonApplication) app;
    }
    throw new IllegalStateException("Bad app");
  }
}
