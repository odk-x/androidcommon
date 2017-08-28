package org.opendatakit.activities;

import android.Manifest;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v13.app.ActivityCompat;
import android.support.v13.app.FragmentCompat;
import android.widget.Toast;

import org.opendatakit.androidcommon.R;
import org.opendatakit.utilities.RuntimePermissionUtils;


public abstract class BaseLauncherActivity extends BaseActivity implements FragmentCompat.OnRequestPermissionsResultCallback {
  protected static final int REQUIRED_PERMISSIONS_REQ_CORE = 0;
  protected static final String[] REQUIRED_PERMISSIONS = new String[] {
      Manifest.permission.WRITE_EXTERNAL_STORAGE
  };

  protected Bundle savedInstanceState;

  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    this.savedInstanceState = savedInstanceState;

    if (!RuntimePermissionUtils.checkSelfAnyPermission(this, REQUIRED_PERMISSIONS)) {
      ActivityCompat.requestPermissions(
          this, REQUIRED_PERMISSIONS, REQUIRED_PERMISSIONS_REQ_CORE);
    } else {
      onCreateWithPermission(savedInstanceState);
    }
  }

  protected abstract void onCreateWithPermission(Bundle savedInstanceState);

  @Override
  public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
    super.onRequestPermissionsResult(requestCode, permissions, grantResults);

    if (requestCode != REQUIRED_PERMISSIONS_REQ_CORE) {
      return;
    }

    AlertDialog.Builder builder =
        RuntimePermissionUtils.createPermissionRationaleDialog(this, requestCode, permissions);

    if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
      onCreateWithPermission(savedInstanceState);
    } else {
      if (RuntimePermissionUtils.shouldShowAnyPermissionRationale(this, permissions)) {
        builder
            .setMessage(R.string.required_permission_rationale)
            .setNegativeButton(R.string.cancel, new DialogInterface.OnClickListener() {
              @Override
              public void onClick(DialogInterface dialog, int which) {
                dialog.cancel();
                finish();
              }
            })
            .show();
      } else {
        Toast
            .makeText(this, R.string.required_permission_perm_denied, Toast.LENGTH_LONG)
            .show();
        finish();
      }
    }
  }
}
