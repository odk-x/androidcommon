package org.opendatakit.activities;

import android.Manifest;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.ComponentName;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v13.app.ActivityCompat;
import android.widget.Toast;
import org.opendatakit.androidcommon.R;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.consts.RequestCodeConsts;
import org.opendatakit.dependencies.DependencyChecker;
import org.opendatakit.utilities.RuntimePermissionUtils;

import java.util.ArrayList;
import java.util.Collections;


public abstract class BaseLauncherActivity extends BaseActivity implements ActivityCompat.OnRequestPermissionsResultCallback {
  protected static final int REQUIRED_PERMISSIONS_REQ_CODE = 0;

  protected static final String[] REQUIRED_PERMISSIONS = new String[] {
      Manifest.permission.READ_EXTERNAL_STORAGE,
      Manifest.permission.WRITE_EXTERNAL_STORAGE
  };

  protected ArrayList<String> appSpecific_Required_Permissions = new ArrayList<String>();

  protected Bundle savedInstanceState;

  protected abstract void setAppSpecificPerms();

  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    this.savedInstanceState = savedInstanceState;

    if (!DependencyChecker.checkDependencies(this)) {
      return;
    }

    // 1. check if Services has the right permissions
    //      if not, launch Services
    // 2. check if this app has the right permissions

    if (!RuntimePermissionUtils.checkPackageAllPermission(
        this, IntentConsts.Services.PACKAGE_NAME, REQUIRED_PERMISSIONS)) {
      Intent launchIntent = new Intent();
      launchIntent.setComponent(
          new ComponentName(IntentConsts.Services.PACKAGE_NAME, IntentConsts.Services.MAIN_ACTIVITY));
      launchIntent.setAction(Intent.ACTION_VIEW);
      launchIntent.putExtra(IntentConsts.INTENT_KEY_PERMISSION_ONLY, true);

      startActivityForResult(launchIntent, RequestCodeConsts.RequestCodes.LAUNCH_MAIN_ACTIVITY);
    } else {
      setAppSpecificPerms();
      if (appSpecific_Required_Permissions.size() > 0) {
        for (String perm : REQUIRED_PERMISSIONS) {
          if (!appSpecific_Required_Permissions.contains(perm)) {
            appSpecific_Required_Permissions.add(perm);
          }
        }
      } else {
        Collections.addAll(appSpecific_Required_Permissions, REQUIRED_PERMISSIONS);
      }

      String[] appSpecPermArray = appSpecific_Required_Permissions.toArray(
              new String[appSpecific_Required_Permissions.size()]);
      if (!RuntimePermissionUtils.checkSelfAllPermission(this, appSpecPermArray)) {
        ActivityCompat.requestPermissions(
            this, appSpecPermArray, REQUIRED_PERMISSIONS_REQ_CODE);
      } else {
        onCreateWithPermission(savedInstanceState);
      }
    }
  }

  protected abstract void onCreateWithPermission(Bundle savedInstanceState);

  @Override
  public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
    super.onRequestPermissionsResult(requestCode, permissions, grantResults);

    if (requestCode != REQUIRED_PERMISSIONS_REQ_CODE) {
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

  @Override
  protected void onActivityResult(int requestCode, int resultCode, Intent data) {
    super.onActivityResult(requestCode, resultCode, data);

    if(requestCode != RequestCodeConsts.RequestCodes.LAUNCH_MAIN_ACTIVITY) {
      return;
    }

    if (resultCode != Activity.RESULT_OK) {
      System.exit(0); // cannot properly shutdown without Services having proper permissions
    }

    if (!RuntimePermissionUtils.checkSelfAllPermission(this, REQUIRED_PERMISSIONS)) {
      ActivityCompat.requestPermissions(
          this, REQUIRED_PERMISSIONS, REQUIRED_PERMISSIONS_REQ_CODE);
    } else {
      onCreateWithPermission(savedInstanceState);
    }
  }
}
