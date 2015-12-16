package org.opendatakit.common.android.utilities;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.content.pm.PackageManager;

import org.opendatakit.androidcommon.R;
import org.opendatakit.common.android.application.CommonApplication;

/**
 * This class checks for app dependencies, informing the user if any are missing
 *
 * @author marshallbradley93@gmail.com
 */
public class DependencyChecker {

    private static final String oiFileMgr = "org.openintents.filemanager";
    private static final String coreServices = "org.opendatakit.core";
    private static final String tables = "tables";
    private static final String scan = "scan";

    private final Activity activity;
    private final Context context;

    public DependencyChecker(Activity activity) {
        this.activity = activity;
        this.context = activity.getApplicationContext();
    }

    public boolean checkDependencies() {

        boolean oiInstalled;
        boolean coreInstalled;

        if (tables.equals(((CommonApplication)context).getToolName()) ||
            scan.equals(((CommonApplication)context).getToolName())) { // need to check
        // for OI and Core
            oiInstalled = isPackageInstalled(oiFileMgr);
        } else { // only need to check for Core
            oiInstalled = true;
        }

        coreInstalled = isPackageInstalled(coreServices);

        if (oiInstalled && coreInstalled) { // correct dependencies installed
            return true;
        } else {
            alertMissing(oiInstalled, coreInstalled); // missing dependencies, warn user
            return false;
        }
    }

    private void alertMissing(boolean oiInstalled, boolean coreInstalled) {

        String message;
        String title = context.getString(R.string.dependency_missing);

        if (oiInstalled && !coreInstalled) {
            message = context.getString(R.string.core_missing);
        } else if (!oiInstalled && coreInstalled) {
            message = context.getString(R.string.oi_missing);
        } else {
            message = context.getString(R.string.oi_and_core_missing);
            title = context.getString(R.string.dependencies_missing);
        }

        AlertDialog alert = buildAlert(title, message);
        alert.show();
    }

    private AlertDialog buildAlert(String title, String message) {
        AlertDialog.Builder builder = new AlertDialog.Builder(activity);
        builder.setTitle(title);
        builder.setMessage(message);
        builder.setCancelable(false);
        builder.setPositiveButton("OK",
                new DialogInterface.OnClickListener() {
                    @Override
                    public void onClick(DialogInterface dialog, int which) {
                        activity.finish();
                        System.exit(0);
                    }
                });

        return builder.create();
    }

    private boolean isPackageInstalled(String packageName) {
        PackageManager pm = context.getPackageManager();
        try {
            pm.getPackageInfo(packageName, PackageManager.GET_ACTIVITIES);
            return true;
        } catch (PackageManager.NameNotFoundException e) {
            return false;
        }
    }
}
