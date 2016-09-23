package org.opendatakit.dependencies;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.content.pm.PackageManager;

import org.opendatakit.androidcommon.R;
import org.opendatakit.application.CommonApplication;

/**
 * This class checks for app dependencies, informing the user if any are missing
 *
 * @author marshallbradley93@gmail.com
 */
public class DependencyChecker {

    public static final String surveyAppPkgName = "org.opendatakit.survey";
    public static final String collectAppPkgName = "org.odk.collect.android";

    private static final String oiFileMgr = "org.openintents.filemanager";
    private static final String services = "org.opendatakit.services";
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
        boolean servicesInstalled;

        if (tables.equals(((CommonApplication)context).getToolName()) ||
            scan.equals(((CommonApplication)context).getToolName())) { // need to check
        // for OI and Services
            oiInstalled = isPackageInstalled(context, oiFileMgr);
        } else { // only need to check for Services
            oiInstalled = true;
        }

        servicesInstalled = isPackageInstalled(context, services);

        if (oiInstalled && servicesInstalled) { // correct dependencies installed
            return true;
        } else {
            alertMissing(oiInstalled, servicesInstalled); // missing dependencies, warn user
            return false;
        }
    }

    private void alertMissing(boolean oiInstalled, boolean servicesInstalled) {

        String message;
        String title = context.getString(R.string.dependency_missing);

        if (oiInstalled && !servicesInstalled) {
            message = context.getString(R.string.services_missing);
        } else if (!oiInstalled && servicesInstalled) {
            message = context.getString(R.string.oi_missing);
        } else {
            message = context.getString(R.string.oi_and_services_missing);
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

    public static boolean isPackageInstalled(Context context, String packageName) {
        PackageManager pm = context.getPackageManager();
        try {
            pm.getPackageInfo(packageName, PackageManager.GET_ACTIVITIES);
            return true;
        } catch (PackageManager.NameNotFoundException e) {
            return false;
        }
    }
}
