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
 * It treats tables and scan as depending on both services and oi file manager. Everything else
 * it treats as depending on just services.
 * WARNING!
 * If the required dependencies are not installed, it finish()'s the view and calls System.exit(0).
 *
 * Doesn't use a DialogFragment so it might break if the user rotates the screen. That's a TODO
 *
 * @author marshallbradley93@gmail.com
 */
public class DependencyChecker {

    public static final String surveyAppPkgName = "org.opendatakit.survey";
    private static final String oiFileMgrPkgName = "org.openintents.filemanager";
    private static final String servicesAppPkgName = "org.opendatakit.services";

    private static final String tables = "tables";
    private static final String scan = "scan";

    private DependencyChecker() {
    }

    public static boolean checkDependencies(Activity activity) {

        Context context = activity.getApplicationContext();

        boolean oiInstalled;
        boolean servicesInstalled;

        if (tables.equals(((CommonApplication)context).getToolName()) ||
            scan.equals(((CommonApplication)context).getToolName())) { // need to check
        // for OI and Services
            oiInstalled = isPackageInstalled(context, oiFileMgrPkgName);
        } else { // only need to check for Services
            oiInstalled = true;
        }

        servicesInstalled = isPackageInstalled(context, servicesAppPkgName);

        if (oiInstalled && servicesInstalled) { // correct dependencies installed
            return true;
        } else {
            alertMissing(oiInstalled, servicesInstalled, context, activity); // missing
            // dependencies, warn
            // user
            return false;
        }
    }

    private static void alertMissing(boolean oiInstalled, boolean servicesInstalled, Context
        context, Activity activity) {

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

        AlertDialog alert = buildAlert(title, message, activity);
        alert.show();
    }

    private static AlertDialog buildAlert(String title, String message, final Activity activity) {
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
