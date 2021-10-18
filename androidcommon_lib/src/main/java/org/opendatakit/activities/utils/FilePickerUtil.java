package org.opendatakit.activities.utils;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.provider.DocumentsContract;


public final class FilePickerUtil {
    public static int FILE_PICKER_CODE = 384;

    public static Intent createFilePickerIntent(String title, String fileTypes, String startingDirectory) {
        Intent intent = new Intent(Intent.ACTION_OPEN_DOCUMENT);
        if (title != null) {
            intent.putExtra(Intent.EXTRA_TITLE, title);
        }
        if (startingDirectory != null) {
            intent.putExtra(DocumentsContract.EXTRA_INITIAL_URI, startingDirectory);
        }
        intent.setType(fileTypes);
        intent.addCategory(Intent.CATEGORY_OPENABLE);
        return intent;
    }

    public static Intent createFilePickerIntent(String fileTypes) {
        return FilePickerUtil.createFilePickerIntent(null, fileTypes, null);
    }

    public static boolean isSuccessfulFilePickerResponse(int requestCode, int resultCode) {
        return (requestCode == FILE_PICKER_CODE && resultCode == Activity.RESULT_OK);
    }

    public static Uri getUri(Intent data) {
        Uri fileUri = null;
        if(data != null) {
            fileUri = data.getData();
        }
        return fileUri;
    }

}
