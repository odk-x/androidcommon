/*
 * Copyright (C) 2012-2013 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.common.android.provider;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Environment;
import android.os.ParcelFileDescriptor;

/**
 * The WebKit does better if there is a content provider vending files to it.
 * This provider vends files under the Forms and Instances directories (only).
 * The url is of the form:
 *   content://org.opendatakit.common.android.provider.file/appname/forms/...
 *   content://org.opendatakit.common.android.provider.file/appname/instances/tableid/instanceid/...
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class FileProvider extends ContentProvider {
	private static final String FILE_AUTHORITY = "org.opendatakit.common.android.provider.file";
	private static final String FILE_URL_PREFIX = ContentResolver.SCHEME_CONTENT
			+ "://" + FileProvider.FILE_AUTHORITY;

	public static String getFileOriginString() {
		return ContentResolver.SCHEME_CONTENT + "_" + FILE_AUTHORITY
				+ "_0";
	}

	/**
	 * Directories at the application name level that are inaccessible.
	 * e.g., legacy ODK Collect directories.
	 */
	private static List<String> LEGACY_DIRECTORIES;
	static {
		LEGACY_DIRECTORIES = new ArrayList<String>();
		LEGACY_DIRECTORIES.add("forms");
		LEGACY_DIRECTORIES.add("instances");
		LEGACY_DIRECTORIES.add(".cache");
		LEGACY_DIRECTORIES.add("metadata");
		LEGACY_DIRECTORIES.add("config");
	}

	/**
	 * directories within an application that are inaccessible via the file provider.
	 */
	private static List<String> INACCESSIBLE_DIRECTORIES;
	static {
		INACCESSIBLE_DIRECTORIES = new ArrayList<String>();
		INACCESSIBLE_DIRECTORIES.add("metadata"); // where the database lives...
	}

	// Storage paths
	public static final String ODK_BASE_DIR = Environment
			.getExternalStorageDirectory()
			+ File.separator
			+ "odk";

	public static File getAsFile(String uriString) {
		Uri uri = Uri.parse(uriString);
		if ( !uri.getAuthority().equals(FILE_AUTHORITY) ) {
			throw new IllegalArgumentException("Not a valid uri: " + uriString);
		}
		List<String> segments = uri.getPathSegments();
		if ( segments.size() < 2 ) {
			throw new IllegalArgumentException("Not a valid uri: " + uriString + " application or subdirectory not specified.");
		}

		File f = new File(ODK_BASE_DIR, segments.get(0));
		// exclude LEGACY_DIRECTORIES...
		if ( !f.exists() || !f.isDirectory() || LEGACY_DIRECTORIES.contains(segments.get(0)) ) {
			throw new IllegalArgumentException("Not a valid uri: " + uriString + " invalid application.");
		}
		f = new File(f, segments.get(1));
		if ( !f.exists() || !f.isDirectory() || INACCESSIBLE_DIRECTORIES.contains(segments.get(1)) ) {
			throw new IllegalArgumentException("Not a valid uri: " + uriString + " invalid subdirectory.");
		}
		for ( int i = 2 ; i < segments.size() ; ++i ) {
			f = new File(f, segments.get(i));
		}
		if ( !f.exists() || f.isFile() ) {
			throw new IllegalArgumentException("Not a valid uri: " + uriString + " file does not exists or is not a valid file.");
		}
		return f;
	}

	/**
	 * The constructed URL may be invalid if it references a file that is in
	 * a legacy directory or an inaccessible directory.
	 *
	 * @param filePath
	 * @return Url that this content provider can process to return the file.
	 */
	public static String getAsUrl(File filePath) {

		String fullPath = filePath.getAbsolutePath();
		if (fullPath.startsWith(ODK_BASE_DIR)) {
			fullPath = fullPath.substring(ODK_BASE_DIR.length());
			fullPath = FILE_URL_PREFIX + fullPath;
		} else {
			throw new IllegalArgumentException("Invalid file access: "
					+ filePath.getAbsolutePath());
		}
		return fullPath;
	}

	@Override
	public ParcelFileDescriptor openFile(Uri uri, String mode)
			throws FileNotFoundException {
		String path = uri.getPath();

		File pathFile;
		if (uri.getAuthority().equalsIgnoreCase(FILE_AUTHORITY)) {
			pathFile = new File(ODK_BASE_DIR);
		} else {
			throw new FileNotFoundException("Not a valid uri: " + uri + " file does not exists or is not a file.");
		}

		File realFile = new File(pathFile, path);

		try {
			String parentPath = pathFile.getCanonicalPath();
			String fullPath = realFile.getCanonicalPath();
			if (!fullPath.startsWith(parentPath)) {
				throw new FileNotFoundException("Not a valid uri: " + uri + " canonical path violation: "
						+ realFile.getAbsolutePath());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileNotFoundException("Not a valid uri: " + uri + " canonical path violation: "
					+ realFile.getAbsolutePath());
		}

		if ( !realFile.isFile() ) {
			throw new FileNotFoundException("Not a valid uri: " + uri + " is not a file.");
		}
		return ParcelFileDescriptor.open(realFile,
				ParcelFileDescriptor.MODE_READ_ONLY);
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		return null;
	}

	@Override
	public boolean onCreate() {
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

}
