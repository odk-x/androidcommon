# androidcommon

This project is __*actively maintained*__

It is part of the ODK 2.0 Android tools suite.

This is a library APK used by all the ODK 2.0 Android tools.

The developer [wiki](https://github.com/opendatakit/opendatakit/wiki) (including release notes) and
[issues tracker](https://github.com/opendatakit/opendatakit/issues) are located under
the [**opendatakit**](https://github.com/opendatakit/opendatakit) project.

The Google group for software engineering questions is: [opendatakit-developers@](https://groups.google.com/forum/#!forum/opendatakit-developers)

## Setting up your environment and building the project

General instructions for setting up an ODK 2.0 environment can be found at our [DevEnv Setup wiki page](https://github.com/opendatakit/opendatakit/wiki/DevEnv-Setup)

Install [Android Studio](http://developer.android.com/tools/studio/index.html) and the [SDK](http://developer.android.com/sdk/index.html#Other).

This project depends on the ODK [androidlibrary](https://github.com/opendatakit/androidlibrary) project; its binaries will be downloaded automatically fom our maven repository during the build phase. If you wish to modify that project yourself, you must clone it into the same parent directory as androidcommon. You directory stucture should resemble the following:

        |-- odk

            |-- androidcommon

            |-- androidlibrary


  * Note that this only applies if you are modifying androidlibrary. If you use the maven dependencies (the default option), the project will not show up in your directory. 

Open the androidcommon project directory in Android Studio.

Now you should be ready to build, by selecting `Build->Make Project`.

## Running

**NOTE** this project will NOT run on an Android device by itself, it is simply a library for use in other ODK projects.
