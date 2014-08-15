package org.opendatakit.sync.service;

import android.os.Parcel;
import android.os.Parcelable;

public enum SyncProgressState implements Parcelable {
  INIT, STARTING, APP_FILES, TABLE_FILES, ROWS, COMPLETE, ERROR;

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel dest, int flags) {
    dest.writeString(this.name());
  }

  public static final Parcelable.Creator<SyncProgressState> CREATOR = new Parcelable.Creator<SyncProgressState>() {
    public SyncProgressState createFromParcel(Parcel in) {
      return SyncProgressState.valueOf(in.readString());
    }

    public SyncProgressState[] newArray(int size) {
      return new SyncProgressState[size];
    }
  };

}