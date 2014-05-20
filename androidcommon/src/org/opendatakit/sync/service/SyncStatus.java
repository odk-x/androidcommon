package org.opendatakit.sync.service;

import android.os.Parcel;
import android.os.Parcelable;

public enum SyncStatus implements Parcelable {
  INIT, SYNCING, NETWORK_ERROR, FILE_ERROR, AUTH_RESOLUTION, CONFLICT_RESOLUTION, SYNC_COMPLETE;

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel dest, int flags) {
    dest.writeString(this.name());
  }

  public static final Parcelable.Creator<SyncStatus> CREATOR = new Parcelable.Creator<SyncStatus>() {
    public SyncStatus createFromParcel(Parcel in) {
      return SyncStatus.valueOf(in.readString());
    }

    public SyncStatus[] newArray(int size) {
      return new SyncStatus[size];
    }
  };

}
