/*
 * Copyright (C) 2017 University of Washington
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

package org.opendatakit.views;

import android.os.Parcel;
import android.os.Parcelable;

/**
 * Created by jbeorse on 5/4/17.
 */

public class ViewDataQueryParams implements Parcelable {
   String tableId;
   String rowId;
   String whereClause;
   String[] selectionArgs;
   String[] groupBy;
   String having;
   String orderByElemKey;
   String orderByDir;

   public ViewDataQueryParams(String tableId, String rowId, String whereClause, String[]
       selectionArgs, String[] groupBy, String having, String orderByElemKey, String orderByDir) {
      this.tableId = tableId;
      this.rowId = rowId;
      this.whereClause = whereClause;
      this.selectionArgs = selectionArgs;
      this.groupBy = groupBy;
      this.having = having;
      this.orderByElemKey = orderByElemKey;
      this.orderByDir = orderByDir;
   }

   public ViewDataQueryParams(Parcel in) {
      this.tableId = readStringFromParcel(in);
      this.rowId = readStringFromParcel(in);
      this.whereClause = readStringFromParcel(in);
      this.selectionArgs = readStringArrFromParcel(in);
      this.groupBy = readStringArrFromParcel(in);
      this.having = readStringFromParcel(in);
      this.orderByElemKey = readStringFromParcel(in);
      this.orderByDir = readStringFromParcel(in);
   }

   public boolean isSingleRowQuery() {
      return (rowId != null && !rowId.isEmpty());
   }

   @Override
   public int describeContents() {
      return 0;
   }

   @Override
   public void writeToParcel(Parcel dest, int flags) {
      writeStringToParcel(dest, tableId);
      writeStringToParcel(dest, rowId);
      writeStringToParcel(dest, whereClause);
      writeStringArrToParcel(dest, selectionArgs);
      writeStringArrToParcel(dest, groupBy);
      writeStringToParcel(dest, having);
      writeStringToParcel(dest, orderByElemKey);
      writeStringToParcel(dest, orderByDir);
   }

   private void writeStringToParcel(Parcel p, String s) {
      p.writeByte((byte)(s != null ? 1 : 0));
      if (s != null) p.writeString(s);
   }

   private void writeStringArrToParcel(Parcel p, String[] s) {
      if (s == null || s.length == 0) {
         p.writeInt(0);
      } else {
         p.writeInt(s.length);
         p.writeStringArray(s);
      }
   }

   private String readStringFromParcel(Parcel p) {
      boolean isPresent = p.readByte() == 1;
      return isPresent ? p.readString() : null;
   }

   private String[] readStringArrFromParcel(Parcel p) {
      int len = p.readInt();
      if (len <= 0) {
         return null;
      }

      String[] ret = new String[len];
      p.readStringArray(ret);
      return ret;
   }

   public static final Parcelable.Creator<ViewDataQueryParams> CREATOR =
       new Parcelable.Creator<ViewDataQueryParams>() {
          public ViewDataQueryParams createFromParcel(Parcel in) {
             return new ViewDataQueryParams(in);
          }

          public ViewDataQueryParams[] newArray(int size) {
             return new ViewDataQueryParams[size];
          }
       };
}
