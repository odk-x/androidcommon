package org.opendatakit.common.android.data;

/**
 * Created by clarice on 9/29/15.
 */
public final class RowColorObject {

  private final String mRowId;
  private final int mRowIndex;
  private final String mHexForeground;
  private final String mHexBackground;

  public RowColorObject(String rowId, int rowIndex, String foreground, String background) {

    this.mRowId = rowId;
    this.mRowIndex = rowIndex;
    this.mHexForeground = foreground;
    this.mHexBackground = background;
  }

  public final String getRowId() { return mRowId; }

  public final int getRowIndex() { return mRowIndex; }

  public final String getForegroundColor() {
    return mHexForeground;
  }

  public final String getBackgroundColor() {
    return mHexBackground;
  }

}
