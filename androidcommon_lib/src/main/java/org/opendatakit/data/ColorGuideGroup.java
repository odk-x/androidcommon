package org.opendatakit.data;

import org.opendatakit.database.data.Row;
import org.opendatakit.database.data.UserTable;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by clarice on 3/22/16.
 */
public class ColorGuideGroup {

  private Map<String, ColorGuide> mRowIdToColors = new TreeMap<String, ColorGuide>();
  private ColorRuleGroup mCRG;
  private UserTable mUT;

  public ColorGuideGroup(ColorRuleGroup crg, UserTable ut) {
    if (crg == null) {
      return;
    }

    mCRG = crg;

    if (ut == null) {
      return;
    }

    mUT = ut;

    for (int i = 0; i < mUT.getNumberOfRows(); i++) {
      ColorGuide tcg = mCRG.getColorGuide(mUT.getColumnDefinitions(), mUT.getRowAtIndex(i));
      mRowIdToColors.put(mUT.getRowId(i), tcg);
    }

  }

  public Map<String, ColorGuide> getAllColorGuides() {
    return mRowIdToColors;
  }

  public ColorGuide getColorGuideForRowIndex(int i) {
    ColorGuide cg = null;

    Row colorRow = null;

    try {
      colorRow = mUT.getRowAtIndex(i);
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    }

    if (colorRow != null && mRowIdToColors != null) {
      if (mRowIdToColors.containsKey(mUT.getRowId(i))) {
        cg = mRowIdToColors.get(mUT.getRowId(i));
      }
    }
    return cg;
  }

  public ColorGuide getColorGuideForRowId(String rowId) {
    ColorGuide cg = null;

    if (mRowIdToColors != null) {
      if (mRowIdToColors.containsKey(rowId)) {
        cg = mRowIdToColors.get(rowId);
      }
    }
    return cg;
  }

}

