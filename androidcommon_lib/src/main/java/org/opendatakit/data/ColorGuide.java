/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.data;

/**
 * Class for interpreting the result of a test of the rule group. When this
 * is returned you are able to distinguish via the didMatch method
 * whether or not the rule should apply.
 * @author sudar.sam@gmail.com
 *
 */
public final class ColorGuide {

  private final int mForeground;
  private final int mBackground;

  public ColorGuide(int foreground, int background) {
    this.mForeground = foreground;
    this.mBackground = background;
  }

  public final int getForeground() {
    return mForeground;
  }

  public final int getBackground() {
    return mBackground;
  }
}