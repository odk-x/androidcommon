package org.opendatakit.common.android.utilities;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

public class ODKDataUtils {

  public static String genUUID() {
    return "uuid:" + UUID.randomUUID().toString();
  }

  public static String getLocalizedDisplayName(String displayName) {
    Locale locale = Locale.getDefault();
    String full_locale = locale.toString();
    int underscore = full_locale.indexOf('_');
    String lang_only_locale = (underscore == -1)
        ? full_locale : full_locale.substring(0, underscore);
    
    if (displayName.startsWith("\"") && displayName.endsWith("\"")) {
      return displayName.substring(1, displayName.length() - 1);
    } else if (displayName.startsWith("{") && displayName.endsWith("}")) {
      try {
        Map<String, Object> localeMap = ODKFileUtils.mapper.readValue(displayName, Map.class);
        String candidate = (String) localeMap.get(full_locale);
        if (candidate != null) {
          return candidate;
        }
        candidate = (String) localeMap.get(lang_only_locale);
        if (candidate != null) {
          return candidate;
        }
        candidate = (String) localeMap.get("default");
        if (candidate != null) {
          return candidate;
        }
        return null;
      } catch (JsonParseException e) {
        e.printStackTrace();
        throw new IllegalStateException("bad displayName: " + displayName);
      } catch (JsonMappingException e) {
        e.printStackTrace();
        throw new IllegalStateException("bad displayName: " + displayName);
      } catch (IOException e) {
        e.printStackTrace();
        throw new IllegalStateException("bad displayName: " + displayName);
      }
    } else {
      throw new IllegalStateException("bad displayName: " + displayName);
    }
  }

}
