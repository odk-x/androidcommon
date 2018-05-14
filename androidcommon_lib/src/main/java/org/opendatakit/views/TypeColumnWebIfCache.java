package org.opendatakit.views;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.provider.DataTableColumns;

import java.util.HashMap;

public class TypeColumnWebIfCache {
   HashMap<String, Class> nameCache;
   HashMap<Integer, Class> indexCache;

   public TypeColumnWebIfCache(OrderedColumns orderedColumns, BaseTable baseTable) {
      nameCache = new HashMap<String, Class>();
      indexCache = new HashMap<Integer, Class>();

      if (orderedColumns == null || baseTable == null) {
         return;
      }

      // remember to keep in sync with TypedRow function getColumnDataType
      for (int index = 0; index < baseTable.getWidth(); index++) {
         String key = baseTable.getElementKey(index);
         try {
            ColumnDefinition columnDefinition = orderedColumns.find(key);
            ElementDataType dataType = columnDefinition.getType().getDataType();
            Class clazz = TypedRow.getOdkDataWebIfType(dataType);
            nameCache.put(key, clazz);
            indexCache.put(index, clazz);

         } catch (IllegalArgumentException ile) {
            // Logic for the admin columns, all but CONFLICT_TYPE are strings
            // AND also defaults to string in case usage
            // of alias column names being return
            if (DataTableColumns.CONFLICT_TYPE.equals(key)) {
               nameCache.put(DataTableColumns.CONFLICT_TYPE, Integer.class);
               indexCache.put(index, Integer.class);
            } else {
               nameCache.put(key, String.class);
               indexCache.put(index, String.class);
            }
         }
      }
   }

   Object getOdkDataWebIfDataByIndex(int i, TypedRow row) {
      if (indexCache.containsKey(i)) {
         return row.getDataType(i, this.indexCache.get(i));
      } else {
         return row.getDataType(i, String.class);
      }
   }

   Object getOdkDataWebIfDataByKey(String key, TypedRow row) {
      if (nameCache.containsKey(key)) {
         return row.getDataType(key, this.nameCache.get(key));
      } else {
         return row.getDataType(key, String.class);
      }
   }
}
