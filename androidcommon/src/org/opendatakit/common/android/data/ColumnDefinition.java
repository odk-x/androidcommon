package org.opendatakit.common.android.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.utilities.NameUtil;
import org.opendatakit.common.android.utilities.ODKFileUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class ColumnDefinition {
  private static final String JSON_SCHEMA_ELEMENT_KEY = "elementKey";
  private static final String JSON_SCHEMA_ELEMENT_TYPE = "elementType";
  private static final String JSON_SCHEMA_PROPERTIES = "properties";
  private static final String JSON_SCHEMA_ITEMS = "items";
  private static final String JSON_SCHEMA_TYPE = "type";

  private final Column column;

  // public final String elementKey;
  // public final String elementName;
  // public final String elementType;
  private boolean isUnitOfRetention = true; // assumed until revised...

  final ArrayList<ColumnDefinition> children = new ArrayList<ColumnDefinition>();
  ColumnDefinition parent = null;

  public ColumnDefinition(String elementKey, String elementName, String elementType,
      String listChildElementKeys) {
    this.column = new Column(elementKey, elementName, elementType, listChildElementKeys);
  }
  
  public String getElementKey() {
    return column.getElementKey();
  }

  public String getElementName() {
    return column.getElementName();
  }

  public String getElementType() {
    return column.getElementType();
  }

  public String getListChildElementKeys() {
    return column.getListChildElementKeys();
  }

  private void setParent(ColumnDefinition parent) {
    this.parent = parent;
  }
  
  public ColumnDefinition getParent() {
    return this.parent;
  }

  public void addChild(ColumnDefinition child) {
    child.setParent(this);
    children.add(child);
  }
  
  public List<ColumnDefinition> getChildren() {
    return Collections.unmodifiableList(this.children);
  }

  public boolean isUnitOfRetention() {
    return isUnitOfRetention;
  }
  
  void setNotUnitOfRetention() {
    isUnitOfRetention = false;
  }

  public String toString() {
    return column.toString();
  }

  public int hashCode() {
    return column.hashCode();
  }

  public boolean equals(Object obj) {
    if ( obj == null || !(obj instanceof ColumnDefinition) ) {
      return false;
    }
    ColumnDefinition o = (ColumnDefinition) obj;
    
    return column.equals(o.column);
  }

  /**
   * Binary search using elementKey. The ColumnDefinition
   * list returned by ColumnDefinition.buildColumnDefinitions()
   * is ordered. This function makes use of that property to 
   * quickly retrieve the definition for an elementKey.
   * 
   * @param orderedDefns
   * @param columnName
   * @return
   */
  public static ColumnDefinition find(List<ColumnDefinition> orderedDefns, String columnName) {
    int iLow = 0;
    int iHigh = orderedDefns.size();
    int iGuess = (iLow + iHigh)/2;
    while ( iLow != iHigh ) {
      ColumnDefinition cd = orderedDefns.get(iGuess);
      int cmp = cd.getElementKey().compareTo(columnName);
      if ( cmp == 0 ) {
        return cd;
      }
      if ( cmp < 0 ) {
        iHigh = iGuess;
      } else {
        iLow = iGuess+1;
      }
      iGuess = (iLow + iHigh)/2;
    }
    
    if ( iLow >= orderedDefns.size() ) {
      throw new IllegalStateException("could not find elementKey in columns list: " + columnName);
    }
    
    ColumnDefinition cd = orderedDefns.get(iGuess);
    if ( cd.getElementKey().equals(columnName) ) {
      return cd;
    }
    throw new IllegalStateException("could not find elementKey in columns list: " + columnName);
  }

  /**
   * Helper class for building ColumnDefinition objects from Column objects.
   * 
   * @author mitchellsundt@gmail.com
   */
  private static class ColumnContainer {
    public ColumnDefinition defn = null;
    public ArrayList<String> children = null;
  };

  /**
   * Construct the rich ColumnDefinition objects for a table from the 
   * underlying information in the list of Column objects.
   * 
   * @param columns
   * @return
   */
  public static final List<ColumnDefinition> buildColumnDefinitions(List<Column> columns) {

    Map<String, ColumnDefinition> colDefs = new HashMap<String, ColumnDefinition>();
    List<ColumnContainer> ccList = new ArrayList<ColumnContainer>();
    for ( Column col : columns ) {
      if ( !NameUtil.isValidUserDefinedDatabaseName(col.getElementKey()) ) {
        throw new IllegalArgumentException("ColumnDefinition: invalid user-defined column name: " + col.getElementKey());
      }
      ColumnDefinition cd = new ColumnDefinition(col.getElementKey(), col.getElementName(), col.getElementType(), col.getListChildElementKeys());
      ColumnContainer cc = new ColumnContainer();
      cc.defn = cd;
      String children = col.getListChildElementKeys();
      if ( children != null && children.length() != 0 ) {
        ArrayList<String> chi;
        try {
          chi = ODKFileUtils.mapper.readValue(children, ArrayList.class);
        } catch (JsonParseException e) {
          e.printStackTrace();
          throw new IllegalArgumentException("Invalid list of children: " + children);
        } catch (JsonMappingException e) {
          e.printStackTrace();
          throw new IllegalArgumentException("Invalid list of children: " + children);
        } catch (IOException e) {
          e.printStackTrace();
          throw new IllegalArgumentException("Invalid list of children: " + children);
        }
        cc.children = chi;
        ccList.add(cc);
      }
      colDefs.put(cd.getElementKey(), cd);
    }
    for ( ColumnContainer cc : ccList ) {
      ColumnDefinition cparent = cc.defn;
      for ( String childKey : cc.children ) {
        ColumnDefinition cchild = colDefs.get(childKey);
        if ( cchild == null ) {
          throw new IllegalArgumentException("Child elementkey " + childKey + " was never defined but referenced in " + cparent.getElementKey() + "!");
        }
        // set up bi-directional linkage of child and parent.
        cparent.addChild(cchild);
      }
    }
    
    // Sanity check:
    // (1) all children elementKeys must have been defined in the Columns list.
    // (2) arrays must have only one child.
    // (3) children must belong to at most one parent
    for ( ColumnContainer cc : ccList) {
      ColumnDefinition defn = cc.defn;

      if ( defn.getChildren().size() != cc.children.size() ) {
        throw new IllegalArgumentException("Not all children of element have been defined! " + defn.getElementKey());
      }
      
      ElementType type = ElementType.parseElementType(defn.getElementType(),
          !defn.getChildren().isEmpty());
      
      if ( type.getDataType() == ElementDataType.array ) {
        if ( defn.getChildren().isEmpty() ) {
          throw new IllegalArgumentException("Column is an array but does not list its children");
        }
        if ( defn.getChildren().size() != 1 ) {
          throw new IllegalArgumentException("Column is an array but has more than one item entry");
        }
      }
      
      for ( ColumnDefinition child : defn.getChildren() ) {
        if ( child.getParent() != defn ) {
          throw new IllegalArgumentException("Column is enclosed by two or more groupings: " + defn.getElementKey());
        }
        if ( !child.getElementKey().equals(defn.getElementKey() + "_" + child.getElementName()) ) {
          throw new IllegalArgumentException("Children are expected to have elementKey equal to parent's elementKey-underscore-childElementName");
        }
      }
    }
    markUnitOfRetention(colDefs);
    List<ColumnDefinition> defns = new ArrayList<ColumnDefinition>(colDefs.values());
    Collections.sort(defns, new Comparator<ColumnDefinition>(){

      @Override
      public int compare(ColumnDefinition lhs, ColumnDefinition rhs) {
        return lhs.getElementKey().compareTo(rhs.getElementKey());
      }});
    
    return defns;
  }

  /**
   *  This must match the code in the javascript layer
   *  
   *  See databaseUtils.markUnitOfRetention
   *  
   *  Sweeps through the collection of ColumnDefinition objects and marks
   *  the ones that exist in the actual database table.
   *  
   * @param defn
   */
  private static void markUnitOfRetention(Map<String, ColumnDefinition> defn) {
    // for all arrays, mark all descendants of the array as not-retained
    // because they are all folded up into the json representation of the array
    for ( String startKey : defn.keySet() ) {
      ColumnDefinition colDefn = defn.get(startKey);
      if ( !colDefn.isUnitOfRetention() ) {
        // this has already been processed
        continue;
      }
      if ( ElementDataType.array.name().equals(colDefn.getElementType()) ) {
        ArrayList<ColumnDefinition> descendantsOfArray = new ArrayList<ColumnDefinition>(colDefn.getChildren());
        ArrayList<ColumnDefinition> scratchArray = new ArrayList<ColumnDefinition>();
        while ( !descendantsOfArray.isEmpty() ) {
            for ( ColumnDefinition subDefn : descendantsOfArray ) {
              if ( !subDefn.isUnitOfRetention() ) {
                  // this has already been processed
                  continue;
              }
              subDefn.setNotUnitOfRetention();
              scratchArray.addAll(subDefn.getChildren());
            }
            descendantsOfArray = scratchArray;
        }
      }
    }
    // and mark any non-arrays with multiple fields as not retained
    for ( String startKey : defn.keySet() ) {
      ColumnDefinition colDefn = defn.get(startKey);
      if ( !colDefn.isUnitOfRetention() ) {
          // this has already been processed
          continue;
      }
      if ( ! ElementDataType.array.name().equals(colDefn.getElementType()) ) {
        if ( !colDefn.getChildren().isEmpty() ) {
          colDefn.setNotUnitOfRetention();
        }
      }
    }
  }

  /**
   * Covert the ColumnDefinition map into a JSON schema.
   *
   * @param defns
   * @return
   */
  public static TreeMap<String, Object> getDataModel(List<ColumnDefinition> orderedDefns) {
    TreeMap<String, Object> model = new TreeMap<String, Object>();

    for (ColumnDefinition c : orderedDefns) {
      if (c.getParent() == null) {
        model.put(c.getElementName(), new TreeMap<String, Object>());
        @SuppressWarnings("unchecked")
        TreeMap<String, Object> jsonSchema = (TreeMap<String, Object>) model.get(c.getElementName());
        getDataModelHelper(jsonSchema, c);
      }
    }
    return model;
  }

  private static void getDataModelHelper(TreeMap<String, Object> jsonSchema, ColumnDefinition c) {
    ElementType type = ElementType.parseElementType(c.getElementType(), !c.getChildren().isEmpty());
    ElementDataType dataType = type.getDataType();
    
    if (dataType == ElementDataType.array) {
      jsonSchema.put(JSON_SCHEMA_TYPE, dataType.name());
      if (!c.getElementType().equals(dataType.name())) {
        jsonSchema.put(JSON_SCHEMA_ELEMENT_TYPE, c.getElementType());
      }
      jsonSchema.put(JSON_SCHEMA_ELEMENT_KEY, c.getElementKey());
      ColumnDefinition ch = c.getChildren().get(0);
      jsonSchema.put(JSON_SCHEMA_ITEMS, new TreeMap<String, Object>());
      @SuppressWarnings("unchecked")
      TreeMap<String, Object> itemSchema = (TreeMap<String, Object>) jsonSchema.get(JSON_SCHEMA_ITEMS);
      getDataModelHelper(itemSchema, ch); // recursion...
    } else if (dataType == ElementDataType.bool) {
      jsonSchema.put(JSON_SCHEMA_TYPE, dataType.name());
      if (!c.getElementType().equals(dataType.name())) {
        jsonSchema.put(JSON_SCHEMA_ELEMENT_TYPE, c.getElementType());
      }
      jsonSchema.put(JSON_SCHEMA_ELEMENT_KEY, c.getElementKey());
    } else if (dataType == ElementDataType.configpath) {
      jsonSchema.put(JSON_SCHEMA_TYPE, ElementDataType.string.name());
      jsonSchema.put(JSON_SCHEMA_ELEMENT_TYPE, c.getElementType());
      jsonSchema.put(JSON_SCHEMA_ELEMENT_KEY, c.getElementKey());
    } else if (dataType == ElementDataType.integer) {
      jsonSchema.put(JSON_SCHEMA_TYPE, dataType.name());
      if (!c.getElementType().equals(dataType.name())) {
        jsonSchema.put(JSON_SCHEMA_ELEMENT_TYPE, c.getElementType());
      }
      jsonSchema.put(JSON_SCHEMA_ELEMENT_KEY, c.getElementKey());
    } else if (dataType == ElementDataType.number) {
      jsonSchema.put(JSON_SCHEMA_TYPE, dataType.name());
      if (!c.getElementType().equals(dataType.name())) {
        jsonSchema.put(JSON_SCHEMA_ELEMENT_TYPE, c.getElementType());
      }
      jsonSchema.put(JSON_SCHEMA_ELEMENT_KEY, c.getElementKey());
    } else if (dataType == ElementDataType.object) {
      jsonSchema.put(JSON_SCHEMA_TYPE, dataType.name());
      if (!c.getElementType().equals(dataType.name())) {
        jsonSchema.put(JSON_SCHEMA_ELEMENT_TYPE, c.getElementType());
      }
      jsonSchema.put(JSON_SCHEMA_ELEMENT_KEY, c.getElementKey());
      jsonSchema.put(JSON_SCHEMA_PROPERTIES, new TreeMap<String, Object>());
      @SuppressWarnings("unchecked")
      TreeMap<String, Object> propertiesSchema = (TreeMap<String, Object>) jsonSchema
          .get(JSON_SCHEMA_PROPERTIES);
      for (ColumnDefinition ch : c.getChildren()) {
        propertiesSchema.put(c.getElementName(), new TreeMap<String, Object>());
        @SuppressWarnings("unchecked")
        TreeMap<String, Object> itemSchema = (TreeMap<String, Object>) propertiesSchema
            .get(c.getElementName());
        getDataModelHelper(itemSchema, ch); // recursion...
      }
    } else if (dataType == ElementDataType.rowpath) {
      jsonSchema.put(JSON_SCHEMA_TYPE, ElementDataType.string.name());
      jsonSchema.put(JSON_SCHEMA_ELEMENT_TYPE, ElementDataType.rowpath.name());
      jsonSchema.put(JSON_SCHEMA_ELEMENT_KEY, c.getElementKey());
    } else if (dataType == ElementDataType.string) {
      jsonSchema.put(JSON_SCHEMA_TYPE, ElementDataType.string.name());
      if (!c.getElementType().equals(dataType.name())) {
        jsonSchema.put(JSON_SCHEMA_ELEMENT_TYPE, c.getElementType());
      }
      jsonSchema.put(JSON_SCHEMA_ELEMENT_KEY, c.getElementKey());
    } else {
      throw new IllegalStateException("unexpected alternative ElementDataType");
    }
  }

}