/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class CompactionRequest implements org.apache.thrift.TBase<CompactionRequest, CompactionRequest._Fields>, java.io.Serializable, Cloneable, Comparable<CompactionRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("CompactionRequest");

  private static final org.apache.thrift.protocol.TField DBNAME_FIELD_DESC = new org.apache.thrift.protocol.TField("dbname", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TABLENAME_FIELD_DESC = new org.apache.thrift.protocol.TField("tablename", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField PARTITIONNAME_FIELD_DESC = new org.apache.thrift.protocol.TField("partitionname", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("type", org.apache.thrift.protocol.TType.I32, (short)4);
  private static final org.apache.thrift.protocol.TField RUNAS_FIELD_DESC = new org.apache.thrift.protocol.TField("runas", org.apache.thrift.protocol.TType.STRING, (short)5);
  private static final org.apache.thrift.protocol.TField PROPERTIES_FIELD_DESC = new org.apache.thrift.protocol.TField("properties", org.apache.thrift.protocol.TType.MAP, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CompactionRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CompactionRequestTupleSchemeFactory());
  }

  private String dbname; // required
  private String tablename; // required
  private String partitionname; // optional
  private CompactionType type; // required
  private String runas; // optional
  private Map<String,String> properties; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DBNAME((short)1, "dbname"),
    TABLENAME((short)2, "tablename"),
    PARTITIONNAME((short)3, "partitionname"),
    /**
     * 
     * @see CompactionType
     */
    TYPE((short)4, "type"),
    RUNAS((short)5, "runas"),
    PROPERTIES((short)6, "properties");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // DBNAME
          return DBNAME;
        case 2: // TABLENAME
          return TABLENAME;
        case 3: // PARTITIONNAME
          return PARTITIONNAME;
        case 4: // TYPE
          return TYPE;
        case 5: // RUNAS
          return RUNAS;
        case 6: // PROPERTIES
          return PROPERTIES;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.PARTITIONNAME,_Fields.RUNAS,_Fields.PROPERTIES};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.DBNAME, new org.apache.thrift.meta_data.FieldMetaData("dbname", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TABLENAME, new org.apache.thrift.meta_data.FieldMetaData("tablename", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PARTITIONNAME, new org.apache.thrift.meta_data.FieldMetaData("partitionname", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData("type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, CompactionType.class)));
    tmpMap.put(_Fields.RUNAS, new org.apache.thrift.meta_data.FieldMetaData("runas", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PROPERTIES, new org.apache.thrift.meta_data.FieldMetaData("properties", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(CompactionRequest.class, metaDataMap);
  }

  public CompactionRequest() {
  }

  public CompactionRequest(
    String dbname,
    String tablename,
    CompactionType type)
  {
    this();
    this.dbname = dbname;
    this.tablename = tablename;
    this.type = type;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public CompactionRequest(CompactionRequest other) {
    if (other.isSetDbname()) {
      this.dbname = other.dbname;
    }
    if (other.isSetTablename()) {
      this.tablename = other.tablename;
    }
    if (other.isSetPartitionname()) {
      this.partitionname = other.partitionname;
    }
    if (other.isSetType()) {
      this.type = other.type;
    }
    if (other.isSetRunas()) {
      this.runas = other.runas;
    }
    if (other.isSetProperties()) {
      Map<String,String> __this__properties = new HashMap<String,String>(other.properties);
      this.properties = __this__properties;
    }
  }

  public CompactionRequest deepCopy() {
    return new CompactionRequest(this);
  }

  @Override
  public void clear() {
    this.dbname = null;
    this.tablename = null;
    this.partitionname = null;
    this.type = null;
    this.runas = null;
    this.properties = null;
  }

  public String getDbname() {
    return this.dbname;
  }

  public void setDbname(String dbname) {
    this.dbname = dbname;
  }

  public void unsetDbname() {
    this.dbname = null;
  }

  /** Returns true if field dbname is set (has been assigned a value) and false otherwise */
  public boolean isSetDbname() {
    return this.dbname != null;
  }

  public void setDbnameIsSet(boolean value) {
    if (!value) {
      this.dbname = null;
    }
  }

  public String getTablename() {
    return this.tablename;
  }

  public void setTablename(String tablename) {
    this.tablename = tablename;
  }

  public void unsetTablename() {
    this.tablename = null;
  }

  /** Returns true if field tablename is set (has been assigned a value) and false otherwise */
  public boolean isSetTablename() {
    return this.tablename != null;
  }

  public void setTablenameIsSet(boolean value) {
    if (!value) {
      this.tablename = null;
    }
  }

  public String getPartitionname() {
    return this.partitionname;
  }

  public void setPartitionname(String partitionname) {
    this.partitionname = partitionname;
  }

  public void unsetPartitionname() {
    this.partitionname = null;
  }

  /** Returns true if field partitionname is set (has been assigned a value) and false otherwise */
  public boolean isSetPartitionname() {
    return this.partitionname != null;
  }

  public void setPartitionnameIsSet(boolean value) {
    if (!value) {
      this.partitionname = null;
    }
  }

  /**
   * 
   * @see CompactionType
   */
  public CompactionType getType() {
    return this.type;
  }

  /**
   * 
   * @see CompactionType
   */
  public void setType(CompactionType type) {
    this.type = type;
  }

  public void unsetType() {
    this.type = null;
  }

  /** Returns true if field type is set (has been assigned a value) and false otherwise */
  public boolean isSetType() {
    return this.type != null;
  }

  public void setTypeIsSet(boolean value) {
    if (!value) {
      this.type = null;
    }
  }

  public String getRunas() {
    return this.runas;
  }

  public void setRunas(String runas) {
    this.runas = runas;
  }

  public void unsetRunas() {
    this.runas = null;
  }

  /** Returns true if field runas is set (has been assigned a value) and false otherwise */
  public boolean isSetRunas() {
    return this.runas != null;
  }

  public void setRunasIsSet(boolean value) {
    if (!value) {
      this.runas = null;
    }
  }

  public int getPropertiesSize() {
    return (this.properties == null) ? 0 : this.properties.size();
  }

  public void putToProperties(String key, String val) {
    if (this.properties == null) {
      this.properties = new HashMap<String,String>();
    }
    this.properties.put(key, val);
  }

  public Map<String,String> getProperties() {
    return this.properties;
  }

  public void setProperties(Map<String,String> properties) {
    this.properties = properties;
  }

  public void unsetProperties() {
    this.properties = null;
  }

  /** Returns true if field properties is set (has been assigned a value) and false otherwise */
  public boolean isSetProperties() {
    return this.properties != null;
  }

  public void setPropertiesIsSet(boolean value) {
    if (!value) {
      this.properties = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case DBNAME:
      if (value == null) {
        unsetDbname();
      } else {
        setDbname((String)value);
      }
      break;

    case TABLENAME:
      if (value == null) {
        unsetTablename();
      } else {
        setTablename((String)value);
      }
      break;

    case PARTITIONNAME:
      if (value == null) {
        unsetPartitionname();
      } else {
        setPartitionname((String)value);
      }
      break;

    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((CompactionType)value);
      }
      break;

    case RUNAS:
      if (value == null) {
        unsetRunas();
      } else {
        setRunas((String)value);
      }
      break;

    case PROPERTIES:
      if (value == null) {
        unsetProperties();
      } else {
        setProperties((Map<String,String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case DBNAME:
      return getDbname();

    case TABLENAME:
      return getTablename();

    case PARTITIONNAME:
      return getPartitionname();

    case TYPE:
      return getType();

    case RUNAS:
      return getRunas();

    case PROPERTIES:
      return getProperties();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case DBNAME:
      return isSetDbname();
    case TABLENAME:
      return isSetTablename();
    case PARTITIONNAME:
      return isSetPartitionname();
    case TYPE:
      return isSetType();
    case RUNAS:
      return isSetRunas();
    case PROPERTIES:
      return isSetProperties();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof CompactionRequest)
      return this.equals((CompactionRequest)that);
    return false;
  }

  public boolean equals(CompactionRequest that) {
    if (that == null)
      return false;

    boolean this_present_dbname = true && this.isSetDbname();
    boolean that_present_dbname = true && that.isSetDbname();
    if (this_present_dbname || that_present_dbname) {
      if (!(this_present_dbname && that_present_dbname))
        return false;
      if (!this.dbname.equals(that.dbname))
        return false;
    }

    boolean this_present_tablename = true && this.isSetTablename();
    boolean that_present_tablename = true && that.isSetTablename();
    if (this_present_tablename || that_present_tablename) {
      if (!(this_present_tablename && that_present_tablename))
        return false;
      if (!this.tablename.equals(that.tablename))
        return false;
    }

    boolean this_present_partitionname = true && this.isSetPartitionname();
    boolean that_present_partitionname = true && that.isSetPartitionname();
    if (this_present_partitionname || that_present_partitionname) {
      if (!(this_present_partitionname && that_present_partitionname))
        return false;
      if (!this.partitionname.equals(that.partitionname))
        return false;
    }

    boolean this_present_type = true && this.isSetType();
    boolean that_present_type = true && that.isSetType();
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (!this.type.equals(that.type))
        return false;
    }

    boolean this_present_runas = true && this.isSetRunas();
    boolean that_present_runas = true && that.isSetRunas();
    if (this_present_runas || that_present_runas) {
      if (!(this_present_runas && that_present_runas))
        return false;
      if (!this.runas.equals(that.runas))
        return false;
    }

    boolean this_present_properties = true && this.isSetProperties();
    boolean that_present_properties = true && that.isSetProperties();
    if (this_present_properties || that_present_properties) {
      if (!(this_present_properties && that_present_properties))
        return false;
      if (!this.properties.equals(that.properties))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_dbname = true && (isSetDbname());
    list.add(present_dbname);
    if (present_dbname)
      list.add(dbname);

    boolean present_tablename = true && (isSetTablename());
    list.add(present_tablename);
    if (present_tablename)
      list.add(tablename);

    boolean present_partitionname = true && (isSetPartitionname());
    list.add(present_partitionname);
    if (present_partitionname)
      list.add(partitionname);

    boolean present_type = true && (isSetType());
    list.add(present_type);
    if (present_type)
      list.add(type.getValue());

    boolean present_runas = true && (isSetRunas());
    list.add(present_runas);
    if (present_runas)
      list.add(runas);

    boolean present_properties = true && (isSetProperties());
    list.add(present_properties);
    if (present_properties)
      list.add(properties);

    return list.hashCode();
  }

  @Override
  public int compareTo(CompactionRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetDbname()).compareTo(other.isSetDbname());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDbname()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dbname, other.dbname);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTablename()).compareTo(other.isSetTablename());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTablename()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tablename, other.tablename);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPartitionname()).compareTo(other.isSetPartitionname());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPartitionname()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.partitionname, other.partitionname);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetType()).compareTo(other.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type, other.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetRunas()).compareTo(other.isSetRunas());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRunas()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.runas, other.runas);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetProperties()).compareTo(other.isSetProperties());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetProperties()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.properties, other.properties);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CompactionRequest(");
    boolean first = true;

    sb.append("dbname:");
    if (this.dbname == null) {
      sb.append("null");
    } else {
      sb.append(this.dbname);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("tablename:");
    if (this.tablename == null) {
      sb.append("null");
    } else {
      sb.append(this.tablename);
    }
    first = false;
    if (isSetPartitionname()) {
      if (!first) sb.append(", ");
      sb.append("partitionname:");
      if (this.partitionname == null) {
        sb.append("null");
      } else {
        sb.append(this.partitionname);
      }
      first = false;
    }
    if (!first) sb.append(", ");
    sb.append("type:");
    if (this.type == null) {
      sb.append("null");
    } else {
      sb.append(this.type);
    }
    first = false;
    if (isSetRunas()) {
      if (!first) sb.append(", ");
      sb.append("runas:");
      if (this.runas == null) {
        sb.append("null");
      } else {
        sb.append(this.runas);
      }
      first = false;
    }
    if (isSetProperties()) {
      if (!first) sb.append(", ");
      sb.append("properties:");
      if (this.properties == null) {
        sb.append("null");
      } else {
        sb.append(this.properties);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetDbname()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'dbname' is unset! Struct:" + toString());
    }

    if (!isSetTablename()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'tablename' is unset! Struct:" + toString());
    }

    if (!isSetType()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'type' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class CompactionRequestStandardSchemeFactory implements SchemeFactory {
    public CompactionRequestStandardScheme getScheme() {
      return new CompactionRequestStandardScheme();
    }
  }

  private static class CompactionRequestStandardScheme extends StandardScheme<CompactionRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, CompactionRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // DBNAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dbname = iprot.readString();
              struct.setDbnameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TABLENAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.tablename = iprot.readString();
              struct.setTablenameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // PARTITIONNAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.partitionname = iprot.readString();
              struct.setPartitionnameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.type = org.apache.hadoop.hive.metastore.api.CompactionType.findByValue(iprot.readI32());
              struct.setTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // RUNAS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.runas = iprot.readString();
              struct.setRunasIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // PROPERTIES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map604 = iprot.readMapBegin();
                struct.properties = new HashMap<String,String>(2*_map604.size);
                String _key605;
                String _val606;
                for (int _i607 = 0; _i607 < _map604.size; ++_i607)
                {
                  _key605 = iprot.readString();
                  _val606 = iprot.readString();
                  struct.properties.put(_key605, _val606);
                }
                iprot.readMapEnd();
              }
              struct.setPropertiesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, CompactionRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.dbname != null) {
        oprot.writeFieldBegin(DBNAME_FIELD_DESC);
        oprot.writeString(struct.dbname);
        oprot.writeFieldEnd();
      }
      if (struct.tablename != null) {
        oprot.writeFieldBegin(TABLENAME_FIELD_DESC);
        oprot.writeString(struct.tablename);
        oprot.writeFieldEnd();
      }
      if (struct.partitionname != null) {
        if (struct.isSetPartitionname()) {
          oprot.writeFieldBegin(PARTITIONNAME_FIELD_DESC);
          oprot.writeString(struct.partitionname);
          oprot.writeFieldEnd();
        }
      }
      if (struct.type != null) {
        oprot.writeFieldBegin(TYPE_FIELD_DESC);
        oprot.writeI32(struct.type.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.runas != null) {
        if (struct.isSetRunas()) {
          oprot.writeFieldBegin(RUNAS_FIELD_DESC);
          oprot.writeString(struct.runas);
          oprot.writeFieldEnd();
        }
      }
      if (struct.properties != null) {
        if (struct.isSetProperties()) {
          oprot.writeFieldBegin(PROPERTIES_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.properties.size()));
            for (Map.Entry<String, String> _iter608 : struct.properties.entrySet())
            {
              oprot.writeString(_iter608.getKey());
              oprot.writeString(_iter608.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CompactionRequestTupleSchemeFactory implements SchemeFactory {
    public CompactionRequestTupleScheme getScheme() {
      return new CompactionRequestTupleScheme();
    }
  }

  private static class CompactionRequestTupleScheme extends TupleScheme<CompactionRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, CompactionRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.dbname);
      oprot.writeString(struct.tablename);
      oprot.writeI32(struct.type.getValue());
      BitSet optionals = new BitSet();
      if (struct.isSetPartitionname()) {
        optionals.set(0);
      }
      if (struct.isSetRunas()) {
        optionals.set(1);
      }
      if (struct.isSetProperties()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetPartitionname()) {
        oprot.writeString(struct.partitionname);
      }
      if (struct.isSetRunas()) {
        oprot.writeString(struct.runas);
      }
      if (struct.isSetProperties()) {
        {
          oprot.writeI32(struct.properties.size());
          for (Map.Entry<String, String> _iter609 : struct.properties.entrySet())
          {
            oprot.writeString(_iter609.getKey());
            oprot.writeString(_iter609.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, CompactionRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.dbname = iprot.readString();
      struct.setDbnameIsSet(true);
      struct.tablename = iprot.readString();
      struct.setTablenameIsSet(true);
      struct.type = org.apache.hadoop.hive.metastore.api.CompactionType.findByValue(iprot.readI32());
      struct.setTypeIsSet(true);
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.partitionname = iprot.readString();
        struct.setPartitionnameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.runas = iprot.readString();
        struct.setRunasIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map610 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.properties = new HashMap<String,String>(2*_map610.size);
          String _key611;
          String _val612;
          for (int _i613 = 0; _i613 < _map610.size; ++_i613)
          {
            _key611 = iprot.readString();
            _val612 = iprot.readString();
            struct.properties.put(_key611, _val612);
          }
        }
        struct.setPropertiesIsSet(true);
      }
    }
  }

}

