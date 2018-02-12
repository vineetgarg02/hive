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
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class TableStatsResult implements org.apache.thrift.TBase<TableStatsResult, TableStatsResult._Fields>, java.io.Serializable, Cloneable, Comparable<TableStatsResult> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TableStatsResult");

  private static final org.apache.thrift.protocol.TField TABLE_STATS_FIELD_DESC = new org.apache.thrift.protocol.TField("tableStats", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TableStatsResultStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TableStatsResultTupleSchemeFactory());
  }

  private List<ColumnStatisticsObj> tableStats; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TABLE_STATS((short)1, "tableStats");

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
        case 1: // TABLE_STATS
          return TABLE_STATS;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TABLE_STATS, new org.apache.thrift.meta_data.FieldMetaData("tableStats", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ColumnStatisticsObj.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TableStatsResult.class, metaDataMap);
  }

  public TableStatsResult() {
  }

  public TableStatsResult(
    List<ColumnStatisticsObj> tableStats)
  {
    this();
    this.tableStats = tableStats;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TableStatsResult(TableStatsResult other) {
    if (other.isSetTableStats()) {
      List<ColumnStatisticsObj> __this__tableStats = new ArrayList<ColumnStatisticsObj>(other.tableStats.size());
      for (ColumnStatisticsObj other_element : other.tableStats) {
        __this__tableStats.add(new ColumnStatisticsObj(other_element));
      }
      this.tableStats = __this__tableStats;
    }
  }

  public TableStatsResult deepCopy() {
    return new TableStatsResult(this);
  }

  @Override
  public void clear() {
    this.tableStats = null;
  }

  public int getTableStatsSize() {
    return (this.tableStats == null) ? 0 : this.tableStats.size();
  }

  public java.util.Iterator<ColumnStatisticsObj> getTableStatsIterator() {
    return (this.tableStats == null) ? null : this.tableStats.iterator();
  }

  public void addToTableStats(ColumnStatisticsObj elem) {
    if (this.tableStats == null) {
      this.tableStats = new ArrayList<ColumnStatisticsObj>();
    }
    this.tableStats.add(elem);
  }

  public List<ColumnStatisticsObj> getTableStats() {
    return this.tableStats;
  }

  public void setTableStats(List<ColumnStatisticsObj> tableStats) {
    this.tableStats = tableStats;
  }

  public void unsetTableStats() {
    this.tableStats = null;
  }

  /** Returns true if field tableStats is set (has been assigned a value) and false otherwise */
  public boolean isSetTableStats() {
    return this.tableStats != null;
  }

  public void setTableStatsIsSet(boolean value) {
    if (!value) {
      this.tableStats = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TABLE_STATS:
      if (value == null) {
        unsetTableStats();
      } else {
        setTableStats((List<ColumnStatisticsObj>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TABLE_STATS:
      return getTableStats();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TABLE_STATS:
      return isSetTableStats();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TableStatsResult)
      return this.equals((TableStatsResult)that);
    return false;
  }

  public boolean equals(TableStatsResult that) {
    if (that == null)
      return false;

    boolean this_present_tableStats = true && this.isSetTableStats();
    boolean that_present_tableStats = true && that.isSetTableStats();
    if (this_present_tableStats || that_present_tableStats) {
      if (!(this_present_tableStats && that_present_tableStats))
        return false;
      if (!this.tableStats.equals(that.tableStats))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_tableStats = true && (isSetTableStats());
    list.add(present_tableStats);
    if (present_tableStats)
      list.add(tableStats);

    return list.hashCode();
  }

  @Override
  public int compareTo(TableStatsResult other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetTableStats()).compareTo(other.isSetTableStats());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableStats()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableStats, other.tableStats);
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
    StringBuilder sb = new StringBuilder("TableStatsResult(");
    boolean first = true;

    sb.append("tableStats:");
    if (this.tableStats == null) {
      sb.append("null");
    } else {
      sb.append(this.tableStats);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetTableStats()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'tableStats' is unset! Struct:" + toString());
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

  private static class TableStatsResultStandardSchemeFactory implements SchemeFactory {
    public TableStatsResultStandardScheme getScheme() {
      return new TableStatsResultStandardScheme();
    }
  }

  private static class TableStatsResultStandardScheme extends StandardScheme<TableStatsResult> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TableStatsResult struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TABLE_STATS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list410 = iprot.readListBegin();
                struct.tableStats = new ArrayList<ColumnStatisticsObj>(_list410.size);
                ColumnStatisticsObj _elem411;
                for (int _i412 = 0; _i412 < _list410.size; ++_i412)
                {
                  _elem411 = new ColumnStatisticsObj();
                  _elem411.read(iprot);
                  struct.tableStats.add(_elem411);
                }
                iprot.readListEnd();
              }
              struct.setTableStatsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TableStatsResult struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tableStats != null) {
        oprot.writeFieldBegin(TABLE_STATS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.tableStats.size()));
          for (ColumnStatisticsObj _iter413 : struct.tableStats)
          {
            _iter413.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TableStatsResultTupleSchemeFactory implements SchemeFactory {
    public TableStatsResultTupleScheme getScheme() {
      return new TableStatsResultTupleScheme();
    }
  }

  private static class TableStatsResultTupleScheme extends TupleScheme<TableStatsResult> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TableStatsResult struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.tableStats.size());
        for (ColumnStatisticsObj _iter414 : struct.tableStats)
        {
          _iter414.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TableStatsResult struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list415 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.tableStats = new ArrayList<ColumnStatisticsObj>(_list415.size);
        ColumnStatisticsObj _elem416;
        for (int _i417 = 0; _i417 < _list415.size; ++_i417)
        {
          _elem416 = new ColumnStatisticsObj();
          _elem416.read(iprot);
          struct.tableStats.add(_elem416);
        }
      }
      struct.setTableStatsIsSet(true);
    }
  }

}

