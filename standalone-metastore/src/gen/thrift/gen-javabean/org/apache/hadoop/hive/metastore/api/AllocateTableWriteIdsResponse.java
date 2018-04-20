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
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class AllocateTableWriteIdsResponse implements org.apache.thrift.TBase<AllocateTableWriteIdsResponse, AllocateTableWriteIdsResponse._Fields>, java.io.Serializable, Cloneable, Comparable<AllocateTableWriteIdsResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("AllocateTableWriteIdsResponse");

  private static final org.apache.thrift.protocol.TField TXN_TO_WRITE_IDS_FIELD_DESC = new org.apache.thrift.protocol.TField("txnToWriteIds", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new AllocateTableWriteIdsResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new AllocateTableWriteIdsResponseTupleSchemeFactory());
  }

  private List<TxnToWriteId> txnToWriteIds; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TXN_TO_WRITE_IDS((short)1, "txnToWriteIds");

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
        case 1: // TXN_TO_WRITE_IDS
          return TXN_TO_WRITE_IDS;
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
    tmpMap.put(_Fields.TXN_TO_WRITE_IDS, new org.apache.thrift.meta_data.FieldMetaData("txnToWriteIds", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TxnToWriteId.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(AllocateTableWriteIdsResponse.class, metaDataMap);
  }

  public AllocateTableWriteIdsResponse() {
  }

  public AllocateTableWriteIdsResponse(
    List<TxnToWriteId> txnToWriteIds)
  {
    this();
    this.txnToWriteIds = txnToWriteIds;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public AllocateTableWriteIdsResponse(AllocateTableWriteIdsResponse other) {
    if (other.isSetTxnToWriteIds()) {
      List<TxnToWriteId> __this__txnToWriteIds = new ArrayList<TxnToWriteId>(other.txnToWriteIds.size());
      for (TxnToWriteId other_element : other.txnToWriteIds) {
        __this__txnToWriteIds.add(new TxnToWriteId(other_element));
      }
      this.txnToWriteIds = __this__txnToWriteIds;
    }
  }

  public AllocateTableWriteIdsResponse deepCopy() {
    return new AllocateTableWriteIdsResponse(this);
  }

  @Override
  public void clear() {
    this.txnToWriteIds = null;
  }

  public int getTxnToWriteIdsSize() {
    return (this.txnToWriteIds == null) ? 0 : this.txnToWriteIds.size();
  }

  public java.util.Iterator<TxnToWriteId> getTxnToWriteIdsIterator() {
    return (this.txnToWriteIds == null) ? null : this.txnToWriteIds.iterator();
  }

  public void addToTxnToWriteIds(TxnToWriteId elem) {
    if (this.txnToWriteIds == null) {
      this.txnToWriteIds = new ArrayList<TxnToWriteId>();
    }
    this.txnToWriteIds.add(elem);
  }

  public List<TxnToWriteId> getTxnToWriteIds() {
    return this.txnToWriteIds;
  }

  public void setTxnToWriteIds(List<TxnToWriteId> txnToWriteIds) {
    this.txnToWriteIds = txnToWriteIds;
  }

  public void unsetTxnToWriteIds() {
    this.txnToWriteIds = null;
  }

  /** Returns true if field txnToWriteIds is set (has been assigned a value) and false otherwise */
  public boolean isSetTxnToWriteIds() {
    return this.txnToWriteIds != null;
  }

  public void setTxnToWriteIdsIsSet(boolean value) {
    if (!value) {
      this.txnToWriteIds = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TXN_TO_WRITE_IDS:
      if (value == null) {
        unsetTxnToWriteIds();
      } else {
        setTxnToWriteIds((List<TxnToWriteId>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TXN_TO_WRITE_IDS:
      return getTxnToWriteIds();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TXN_TO_WRITE_IDS:
      return isSetTxnToWriteIds();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof AllocateTableWriteIdsResponse)
      return this.equals((AllocateTableWriteIdsResponse)that);
    return false;
  }

  public boolean equals(AllocateTableWriteIdsResponse that) {
    if (that == null)
      return false;

    boolean this_present_txnToWriteIds = true && this.isSetTxnToWriteIds();
    boolean that_present_txnToWriteIds = true && that.isSetTxnToWriteIds();
    if (this_present_txnToWriteIds || that_present_txnToWriteIds) {
      if (!(this_present_txnToWriteIds && that_present_txnToWriteIds))
        return false;
      if (!this.txnToWriteIds.equals(that.txnToWriteIds))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_txnToWriteIds = true && (isSetTxnToWriteIds());
    list.add(present_txnToWriteIds);
    if (present_txnToWriteIds)
      list.add(txnToWriteIds);

    return list.hashCode();
  }

  @Override
  public int compareTo(AllocateTableWriteIdsResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetTxnToWriteIds()).compareTo(other.isSetTxnToWriteIds());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTxnToWriteIds()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.txnToWriteIds, other.txnToWriteIds);
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
    StringBuilder sb = new StringBuilder("AllocateTableWriteIdsResponse(");
    boolean first = true;

    sb.append("txnToWriteIds:");
    if (this.txnToWriteIds == null) {
      sb.append("null");
    } else {
      sb.append(this.txnToWriteIds);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetTxnToWriteIds()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'txnToWriteIds' is unset! Struct:" + toString());
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

  private static class AllocateTableWriteIdsResponseStandardSchemeFactory implements SchemeFactory {
    public AllocateTableWriteIdsResponseStandardScheme getScheme() {
      return new AllocateTableWriteIdsResponseStandardScheme();
    }
  }

  private static class AllocateTableWriteIdsResponseStandardScheme extends StandardScheme<AllocateTableWriteIdsResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, AllocateTableWriteIdsResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TXN_TO_WRITE_IDS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list634 = iprot.readListBegin();
                struct.txnToWriteIds = new ArrayList<TxnToWriteId>(_list634.size);
                TxnToWriteId _elem635;
                for (int _i636 = 0; _i636 < _list634.size; ++_i636)
                {
                  _elem635 = new TxnToWriteId();
                  _elem635.read(iprot);
                  struct.txnToWriteIds.add(_elem635);
                }
                iprot.readListEnd();
              }
              struct.setTxnToWriteIdsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, AllocateTableWriteIdsResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.txnToWriteIds != null) {
        oprot.writeFieldBegin(TXN_TO_WRITE_IDS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.txnToWriteIds.size()));
          for (TxnToWriteId _iter637 : struct.txnToWriteIds)
          {
            _iter637.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class AllocateTableWriteIdsResponseTupleSchemeFactory implements SchemeFactory {
    public AllocateTableWriteIdsResponseTupleScheme getScheme() {
      return new AllocateTableWriteIdsResponseTupleScheme();
    }
  }

  private static class AllocateTableWriteIdsResponseTupleScheme extends TupleScheme<AllocateTableWriteIdsResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, AllocateTableWriteIdsResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.txnToWriteIds.size());
        for (TxnToWriteId _iter638 : struct.txnToWriteIds)
        {
          _iter638.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, AllocateTableWriteIdsResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list639 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.txnToWriteIds = new ArrayList<TxnToWriteId>(_list639.size);
        TxnToWriteId _elem640;
        for (int _i641 = 0; _i641 < _list639.size; ++_i641)
        {
          _elem640 = new TxnToWriteId();
          _elem640.read(iprot);
          struct.txnToWriteIds.add(_elem640);
        }
      }
      struct.setTxnToWriteIdsIsSet(true);
    }
  }

}

