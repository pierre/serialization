/*
 * Copyright 2010-2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.serialization.event;

import org.apache.thrift.TBase;
import org.apache.thrift.TBaseHelper;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class TLoggingEvent implements TBase<TLoggingEvent, TLoggingEvent._Fields>, java.io.Serializable, Cloneable
{
    private static final TStruct STRUCT_DESC = new TStruct("TLoggingEvent");

    private static final TField EVENT_DATE_FIELD_DESC = new TField("eventDate", TType.I64, (short) 1);
    private static final TField LEVEL_FIELD_DESC = new TField("level", TType.STRING, (short) 2);
    private static final TField MESSAGE_FIELD_DESC = new TField("message", TType.STRING, (short) 3);
    private static final TField LOGGERNAME_FIELD_DESC = new TField("loggername", TType.STRING, (short) 4);
    private static final TField LOCATION_FILENAME_FIELD_DESC = new TField("locationFilename", TType.STRING, (short) 5);
    private static final TField LOCATION_METHOD_NAME_FIELD_DESC = new TField("locationMethodName", TType.STRING, (short) 6);
    private static final TField LOCATION_CLASS_NAME_FIELD_DESC = new TField("locationClassName", TType.STRING, (short) 7);
    private static final TField LOCATION_LINE_NUMBER_FIELD_DESC = new TField("locationLineNumber", TType.STRING, (short) 8);
    private static final TField NDC_FIELD_DESC = new TField("ndc", TType.STRING, (short) 9);
    private static final TField THREAD_NAME_FIELD_DESC = new TField("threadName", TType.STRING, (short) 10);
    private static final TField THROWABLE_LOCALIZED_MESSAGE_FIELD_DESC = new TField("throwableLocalizedMessage", TType.STRING, (short) 11);
    private static final TField THROWABLE_MESSAGE_FIELD_DESC = new TField("throwableMessage", TType.STRING, (short) 12);
    private static final TField STACKTRACE_FIELD_DESC = new TField("stacktrace", TType.STRING, (short) 13);
    private static final TField CORE_IP_FIELD_DESC = new TField("coreIp", TType.STRING, (short) 14);
    private static final TField CORE_HOSTNAME_FIELD_DESC = new TField("coreHostname", TType.STRING, (short) 15);
    private static final TField CORE_TYPE_FIELD_DESC = new TField("coreType", TType.STRING, (short) 16);
    private static final TField TRACE_FIELD_DESC = new TField("trace", TType.STRING, (short) 17);

    public long eventDate;
    public String level;
    public String message;
    public String loggername;
    public String locationFilename;
    public String locationMethodName;
    public String locationClassName;
    public String locationLineNumber;
    public String ndc;
    public String threadName;
    public String throwableLocalizedMessage;
    public String throwableMessage;
    public String stacktrace;
    public String coreIp;
    public String coreHostname;
    public String coreType;
    public String trace;

    /**
     * The set of fields this struct contains, along with convenience methods for finding and manipulating them.
     */
    public enum _Fields implements TFieldIdEnum
    {
        EVENT_DATE((short) 1, "eventDate"),
        LEVEL((short) 2, "level"),
        MESSAGE((short) 3, "message"),
        LOGGERNAME((short) 4, "loggername"),
        LOCATION_FILENAME((short) 5, "locationFilename"),
        LOCATION_METHOD_NAME((short) 6, "locationMethodName"),
        LOCATION_CLASS_NAME((short) 7, "locationClassName"),
        LOCATION_LINE_NUMBER((short) 8, "locationLineNumber"),
        NDC((short) 9, "ndc"),
        THREAD_NAME((short) 10, "threadName"),
        THROWABLE_LOCALIZED_MESSAGE((short) 11, "throwableLocalizedMessage"),
        THROWABLE_MESSAGE((short) 12, "throwableMessage"),
        STACKTRACE((short) 13, "stacktrace"),
        CORE_IP((short) 14, "coreIp"),
        CORE_HOSTNAME((short) 15, "coreHostname"),
        CORE_TYPE((short) 16, "coreType"),
        TRACE((short) 17, "trace");

        private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

        static {
            for (_Fields field : EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, or null if its not found.
         */
        public static _Fields findByThriftId(int fieldId)
        {
            switch (fieldId) {
                case 1: // EVENT_DATE
                    return EVENT_DATE;
                case 2: // LEVEL
                    return LEVEL;
                case 3: // MESSAGE
                    return MESSAGE;
                case 4: // LOGGERNAME
                    return LOGGERNAME;
                case 5: // LOCATION_FILENAME
                    return LOCATION_FILENAME;
                case 6: // LOCATION_METHOD_NAME
                    return LOCATION_METHOD_NAME;
                case 7: // LOCATION_CLASS_NAME
                    return LOCATION_CLASS_NAME;
                case 8: // LOCATION_LINE_NUMBER
                    return LOCATION_LINE_NUMBER;
                case 9: // NDC
                    return NDC;
                case 10: // THREAD_NAME
                    return THREAD_NAME;
                case 11: // THROWABLE_LOCALIZED_MESSAGE
                    return THROWABLE_LOCALIZED_MESSAGE;
                case 12: // THROWABLE_MESSAGE
                    return THROWABLE_MESSAGE;
                case 13: // STACKTRACE
                    return STACKTRACE;
                case 14: // CORE_IP
                    return CORE_IP;
                case 15: // CORE_HOSTNAME
                    return CORE_HOSTNAME;
                case 16: // CORE_TYPE
                    return CORE_TYPE;
                case 17: // TRACE
                    return TRACE;
                default:
                    return null;
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, throwing an exception
         * if it is not found.
         */
        public static _Fields findByThriftIdOrThrow(int fieldId)
        {
            _Fields fields = findByThriftId(fieldId);
            if (fields == null) {
                throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
            }
            return fields;
        }

        /**
         * Find the _Fields constant that matches name, or null if its not found.
         */
        public static _Fields findByName(String name)
        {
            return byName.get(name);
        }

        private final short _thriftId;
        private final String _fieldName;

        _Fields(short thriftId, String fieldName)
        {
            _thriftId = thriftId;
            _fieldName = fieldName;
        }

        public short getThriftFieldId()
        {
            return _thriftId;
        }

        public String getFieldName()
        {
            return _fieldName;
        }
    }

    // isset id assignments
    private static final int __EVENTDATE_ISSET_ID = 0;
    private BitSet __isset_bit_vector = new BitSet(1);

    public static final Map<_Fields, FieldMetaData> metaDataMap;

    static {
        Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
        tmpMap.put(_Fields.EVENT_DATE, new FieldMetaData("eventDate", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.I64)));
        tmpMap.put(_Fields.LEVEL, new FieldMetaData("level", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.MESSAGE, new FieldMetaData("message", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.LOGGERNAME, new FieldMetaData("loggername", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.LOCATION_FILENAME, new FieldMetaData("locationFilename", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.LOCATION_METHOD_NAME, new FieldMetaData("locationMethodName", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.LOCATION_CLASS_NAME, new FieldMetaData("locationClassName", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.LOCATION_LINE_NUMBER, new FieldMetaData("locationLineNumber", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.NDC, new FieldMetaData("ndc", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.THREAD_NAME, new FieldMetaData("threadName", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.THROWABLE_LOCALIZED_MESSAGE, new FieldMetaData("throwableLocalizedMessage", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.THROWABLE_MESSAGE, new FieldMetaData("throwableMessage", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.STACKTRACE, new FieldMetaData("stacktrace", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.CORE_IP, new FieldMetaData("coreIp", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.CORE_HOSTNAME, new FieldMetaData("coreHostname", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.CORE_TYPE, new FieldMetaData("coreType", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        tmpMap.put(_Fields.TRACE, new FieldMetaData("trace", TFieldRequirementType.DEFAULT,
            new FieldValueMetaData(TType.STRING)));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        FieldMetaData.addStructMetaDataMap(TLoggingEvent.class, metaDataMap);
    }

    public TLoggingEvent()
    {
    }

    public TLoggingEvent(
        long eventDate,
        String level,
        String message,
        String loggername,
        String locationFilename,
        String locationMethodName,
        String locationClassName,
        String locationLineNumber,
        String ndc,
        String threadName,
        String throwableLocalizedMessage,
        String throwableMessage,
        String stacktrace,
        String coreIp,
        String coreHostname,
        String coreType,
        String trace)
    {
        this();
        this.eventDate = eventDate;
        setEventDateIsSet(true);
        this.level = level;
        this.message = message;
        this.loggername = loggername;
        this.locationFilename = locationFilename;
        this.locationMethodName = locationMethodName;
        this.locationClassName = locationClassName;
        this.locationLineNumber = locationLineNumber;
        this.ndc = ndc;
        this.threadName = threadName;
        this.throwableLocalizedMessage = throwableLocalizedMessage;
        this.throwableMessage = throwableMessage;
        this.stacktrace = stacktrace;
        this.coreIp = coreIp;
        this.coreHostname = coreHostname;
        this.coreType = coreType;
        this.trace = trace;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public TLoggingEvent(TLoggingEvent other)
    {
        __isset_bit_vector.clear();
        __isset_bit_vector.or(other.__isset_bit_vector);
        this.eventDate = other.eventDate;
        if (other.isSetLevel()) {
            this.level = other.level;
        }
        if (other.isSetMessage()) {
            this.message = other.message;
        }
        if (other.isSetLoggername()) {
            this.loggername = other.loggername;
        }
        if (other.isSetLocationFilename()) {
            this.locationFilename = other.locationFilename;
        }
        if (other.isSetLocationMethodName()) {
            this.locationMethodName = other.locationMethodName;
        }
        if (other.isSetLocationClassName()) {
            this.locationClassName = other.locationClassName;
        }
        if (other.isSetLocationLineNumber()) {
            this.locationLineNumber = other.locationLineNumber;
        }
        if (other.isSetNdc()) {
            this.ndc = other.ndc;
        }
        if (other.isSetThreadName()) {
            this.threadName = other.threadName;
        }
        if (other.isSetThrowableLocalizedMessage()) {
            this.throwableLocalizedMessage = other.throwableLocalizedMessage;
        }
        if (other.isSetThrowableMessage()) {
            this.throwableMessage = other.throwableMessage;
        }
        if (other.isSetStacktrace()) {
            this.stacktrace = other.stacktrace;
        }
        if (other.isSetCoreIp()) {
            this.coreIp = other.coreIp;
        }
        if (other.isSetCoreHostname()) {
            this.coreHostname = other.coreHostname;
        }
        if (other.isSetCoreType()) {
            this.coreType = other.coreType;
        }
        if (other.isSetTrace()) {
            this.trace = other.trace;
        }
    }

    public TLoggingEvent deepCopy()
    {
        return new TLoggingEvent(this);
    }

    @Override
    public void clear()
    {
        setEventDateIsSet(false);
        this.eventDate = 0;
        this.level = null;
        this.message = null;
        this.loggername = null;
        this.locationFilename = null;
        this.locationMethodName = null;
        this.locationClassName = null;
        this.locationLineNumber = null;
        this.ndc = null;
        this.threadName = null;
        this.throwableLocalizedMessage = null;
        this.throwableMessage = null;
        this.stacktrace = null;
        this.coreIp = null;
        this.coreHostname = null;
        this.coreType = null;
        this.trace = null;
    }

    public long getEventDate()
    {
        return this.eventDate;
    }

    public TLoggingEvent setEventDate(long eventDate)
    {
        this.eventDate = eventDate;
        setEventDateIsSet(true);
        return this;
    }

    public void unsetEventDate()
    {
        __isset_bit_vector.clear(__EVENTDATE_ISSET_ID);
    }

    /**
     * Returns true if field eventDate is set (has been asigned a value) and false otherwise
     */
    public boolean isSetEventDate()
    {
        return __isset_bit_vector.get(__EVENTDATE_ISSET_ID);
    }

    public void setEventDateIsSet(boolean value)
    {
        __isset_bit_vector.set(__EVENTDATE_ISSET_ID, value);
    }

    public String getLevel()
    {
        return this.level;
    }

    public TLoggingEvent setLevel(String level)
    {
        this.level = level;
        return this;
    }

    public void unsetLevel()
    {
        this.level = null;
    }

    /**
     * Returns true if field level is set (has been asigned a value) and false otherwise
     */
    public boolean isSetLevel()
    {
        return this.level != null;
    }

    public void setLevelIsSet(boolean value)
    {
        if (!value) {
            this.level = null;
        }
    }

    public String getMessage()
    {
        return this.message;
    }

    public TLoggingEvent setMessage(String message)
    {
        this.message = message;
        return this;
    }

    public void unsetMessage()
    {
        this.message = null;
    }

    /**
     * Returns true if field message is set (has been asigned a value) and false otherwise
     */
    public boolean isSetMessage()
    {
        return this.message != null;
    }

    public void setMessageIsSet(boolean value)
    {
        if (!value) {
            this.message = null;
        }
    }

    public String getLoggername()
    {
        return this.loggername;
    }

    public TLoggingEvent setLoggername(String loggername)
    {
        this.loggername = loggername;
        return this;
    }

    public void unsetLoggername()
    {
        this.loggername = null;
    }

    /**
     * Returns true if field loggername is set (has been asigned a value) and false otherwise
     */
    public boolean isSetLoggername()
    {
        return this.loggername != null;
    }

    public void setLoggernameIsSet(boolean value)
    {
        if (!value) {
            this.loggername = null;
        }
    }

    public String getLocationFilename()
    {
        return this.locationFilename;
    }

    public TLoggingEvent setLocationFilename(String locationFilename)
    {
        this.locationFilename = locationFilename;
        return this;
    }

    public void unsetLocationFilename()
    {
        this.locationFilename = null;
    }

    /**
     * Returns true if field locationFilename is set (has been asigned a value) and false otherwise
     */
    public boolean isSetLocationFilename()
    {
        return this.locationFilename != null;
    }

    public void setLocationFilenameIsSet(boolean value)
    {
        if (!value) {
            this.locationFilename = null;
        }
    }

    public String getLocationMethodName()
    {
        return this.locationMethodName;
    }

    public TLoggingEvent setLocationMethodName(String locationMethodName)
    {
        this.locationMethodName = locationMethodName;
        return this;
    }

    public void unsetLocationMethodName()
    {
        this.locationMethodName = null;
    }

    /**
     * Returns true if field locationMethodName is set (has been asigned a value) and false otherwise
     */
    public boolean isSetLocationMethodName()
    {
        return this.locationMethodName != null;
    }

    public void setLocationMethodNameIsSet(boolean value)
    {
        if (!value) {
            this.locationMethodName = null;
        }
    }

    public String getLocationClassName()
    {
        return this.locationClassName;
    }

    public TLoggingEvent setLocationClassName(String locationClassName)
    {
        this.locationClassName = locationClassName;
        return this;
    }

    public void unsetLocationClassName()
    {
        this.locationClassName = null;
    }

    /**
     * Returns true if field locationClassName is set (has been asigned a value) and false otherwise
     */
    public boolean isSetLocationClassName()
    {
        return this.locationClassName != null;
    }

    public void setLocationClassNameIsSet(boolean value)
    {
        if (!value) {
            this.locationClassName = null;
        }
    }

    public String getLocationLineNumber()
    {
        return this.locationLineNumber;
    }

    public TLoggingEvent setLocationLineNumber(String locationLineNumber)
    {
        this.locationLineNumber = locationLineNumber;
        return this;
    }

    public void unsetLocationLineNumber()
    {
        this.locationLineNumber = null;
    }

    /**
     * Returns true if field locationLineNumber is set (has been asigned a value) and false otherwise
     */
    public boolean isSetLocationLineNumber()
    {
        return this.locationLineNumber != null;
    }

    public void setLocationLineNumberIsSet(boolean value)
    {
        if (!value) {
            this.locationLineNumber = null;
        }
    }

    public String getNdc()
    {
        return this.ndc;
    }

    public TLoggingEvent setNdc(String ndc)
    {
        this.ndc = ndc;
        return this;
    }

    public void unsetNdc()
    {
        this.ndc = null;
    }

    /**
     * Returns true if field ndc is set (has been asigned a value) and false otherwise
     */
    public boolean isSetNdc()
    {
        return this.ndc != null;
    }

    public void setNdcIsSet(boolean value)
    {
        if (!value) {
            this.ndc = null;
        }
    }

    public String getThreadName()
    {
        return this.threadName;
    }

    public TLoggingEvent setThreadName(String threadName)
    {
        this.threadName = threadName;
        return this;
    }

    public void unsetThreadName()
    {
        this.threadName = null;
    }

    /**
     * Returns true if field threadName is set (has been asigned a value) and false otherwise
     */
    public boolean isSetThreadName()
    {
        return this.threadName != null;
    }

    public void setThreadNameIsSet(boolean value)
    {
        if (!value) {
            this.threadName = null;
        }
    }

    public String getThrowableLocalizedMessage()
    {
        return this.throwableLocalizedMessage;
    }

    public TLoggingEvent setThrowableLocalizedMessage(String throwableLocalizedMessage)
    {
        this.throwableLocalizedMessage = throwableLocalizedMessage;
        return this;
    }

    public void unsetThrowableLocalizedMessage()
    {
        this.throwableLocalizedMessage = null;
    }

    /**
     * Returns true if field throwableLocalizedMessage is set (has been asigned a value) and false otherwise
     */
    public boolean isSetThrowableLocalizedMessage()
    {
        return this.throwableLocalizedMessage != null;
    }

    public void setThrowableLocalizedMessageIsSet(boolean value)
    {
        if (!value) {
            this.throwableLocalizedMessage = null;
        }
    }

    public String getThrowableMessage()
    {
        return this.throwableMessage;
    }

    public TLoggingEvent setThrowableMessage(String throwableMessage)
    {
        this.throwableMessage = throwableMessage;
        return this;
    }

    public void unsetThrowableMessage()
    {
        this.throwableMessage = null;
    }

    /**
     * Returns true if field throwableMessage is set (has been asigned a value) and false otherwise
     */
    public boolean isSetThrowableMessage()
    {
        return this.throwableMessage != null;
    }

    public void setThrowableMessageIsSet(boolean value)
    {
        if (!value) {
            this.throwableMessage = null;
        }
    }

    public String getStacktrace()
    {
        return this.stacktrace;
    }

    public TLoggingEvent setStacktrace(String stacktrace)
    {
        this.stacktrace = stacktrace;
        return this;
    }

    public void unsetStacktrace()
    {
        this.stacktrace = null;
    }

    /**
     * Returns true if field stacktrace is set (has been asigned a value) and false otherwise
     */
    public boolean isSetStacktrace()
    {
        return this.stacktrace != null;
    }

    public void setStacktraceIsSet(boolean value)
    {
        if (!value) {
            this.stacktrace = null;
        }
    }

    public String getCoreIp()
    {
        return this.coreIp;
    }

    public TLoggingEvent setCoreIp(String coreIp)
    {
        this.coreIp = coreIp;
        return this;
    }

    public void unsetCoreIp()
    {
        this.coreIp = null;
    }

    /**
     * Returns true if field coreIp is set (has been asigned a value) and false otherwise
     */
    public boolean isSetCoreIp()
    {
        return this.coreIp != null;
    }

    public void setCoreIpIsSet(boolean value)
    {
        if (!value) {
            this.coreIp = null;
        }
    }

    public String getCoreHostname()
    {
        return this.coreHostname;
    }

    public TLoggingEvent setCoreHostname(String coreHostname)
    {
        this.coreHostname = coreHostname;
        return this;
    }

    public void unsetCoreHostname()
    {
        this.coreHostname = null;
    }

    /**
     * Returns true if field coreHostname is set (has been asigned a value) and false otherwise
     */
    public boolean isSetCoreHostname()
    {
        return this.coreHostname != null;
    }

    public void setCoreHostnameIsSet(boolean value)
    {
        if (!value) {
            this.coreHostname = null;
        }
    }

    public String getCoreType()
    {
        return this.coreType;
    }

    public TLoggingEvent setCoreType(String coreType)
    {
        this.coreType = coreType;
        return this;
    }

    public void unsetCoreType()
    {
        this.coreType = null;
    }

    /**
     * Returns true if field coreType is set (has been asigned a value) and false otherwise
     */
    public boolean isSetCoreType()
    {
        return this.coreType != null;
    }

    public void setCoreTypeIsSet(boolean value)
    {
        if (!value) {
            this.coreType = null;
        }
    }

    public String getTrace()
    {
        return this.trace;
    }

    public TLoggingEvent setTrace(String trace)
    {
        this.trace = trace;
        return this;
    }

    public void unsetTrace()
    {
        this.trace = null;
    }

    /**
     * Returns true if field trace is set (has been asigned a value) and false otherwise
     */
    public boolean isSetTrace()
    {
        return this.trace != null;
    }

    public void setTraceIsSet(boolean value)
    {
        if (!value) {
            this.trace = null;
        }
    }

    public void setFieldValue(_Fields field, Object value)
    {
        switch (field) {
            case EVENT_DATE:
                if (value == null) {
                    unsetEventDate();
                }
                else {
                    setEventDate((Long) value);
                }
                break;

            case LEVEL:
                if (value == null) {
                    unsetLevel();
                }
                else {
                    setLevel((String) value);
                }
                break;

            case MESSAGE:
                if (value == null) {
                    unsetMessage();
                }
                else {
                    setMessage((String) value);
                }
                break;

            case LOGGERNAME:
                if (value == null) {
                    unsetLoggername();
                }
                else {
                    setLoggername((String) value);
                }
                break;

            case LOCATION_FILENAME:
                if (value == null) {
                    unsetLocationFilename();
                }
                else {
                    setLocationFilename((String) value);
                }
                break;

            case LOCATION_METHOD_NAME:
                if (value == null) {
                    unsetLocationMethodName();
                }
                else {
                    setLocationMethodName((String) value);
                }
                break;

            case LOCATION_CLASS_NAME:
                if (value == null) {
                    unsetLocationClassName();
                }
                else {
                    setLocationClassName((String) value);
                }
                break;

            case LOCATION_LINE_NUMBER:
                if (value == null) {
                    unsetLocationLineNumber();
                }
                else {
                    setLocationLineNumber((String) value);
                }
                break;

            case NDC:
                if (value == null) {
                    unsetNdc();
                }
                else {
                    setNdc((String) value);
                }
                break;

            case THREAD_NAME:
                if (value == null) {
                    unsetThreadName();
                }
                else {
                    setThreadName((String) value);
                }
                break;

            case THROWABLE_LOCALIZED_MESSAGE:
                if (value == null) {
                    unsetThrowableLocalizedMessage();
                }
                else {
                    setThrowableLocalizedMessage((String) value);
                }
                break;

            case THROWABLE_MESSAGE:
                if (value == null) {
                    unsetThrowableMessage();
                }
                else {
                    setThrowableMessage((String) value);
                }
                break;

            case STACKTRACE:
                if (value == null) {
                    unsetStacktrace();
                }
                else {
                    setStacktrace((String) value);
                }
                break;

            case CORE_IP:
                if (value == null) {
                    unsetCoreIp();
                }
                else {
                    setCoreIp((String) value);
                }
                break;

            case CORE_HOSTNAME:
                if (value == null) {
                    unsetCoreHostname();
                }
                else {
                    setCoreHostname((String) value);
                }
                break;

            case CORE_TYPE:
                if (value == null) {
                    unsetCoreType();
                }
                else {
                    setCoreType((String) value);
                }
                break;

            case TRACE:
                if (value == null) {
                    unsetTrace();
                }
                else {
                    setTrace((String) value);
                }
                break;

        }
    }

    public Object getFieldValue(_Fields field)
    {
        switch (field) {
            case EVENT_DATE:
                return new Long(getEventDate());

            case LEVEL:
                return getLevel();

            case MESSAGE:
                return getMessage();

            case LOGGERNAME:
                return getLoggername();

            case LOCATION_FILENAME:
                return getLocationFilename();

            case LOCATION_METHOD_NAME:
                return getLocationMethodName();

            case LOCATION_CLASS_NAME:
                return getLocationClassName();

            case LOCATION_LINE_NUMBER:
                return getLocationLineNumber();

            case NDC:
                return getNdc();

            case THREAD_NAME:
                return getThreadName();

            case THROWABLE_LOCALIZED_MESSAGE:
                return getThrowableLocalizedMessage();

            case THROWABLE_MESSAGE:
                return getThrowableMessage();

            case STACKTRACE:
                return getStacktrace();

            case CORE_IP:
                return getCoreIp();

            case CORE_HOSTNAME:
                return getCoreHostname();

            case CORE_TYPE:
                return getCoreType();

            case TRACE:
                return getTrace();

        }
        throw new IllegalStateException();
    }

    /**
     * Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
     */
    public boolean isSet(_Fields field)
    {
        if (field == null) {
            throw new IllegalArgumentException();
        }

        switch (field) {
            case EVENT_DATE:
                return isSetEventDate();
            case LEVEL:
                return isSetLevel();
            case MESSAGE:
                return isSetMessage();
            case LOGGERNAME:
                return isSetLoggername();
            case LOCATION_FILENAME:
                return isSetLocationFilename();
            case LOCATION_METHOD_NAME:
                return isSetLocationMethodName();
            case LOCATION_CLASS_NAME:
                return isSetLocationClassName();
            case LOCATION_LINE_NUMBER:
                return isSetLocationLineNumber();
            case NDC:
                return isSetNdc();
            case THREAD_NAME:
                return isSetThreadName();
            case THROWABLE_LOCALIZED_MESSAGE:
                return isSetThrowableLocalizedMessage();
            case THROWABLE_MESSAGE:
                return isSetThrowableMessage();
            case STACKTRACE:
                return isSetStacktrace();
            case CORE_IP:
                return isSetCoreIp();
            case CORE_HOSTNAME:
                return isSetCoreHostname();
            case CORE_TYPE:
                return isSetCoreType();
            case TRACE:
                return isSetTrace();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that)
    {
        if (that == null) {
            return false;
        }
        if (that instanceof TLoggingEvent) {
            return this.equals((TLoggingEvent) that);
        }
        return false;
    }

    public boolean equals(TLoggingEvent that)
    {
        if (that == null) {
            return false;
        }

        boolean this_present_eventDate = true;
        boolean that_present_eventDate = true;
        if (this_present_eventDate || that_present_eventDate) {
            if (!(this_present_eventDate && that_present_eventDate)) {
                return false;
            }
            if (this.eventDate != that.eventDate) {
                return false;
            }
        }

        boolean this_present_level = true && this.isSetLevel();
        boolean that_present_level = true && that.isSetLevel();
        if (this_present_level || that_present_level) {
            if (!(this_present_level && that_present_level)) {
                return false;
            }
            if (!this.level.equals(that.level)) {
                return false;
            }
        }

        boolean this_present_message = true && this.isSetMessage();
        boolean that_present_message = true && that.isSetMessage();
        if (this_present_message || that_present_message) {
            if (!(this_present_message && that_present_message)) {
                return false;
            }
            if (!this.message.equals(that.message)) {
                return false;
            }
        }

        boolean this_present_loggername = true && this.isSetLoggername();
        boolean that_present_loggername = true && that.isSetLoggername();
        if (this_present_loggername || that_present_loggername) {
            if (!(this_present_loggername && that_present_loggername)) {
                return false;
            }
            if (!this.loggername.equals(that.loggername)) {
                return false;
            }
        }

        boolean this_present_locationFilename = true && this.isSetLocationFilename();
        boolean that_present_locationFilename = true && that.isSetLocationFilename();
        if (this_present_locationFilename || that_present_locationFilename) {
            if (!(this_present_locationFilename && that_present_locationFilename)) {
                return false;
            }
            if (!this.locationFilename.equals(that.locationFilename)) {
                return false;
            }
        }

        boolean this_present_locationMethodName = true && this.isSetLocationMethodName();
        boolean that_present_locationMethodName = true && that.isSetLocationMethodName();
        if (this_present_locationMethodName || that_present_locationMethodName) {
            if (!(this_present_locationMethodName && that_present_locationMethodName)) {
                return false;
            }
            if (!this.locationMethodName.equals(that.locationMethodName)) {
                return false;
            }
        }

        boolean this_present_locationClassName = true && this.isSetLocationClassName();
        boolean that_present_locationClassName = true && that.isSetLocationClassName();
        if (this_present_locationClassName || that_present_locationClassName) {
            if (!(this_present_locationClassName && that_present_locationClassName)) {
                return false;
            }
            if (!this.locationClassName.equals(that.locationClassName)) {
                return false;
            }
        }

        boolean this_present_locationLineNumber = true && this.isSetLocationLineNumber();
        boolean that_present_locationLineNumber = true && that.isSetLocationLineNumber();
        if (this_present_locationLineNumber || that_present_locationLineNumber) {
            if (!(this_present_locationLineNumber && that_present_locationLineNumber)) {
                return false;
            }
            if (!this.locationLineNumber.equals(that.locationLineNumber)) {
                return false;
            }
        }

        boolean this_present_ndc = true && this.isSetNdc();
        boolean that_present_ndc = true && that.isSetNdc();
        if (this_present_ndc || that_present_ndc) {
            if (!(this_present_ndc && that_present_ndc)) {
                return false;
            }
            if (!this.ndc.equals(that.ndc)) {
                return false;
            }
        }

        boolean this_present_threadName = true && this.isSetThreadName();
        boolean that_present_threadName = true && that.isSetThreadName();
        if (this_present_threadName || that_present_threadName) {
            if (!(this_present_threadName && that_present_threadName)) {
                return false;
            }
            if (!this.threadName.equals(that.threadName)) {
                return false;
            }
        }

        boolean this_present_throwableLocalizedMessage = true && this.isSetThrowableLocalizedMessage();
        boolean that_present_throwableLocalizedMessage = true && that.isSetThrowableLocalizedMessage();
        if (this_present_throwableLocalizedMessage || that_present_throwableLocalizedMessage) {
            if (!(this_present_throwableLocalizedMessage && that_present_throwableLocalizedMessage)) {
                return false;
            }
            if (!this.throwableLocalizedMessage.equals(that.throwableLocalizedMessage)) {
                return false;
            }
        }

        boolean this_present_throwableMessage = true && this.isSetThrowableMessage();
        boolean that_present_throwableMessage = true && that.isSetThrowableMessage();
        if (this_present_throwableMessage || that_present_throwableMessage) {
            if (!(this_present_throwableMessage && that_present_throwableMessage)) {
                return false;
            }
            if (!this.throwableMessage.equals(that.throwableMessage)) {
                return false;
            }
        }

        boolean this_present_stacktrace = true && this.isSetStacktrace();
        boolean that_present_stacktrace = true && that.isSetStacktrace();
        if (this_present_stacktrace || that_present_stacktrace) {
            if (!(this_present_stacktrace && that_present_stacktrace)) {
                return false;
            }
            if (!this.stacktrace.equals(that.stacktrace)) {
                return false;
            }
        }

        boolean this_present_coreIp = true && this.isSetCoreIp();
        boolean that_present_coreIp = true && that.isSetCoreIp();
        if (this_present_coreIp || that_present_coreIp) {
            if (!(this_present_coreIp && that_present_coreIp)) {
                return false;
            }
            if (!this.coreIp.equals(that.coreIp)) {
                return false;
            }
        }

        boolean this_present_coreHostname = true && this.isSetCoreHostname();
        boolean that_present_coreHostname = true && that.isSetCoreHostname();
        if (this_present_coreHostname || that_present_coreHostname) {
            if (!(this_present_coreHostname && that_present_coreHostname)) {
                return false;
            }
            if (!this.coreHostname.equals(that.coreHostname)) {
                return false;
            }
        }

        boolean this_present_coreType = true && this.isSetCoreType();
        boolean that_present_coreType = true && that.isSetCoreType();
        if (this_present_coreType || that_present_coreType) {
            if (!(this_present_coreType && that_present_coreType)) {
                return false;
            }
            if (!this.coreType.equals(that.coreType)) {
                return false;
            }
        }

        boolean this_present_trace = true && this.isSetTrace();
        boolean that_present_trace = true && that.isSetTrace();
        if (this_present_trace || that_present_trace) {
            if (!(this_present_trace && that_present_trace)) {
                return false;
            }
            if (!this.trace.equals(that.trace)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    public int compareTo(TLoggingEvent other)
    {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;
        TLoggingEvent typedOther = (TLoggingEvent) other;

        lastComparison = Boolean.valueOf(isSetEventDate()).compareTo(typedOther.isSetEventDate());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetEventDate()) {
            lastComparison = TBaseHelper.compareTo(this.eventDate, typedOther.eventDate);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetLevel()).compareTo(typedOther.isSetLevel());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetLevel()) {
            lastComparison = TBaseHelper.compareTo(this.level, typedOther.level);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetMessage()).compareTo(typedOther.isSetMessage());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetMessage()) {
            lastComparison = TBaseHelper.compareTo(this.message, typedOther.message);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetLoggername()).compareTo(typedOther.isSetLoggername());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetLoggername()) {
            lastComparison = TBaseHelper.compareTo(this.loggername, typedOther.loggername);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetLocationFilename()).compareTo(typedOther.isSetLocationFilename());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetLocationFilename()) {
            lastComparison = TBaseHelper.compareTo(this.locationFilename, typedOther.locationFilename);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetLocationMethodName()).compareTo(typedOther.isSetLocationMethodName());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetLocationMethodName()) {
            lastComparison = TBaseHelper.compareTo(this.locationMethodName, typedOther.locationMethodName);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetLocationClassName()).compareTo(typedOther.isSetLocationClassName());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetLocationClassName()) {
            lastComparison = TBaseHelper.compareTo(this.locationClassName, typedOther.locationClassName);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetLocationLineNumber()).compareTo(typedOther.isSetLocationLineNumber());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetLocationLineNumber()) {
            lastComparison = TBaseHelper.compareTo(this.locationLineNumber, typedOther.locationLineNumber);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetNdc()).compareTo(typedOther.isSetNdc());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetNdc()) {
            lastComparison = TBaseHelper.compareTo(this.ndc, typedOther.ndc);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetThreadName()).compareTo(typedOther.isSetThreadName());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetThreadName()) {
            lastComparison = TBaseHelper.compareTo(this.threadName, typedOther.threadName);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetThrowableLocalizedMessage()).compareTo(typedOther.isSetThrowableLocalizedMessage());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetThrowableLocalizedMessage()) {
            lastComparison = TBaseHelper.compareTo(this.throwableLocalizedMessage, typedOther.throwableLocalizedMessage);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetThrowableMessage()).compareTo(typedOther.isSetThrowableMessage());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetThrowableMessage()) {
            lastComparison = TBaseHelper.compareTo(this.throwableMessage, typedOther.throwableMessage);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetStacktrace()).compareTo(typedOther.isSetStacktrace());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetStacktrace()) {
            lastComparison = TBaseHelper.compareTo(this.stacktrace, typedOther.stacktrace);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetCoreIp()).compareTo(typedOther.isSetCoreIp());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetCoreIp()) {
            lastComparison = TBaseHelper.compareTo(this.coreIp, typedOther.coreIp);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetCoreHostname()).compareTo(typedOther.isSetCoreHostname());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetCoreHostname()) {
            lastComparison = TBaseHelper.compareTo(this.coreHostname, typedOther.coreHostname);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetCoreType()).compareTo(typedOther.isSetCoreType());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetCoreType()) {
            lastComparison = TBaseHelper.compareTo(this.coreType, typedOther.coreType);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetTrace()).compareTo(typedOther.isSetTrace());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetTrace()) {
            lastComparison = TBaseHelper.compareTo(this.trace, typedOther.trace);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    public _Fields fieldForId(int fieldId)
    {
        return _Fields.findByThriftId(fieldId);
    }

    public void read(TProtocol iprot) throws TException
    {
        TField field;
        iprot.readStructBegin();
        while (true) {
            field = iprot.readFieldBegin();
            if (field.type == TType.STOP) {
                break;
            }
            switch (field.id) {
                case 1: // EVENT_DATE
                    if (field.type == TType.I64) {
                        this.eventDate = iprot.readI64();
                        setEventDateIsSet(true);
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 2: // LEVEL
                    if (field.type == TType.STRING) {
                        this.level = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 3: // MESSAGE
                    if (field.type == TType.STRING) {
                        this.message = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 4: // LOGGERNAME
                    if (field.type == TType.STRING) {
                        this.loggername = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 5: // LOCATION_FILENAME
                    if (field.type == TType.STRING) {
                        this.locationFilename = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 6: // LOCATION_METHOD_NAME
                    if (field.type == TType.STRING) {
                        this.locationMethodName = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 7: // LOCATION_CLASS_NAME
                    if (field.type == TType.STRING) {
                        this.locationClassName = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 8: // LOCATION_LINE_NUMBER
                    if (field.type == TType.STRING) {
                        this.locationLineNumber = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 9: // NDC
                    if (field.type == TType.STRING) {
                        this.ndc = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 10: // THREAD_NAME
                    if (field.type == TType.STRING) {
                        this.threadName = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 11: // THROWABLE_LOCALIZED_MESSAGE
                    if (field.type == TType.STRING) {
                        this.throwableLocalizedMessage = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 12: // THROWABLE_MESSAGE
                    if (field.type == TType.STRING) {
                        this.throwableMessage = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 13: // STACKTRACE
                    if (field.type == TType.STRING) {
                        this.stacktrace = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 14: // CORE_IP
                    if (field.type == TType.STRING) {
                        this.coreIp = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 15: // CORE_HOSTNAME
                    if (field.type == TType.STRING) {
                        this.coreHostname = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 16: // CORE_TYPE
                    if (field.type == TType.STRING) {
                        this.coreType = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                case 17: // TRACE
                    if (field.type == TType.STRING) {
                        this.trace = iprot.readString();
                    }
                    else {
                        TProtocolUtil.skip(iprot, field.type);
                    }
                    break;
                default:
                    TProtocolUtil.skip(iprot, field.type);
            }
            iprot.readFieldEnd();
        }
        iprot.readStructEnd();

        // check for required fields of primitive type, which can't be checked in the validate method
        validate();
    }

    public void write(TProtocol oprot) throws TException
    {
        validate();

        oprot.writeStructBegin(STRUCT_DESC);
        oprot.writeFieldBegin(EVENT_DATE_FIELD_DESC);
        oprot.writeI64(this.eventDate);
        oprot.writeFieldEnd();
        if (this.level != null) {
            oprot.writeFieldBegin(LEVEL_FIELD_DESC);
            oprot.writeString(this.level);
            oprot.writeFieldEnd();
        }
        if (this.message != null) {
            oprot.writeFieldBegin(MESSAGE_FIELD_DESC);
            oprot.writeString(this.message);
            oprot.writeFieldEnd();
        }
        if (this.loggername != null) {
            oprot.writeFieldBegin(LOGGERNAME_FIELD_DESC);
            oprot.writeString(this.loggername);
            oprot.writeFieldEnd();
        }
        if (this.locationFilename != null) {
            oprot.writeFieldBegin(LOCATION_FILENAME_FIELD_DESC);
            oprot.writeString(this.locationFilename);
            oprot.writeFieldEnd();
        }
        if (this.locationMethodName != null) {
            oprot.writeFieldBegin(LOCATION_METHOD_NAME_FIELD_DESC);
            oprot.writeString(this.locationMethodName);
            oprot.writeFieldEnd();
        }
        if (this.locationClassName != null) {
            oprot.writeFieldBegin(LOCATION_CLASS_NAME_FIELD_DESC);
            oprot.writeString(this.locationClassName);
            oprot.writeFieldEnd();
        }
        if (this.locationLineNumber != null) {
            oprot.writeFieldBegin(LOCATION_LINE_NUMBER_FIELD_DESC);
            oprot.writeString(this.locationLineNumber);
            oprot.writeFieldEnd();
        }
        if (this.ndc != null) {
            oprot.writeFieldBegin(NDC_FIELD_DESC);
            oprot.writeString(this.ndc);
            oprot.writeFieldEnd();
        }
        if (this.threadName != null) {
            oprot.writeFieldBegin(THREAD_NAME_FIELD_DESC);
            oprot.writeString(this.threadName);
            oprot.writeFieldEnd();
        }
        if (this.throwableLocalizedMessage != null) {
            oprot.writeFieldBegin(THROWABLE_LOCALIZED_MESSAGE_FIELD_DESC);
            oprot.writeString(this.throwableLocalizedMessage);
            oprot.writeFieldEnd();
        }
        if (this.throwableMessage != null) {
            oprot.writeFieldBegin(THROWABLE_MESSAGE_FIELD_DESC);
            oprot.writeString(this.throwableMessage);
            oprot.writeFieldEnd();
        }
        if (this.stacktrace != null) {
            oprot.writeFieldBegin(STACKTRACE_FIELD_DESC);
            oprot.writeString(this.stacktrace);
            oprot.writeFieldEnd();
        }
        if (this.coreIp != null) {
            oprot.writeFieldBegin(CORE_IP_FIELD_DESC);
            oprot.writeString(this.coreIp);
            oprot.writeFieldEnd();
        }
        if (this.coreHostname != null) {
            oprot.writeFieldBegin(CORE_HOSTNAME_FIELD_DESC);
            oprot.writeString(this.coreHostname);
            oprot.writeFieldEnd();
        }
        if (this.coreType != null) {
            oprot.writeFieldBegin(CORE_TYPE_FIELD_DESC);
            oprot.writeString(this.coreType);
            oprot.writeFieldEnd();
        }
        if (this.trace != null) {
            oprot.writeFieldBegin(TRACE_FIELD_DESC);
            oprot.writeString(this.trace);
            oprot.writeFieldEnd();
        }
        oprot.writeFieldStop();
        oprot.writeStructEnd();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("TLoggingEvent(");
        boolean first = true;

        sb.append("eventDate:");
        sb.append(this.eventDate);
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("level:");
        if (this.level == null) {
            sb.append("null");
        }
        else {
            sb.append(this.level);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("message:");
        if (this.message == null) {
            sb.append("null");
        }
        else {
            sb.append(this.message);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("loggername:");
        if (this.loggername == null) {
            sb.append("null");
        }
        else {
            sb.append(this.loggername);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("locationFilename:");
        if (this.locationFilename == null) {
            sb.append("null");
        }
        else {
            sb.append(this.locationFilename);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("locationMethodName:");
        if (this.locationMethodName == null) {
            sb.append("null");
        }
        else {
            sb.append(this.locationMethodName);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("locationClassName:");
        if (this.locationClassName == null) {
            sb.append("null");
        }
        else {
            sb.append(this.locationClassName);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("locationLineNumber:");
        if (this.locationLineNumber == null) {
            sb.append("null");
        }
        else {
            sb.append(this.locationLineNumber);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("ndc:");
        if (this.ndc == null) {
            sb.append("null");
        }
        else {
            sb.append(this.ndc);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("threadName:");
        if (this.threadName == null) {
            sb.append("null");
        }
        else {
            sb.append(this.threadName);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("throwableLocalizedMessage:");
        if (this.throwableLocalizedMessage == null) {
            sb.append("null");
        }
        else {
            sb.append(this.throwableLocalizedMessage);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("throwableMessage:");
        if (this.throwableMessage == null) {
            sb.append("null");
        }
        else {
            sb.append(this.throwableMessage);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("stacktrace:");
        if (this.stacktrace == null) {
            sb.append("null");
        }
        else {
            sb.append(this.stacktrace);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("coreIp:");
        if (this.coreIp == null) {
            sb.append("null");
        }
        else {
            sb.append(this.coreIp);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("coreHostname:");
        if (this.coreHostname == null) {
            sb.append("null");
        }
        else {
            sb.append(this.coreHostname);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("coreType:");
        if (this.coreType == null) {
            sb.append("null");
        }
        else {
            sb.append(this.coreType);
        }
        first = false;
        if (!first) {
            sb.append(", ");
        }
        sb.append("trace:");
        if (this.trace == null) {
            sb.append("null");
        }
        else {
            sb.append(this.trace);
        }
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws TException
    {
        // check for required fields
    }

}

