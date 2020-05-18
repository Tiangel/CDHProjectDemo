package com.cloudera.flume.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.flume.sink.KuduOperationsProducer;
import org.apache.kudu.shaded.com.google.common.base.Preconditions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.*;


@InterfaceAudience.Public
@InterfaceStability.Evolving
public class JsonKuduOperationsProducer implements KuduOperationsProducer {

    private static final Logger logger = LoggerFactory.getLogger(JsonKuduOperationsProducer.class);
    private static final String INSERT = "insert";
    private static final String UPSERT = "upsert";
    private static final List<String> validOperations = Lists.newArrayList(UPSERT, INSERT);
    public static final String ENCODING_PROP = "encoding";
    public static final String DEFAULT_ENCODING = "utf-8";
    public static final String OPERATION_PROP = "operation";
    public static final String DEFAULT_OPERATION = UPSERT;

    @Deprecated
    public static final String SKIP_MISSING_COLUMN_PROP = "skipMissingColumn";
    @Deprecated
    public static final boolean DEFAULT_SKIP_MISSING_COLUMN = false;
    @Deprecated
    public static final String SKIP_BAD_COLUMN_VALUE_PROP = "skipBadColumnValue";
    @Deprecated
    public static final boolean DEFAULT_SKIP_BAD_COLUMN_VALUE = false;
    @Deprecated
    public static final String WARN_UNMATCHED_ROWS_PROP = "skipUnmatchedRows";
    @Deprecated
    public static final boolean DEFAULT_WARN_UNMATCHED_ROWS = true;

    public static final String MISSING_COLUMN_POLICY_PROP = "missingColumnPolicy";
    public static final JsonKuduOperationsProducer.ParseErrorPolicy DEFAULT_MISSING_COLUMN_POLICY;
    public static final String BAD_COLUMN_VALUE_POLICY_PROP = "badColumnValuePolicy";
    public static final JsonKuduOperationsProducer.ParseErrorPolicy DEFAULT_BAD_COLUMN_VALUE_POLICY;
    public static final String UNMATCHED_ROW_POLICY_PROP = "unmatchedRowPolicy";
    public static final JsonKuduOperationsProducer.ParseErrorPolicy DEFAULT_UNMATCHED_ROW_POLICY;

    private KuduTable table;
    private Charset charset;
    private String operation;

    private JsonKuduOperationsProducer.ParseErrorPolicy missingColumnPolicy;
    private JsonKuduOperationsProducer.ParseErrorPolicy badColumnValuePolicy;
    private JsonKuduOperationsProducer.ParseErrorPolicy unmatchedRowPolicy;


    public JsonKuduOperationsProducer() {

    }


    @Override
    public void configure(Context context) {

        String charsetName = context.getString(ENCODING_PROP, DEFAULT_ENCODING);
        try {
            charset = Charset.forName(charsetName);
        } catch (IllegalArgumentException e) {
            throw new FlumeException(
                    String.format("Invalid or unsupported charset %s", charsetName), e);
        }
        this.operation = context.getString(OPERATION_PROP, DEFAULT_OPERATION).toLowerCase();
        Preconditions.checkArgument(validOperations.contains(this.operation),
                "Unrecognized operation '%s'", this.operation);

        this.missingColumnPolicy = this.getParseErrorPolicyCheckingDeprecatedProperty(
                context, SKIP_MISSING_COLUMN_PROP, MISSING_COLUMN_POLICY_PROP,
                JsonKuduOperationsProducer.ParseErrorPolicy.WARN,
                JsonKuduOperationsProducer.ParseErrorPolicy.REJECT, DEFAULT_MISSING_COLUMN_POLICY);
        this.badColumnValuePolicy = this.getParseErrorPolicyCheckingDeprecatedProperty(
                context, SKIP_BAD_COLUMN_VALUE_PROP, BAD_COLUMN_VALUE_POLICY_PROP,
                JsonKuduOperationsProducer.ParseErrorPolicy.WARN,
                JsonKuduOperationsProducer.ParseErrorPolicy.REJECT, DEFAULT_BAD_COLUMN_VALUE_POLICY);
        this.unmatchedRowPolicy = this.getParseErrorPolicyCheckingDeprecatedProperty(
                context, WARN_UNMATCHED_ROWS_PROP, UNMATCHED_ROW_POLICY_PROP,
                JsonKuduOperationsProducer.ParseErrorPolicy.WARN,
                JsonKuduOperationsProducer.ParseErrorPolicy.IGNORE, DEFAULT_UNMATCHED_ROW_POLICY);
    }

    @Override
    public void initialize(KuduTable kuduTable) {
        this.table = kuduTable;
    }

    @Override
    public List<Operation> getOperations(Event event) throws FlumeException {
        String raw = new String(event.getBody(), charset);
        Map<String, String> rawMap = JSON.parseObject(raw, HashMap.class);
        Schema schema = this.table.getSchema();
        List<Operation> ops = Lists.newArrayList();
        if (raw != null && !raw.isEmpty()) {
            Operation op;
            switch (operation) {
                case UPSERT:
                    op = this.table.newUpsert();
                    break;
                case INSERT:
                    op = this.table.newInsert();
                    break;
                default:
                    throw new FlumeException(
                            String.format("Unrecognized operation type '%s' in getOperations(): " +
                                    "this should never happen!", this.operation));
            }
            PartialRow row = op.getRow();

            Iterator iterator = schema.getColumns().iterator();
            while (iterator.hasNext()) {
                ColumnSchema col = (ColumnSchema) iterator.next();
//                logger.error("Column:" + col.getName() + "----" + rawMap.get(col.getName()) + "----" + col.getType());
                String msg;
                try {
                    this.coerceAndSet(rawMap.get(col.getName()), col.getName(), col.getType(), row);
                } catch (NumberFormatException e) {
                    msg = String.format("Raw value '%s' couldn't be parsed to type %s for column '%s'",
                            raw, col.getType(), col.getName());
                    this.logOrThrow(this.badColumnValuePolicy, msg, e);
                } catch (IllegalArgumentException e) {
                    msg = String.format("Column '%s' has no matching group in '%s'", col.getName(), raw);
                    this.logOrThrow(this.missingColumnPolicy, msg, e);
                } catch (Exception e) {
                    throw new FlumeException("Failed to create Kudu operation", e);
                }
            }
            ops.add(op);
        }
        return ops;
    }

    private void coerceAndSet(String rawVal, String colName, Type type, PartialRow row) throws NumberFormatException {
        switch (type) {
            case BOOL:
                row.addBoolean(colName, Boolean.parseBoolean(rawVal));
                break;
            case INT8:
                row.addByte(colName, Byte.parseByte(rawVal));
                break;
            case INT16:
                row.addShort(colName, Short.parseShort(rawVal));
                break;
            case INT32:
                row.addInt(colName, Integer.parseInt(rawVal));
                break;
            case INT64:
            case UNIXTIME_MICROS:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            case FLOAT:
                row.addFloat(colName, Float.parseFloat(rawVal));
                break;
            case DOUBLE:
                row.addDouble(colName, Double.parseDouble(rawVal));
                break;
            case DECIMAL:
                row.addDecimal(colName, new BigDecimal(rawVal));
                break;
            case BINARY:
                row.addBinary(colName, rawVal.getBytes(this.charset));
                break;
            case STRING:
                row.addString(colName, rawVal == null ? "" : rawVal);
                break;
            default:
                logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
        }

    }

    private void logOrThrow(JsonKuduOperationsProducer.ParseErrorPolicy policy, String msg, Exception e) throws FlumeException {
        switch (policy) {
            case REJECT:
                throw new FlumeException(msg, e);
            case WARN:
                logger.warn(msg, e);
            case IGNORE:
            default:
        }
    }

    @Override
    public void close() {

    }

    private JsonKuduOperationsProducer.ParseErrorPolicy getParseErrorPolicyCheckingDeprecatedProperty(
            Context context, String deprecatedPropertyName, String newPropertyName,
            JsonKuduOperationsProducer.ParseErrorPolicy trueValue,
            JsonKuduOperationsProducer.ParseErrorPolicy falseValue,
            JsonKuduOperationsProducer.ParseErrorPolicy defaultValue) {
        JsonKuduOperationsProducer.ParseErrorPolicy policy;
        if (context.containsKey(deprecatedPropertyName)) {
            logger.info("Configuration property {} is deprecated. Use {} instead.", deprecatedPropertyName, newPropertyName);
            Preconditions.checkArgument(!context.containsKey(newPropertyName), "Both {} and {} specified. Use only one of them, preferably {}.", deprecatedPropertyName, newPropertyName, newPropertyName);
            policy = context.getBoolean(deprecatedPropertyName) ? trueValue : falseValue;
        } else {
            String policyString = context.getString(newPropertyName, defaultValue.name());
            try {
                policy = JsonKuduOperationsProducer.ParseErrorPolicy.valueOf(policyString.toUpperCase());
            } catch (IllegalArgumentException var10) {
                throw new IllegalArgumentException("Unknown policy '" + policyString + "'. Use one of the following: " + Arrays.toString(JsonKuduOperationsProducer.ParseErrorPolicy.values()), var10);
            }
        }
        return policy;
    }

    static {
        DEFAULT_MISSING_COLUMN_POLICY = JsonKuduOperationsProducer.ParseErrorPolicy.REJECT;
        DEFAULT_BAD_COLUMN_VALUE_POLICY = JsonKuduOperationsProducer.ParseErrorPolicy.REJECT;
        DEFAULT_UNMATCHED_ROW_POLICY = JsonKuduOperationsProducer.ParseErrorPolicy.WARN;
    }

    private static enum ParseErrorPolicy {
        WARN,
        IGNORE,
        REJECT;

        private ParseErrorPolicy() {
        }
    }
}
