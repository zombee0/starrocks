package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TIcebergTableSink;
import com.starrocks.thrift.TParquetDataType;
import com.starrocks.thrift.TParquetRepetitionType;
import com.starrocks.thrift.TParquetSchema;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Locale;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT;

public class IcebergTableSink extends DataSink {
    private IcebergTable icebergTable;
    private final String table;
    private final String location;
    private final String fileFormat;
    private final String compressionCodec;
    private String fileNamePrefix;
    // set after init called
    private TDataSink tDataSink;
    private int tupleId;

    public IcebergTableSink(IcebergTable icebergTable, int id) {
        this.tupleId = id;
        this.icebergTable = icebergTable;
        table = icebergTable.getTable();
        org.apache.iceberg.Table table = icebergTable.getIcebergTable();
        location = table.location();
        fileFormat = table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT).toLowerCase();
        compressionCodec = table.properties().getOrDefault(PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT).toLowerCase(Locale.ROOT);

        tDataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
        TIcebergTableSink tIcebergTableSink = new TIcebergTableSink();
        tIcebergTableSink.setLocation(location);
        tIcebergTableSink.setFile_format(fileFormat);
        tIcebergTableSink.setTarget_table_id(icebergTable.getId());
        tIcebergTableSink.setCompression_codec(compressionCodec);
        tIcebergTableSink.setParquet_schemas(generateParquetSchema(table.schema()));
        tIcebergTableSink.setPartition_index(generateIndexs());
        tDataSink.setIceberg_table_sink(tIcebergTableSink);
    }

    private List<Integer> generateIndexs() {
        List<Integer> indexs = Lists.newArrayList();
        List<String> partitionColumnNames = icebergTable.getPartitionColumnNames();
        List<Types.NestedField> fields = icebergTable.getIcebergTable().schema().columns();
        for (int i = 0; i < fields.size(); i++) {
            if (partitionColumnNames.contains(fields.get(i).name())) {
                indexs.add(i);
            }
        }
        return indexs;
    }

    private List<TParquetSchema> generateParquetSchema(Schema schema) {
        List<TParquetSchema> parquetSchemas = Lists.newArrayList();
        for (Types.NestedField field : schema.columns()) {
            TParquetSchema parquetSchema = new TParquetSchema();
            TParquetRepetitionType repetitionType = field.isOptional() ? TParquetRepetitionType.OPTIONAL : TParquetRepetitionType.REQUIRED;
            parquetSchema.setSchema_repetition_type(repetitionType);
            int id = field.fieldId();
            parquetSchema.setField_id(id);
            parquetSchema.setSchema_column_name(field.name());

            TParquetDataType type;
            switch (field.type().asPrimitiveType().typeId()) {
                case BOOLEAN:
                    type = TParquetDataType.BOOLEAN;
                    break;
                case INTEGER:
                    type = TParquetDataType.INT32;
                    break;
                case LONG:
                    type = TParquetDataType.INT64;
                    break;
                case FLOAT:
                    type = TParquetDataType.FLOAT;
                    break;
                case STRING:
                    type = TParquetDataType.BYTE_ARRAY;
                    break;
                default:
                    throw new StarRocksConnectorException("xxxx not support");
            }
            parquetSchema.setSchema_data_type(type);

            parquetSchemas.add(parquetSchema);
        }
        return parquetSchemas;
    }

    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        return "xxx";
    }

    @Override
    public TDataSink toThrift() {
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }

    public boolean canUsePipeLine() {
        return Config.enable_pipeline_load;
    }
}
