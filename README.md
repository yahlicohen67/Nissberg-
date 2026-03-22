import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

public class IcebergTimeTravelComparison {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set to BATCH for efficient Sort-Merge Joins instead of hash joins
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        TableLoader tableLoader = TableLoader.fromHadoopTable("s3://bucket/path/to/table");
        tableLoader.open();
        Schema schema = tableLoader.loadTable().schema();

        // Dynamically find positions from schema
        int idIdx = schema.caseInsensitiveFindField("id").fieldId();
        int extIdIdx = schema.caseInsensitiveFindField("externalId").fieldId();
        
        // Field Getters for high performance
        RowData.FieldGetter idGetter = RowData.createFieldGetter(Types.StringType.get(), idIdx);
        RowData.FieldGetter extIdGetter = RowData.createFieldGetter(Types.StringType.get(), extIdIdx);

        long twentyFourHoursAgo = System.currentTimeMillis() - (24 * 60 * 60 * 1000);

        // 1. & 2. Sources (Current & History)
        IcebergSource<RowData> currentSource = IcebergSource.forRowData().tableLoader(tableLoader).build();
        IcebergSource<RowData> historySource = IcebergSource.forRowData().tableLoader(tableLoader)
                .asOfTimestamp(twentyFourHoursAgo).build();

        DataStream<RowData> currentStream = env.fromSource(currentSource, 
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Current");
        DataStream<RowData> historyStream = env.fromSource(historySource, 
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "History");

        // 3. Join on 'id'
        DataStream<ComparisonResult> comparedStream = currentStream
                .join(historyStream)
                .where(row -> idGetter.getFieldOrNull(row).toString())
                .equalTo(row -> idGetter.getFieldOrNull(row).toString())
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) 
                .apply(new JoinFunction<RowData, RowData, ComparisonResult>() {
                    @Override
                    public ComparisonResult join(RowData cur, RowData hist) {
                        // Map all fields into a comparison object
                        return new ComparisonResult(
                            extIdGetter.getFieldOrNull(cur).toString(),
                            idGetter.getFieldOrNull(cur).toString(),
                            extractDataFields(cur, schema), // Current values
                            extractDataFields(hist, schema) // Old values
                        );
                    }
                });

        // 4. KeyBy externalId + Process for Elasticsearch Output
        comparedStream
                .keyBy(ComparisonResult::getExternalId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessWindowFunction<ComparisonResult, Map<String, Object>, String, TimeWindow>() {
                    @Override
                    public void process(String extId, Context context, Iterable<ComparisonResult> elements, Collector<Map<String, Object>> out) {
                        for (ComparisonResult record : elements) {
                            // Only emit if there is an actual difference in data
                            if (!record.getCurrentFields().equals(record.getOldFields())) {
                                Map<String, Object> esDoc = new HashMap<>();
                                esDoc.put("id", record.getId());
                                esDoc.put("externalId", extId);
                                esDoc.put("current_data", record.getCurrentFields());
                                esDoc.put("old_data", record.getOldFields());
                                out.collect(esDoc);
                            }
                        }
                    }
                })
                .print(); // Replace with your Elasticsearch Sink

        env.execute("Iceberg Time Travel Comparison");
    }

    private static Map<String, String> extractDataFields(RowData row, Schema schema) {
        Map<String, String> data = new HashMap<>();
        for (Types.NestedField field : schema.columns()) {
            // Skip the keys, grab the actual data
            if (!field.name().equals("id") && !field.name().equals("externalId")) {
                Object val = RowData.createFieldGetter(field.type(), field.fieldId()).getFieldOrNull(row);
                data.put(field.name(), val != null ? val.toString() : null);
            }
        }
        return data;
    }
}
