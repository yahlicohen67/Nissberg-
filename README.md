# Nissberg-
G
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.types.Types;
import org.apache.flink.table.data.RowData;

public class IcebergTimeTravelComparison {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TableLoader tableLoader = TableLoader.fromHadoopTable("s3://bucket/path/to/table");
        long twentyFourHoursAgo = System.currentTimeMillis() - (24 * 60 * 60 * 1000);

        // 1. Read Current Snapshot
        IcebergSource<RowData> currentSource = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .build();

        DataStream<RowData> currentStream = env.fromSource(
                currentSource, 
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), 
                "CurrentSource");

        // 2. Read Historical Snapshot (Time Travel)
        IcebergSource<RowData> historySource = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .asOfTimestamp(twentyFourHoursAgo)
                .build();

        DataStream<RowData> historyStream = env.fromSource(
                historySource, 
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), 
                "HistorySource");

        // 3. Join on 'id' and create Comparison Records
        DataStream<ComparisonResult> comparedStream = currentStream
                .join(historyStream)
                .where(row -> row.getString(0).toString()) // Assume index 0 is 'id'
                .equalTo(row -> row.getString(0).toString())
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // Necessary for joins in DataStream
                .apply(new JoinFunction<RowData, RowData, ComparisonResult>() {
                    @Override
                    public ComparisonResult join(RowData current, RowData history) {
                        return new ComparisonResult(
                            current.getString(1).toString(), // externalId
                            current.getString(0).toString(), // id
                            current.getString(2).toString(), // currentVal
                            history.getString(2).toString()  // oldVal
                        );
                    }
                });

        // 4. Efficiency: Key by externalId and perform Windowing
        comparedStream
                .keyBy(ComparisonResult::getExternalId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new YourAggregateFunction()) 
                .print();

        env.execute("Iceberg Time Travel Comparison");
    }
}
