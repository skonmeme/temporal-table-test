package aa

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.shaded.org.joda.time.DateTime
import org.apache.flink.types.Row
import org.apache.flink.util.Collector


object Main {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(50L)
    val tEnv = StreamTableEnvironment.create(env)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val oldStream = env.addSource(new SourceFunction[(String, Long, Long)] {
      override def run(ctx: SourceFunction.SourceContext[(String, Long, Long)]): Unit = {
        ctx.collect(("0", -1, Long.MinValue))
        while (true) {
          Thread.sleep(10000L)
          ctx.collect(("0", -1, DateTime.now.getMillis))
        }
      }
      override def cancel(): Unit = {}
    })

    val specStream = env
      .fromElements(("0", 1, new Timestamp(0L)), ("1", 3, new Timestamp(10L)))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int, Timestamp)](Time.seconds(1L)) {
        override def extractTimestamp(element: (String, Int, Timestamp)): Long = element._3.getTime
      })
    val specTable = tEnv.fromDataStream(specStream, 'p_key, 'p_value, 'p_time.rowtime)
      .createTemporalTableFunction('p_time, 'p_key)
    tEnv.registerFunction("spec", specTable)

    val newStream = env
      .addSource(new SourceFunction[(String, Long, Long)] {
        override def run(ctx: SourceFunction.SourceContext[(String, Long, Long)]): Unit = {
          var count: Long = 0
          while (true) {
            val time = DateTime.now.getMillis
            ctx.collect(("0", count, time))
            ctx.collect(("1", count, time))
            Thread.sleep(100L)
            count += 1
          }
        }
        override def cancel(): Unit = {}
      })
      .connect(oldStream)
      .map((m1: (String, Long, Long)) => m1, (m2: (String, Long, Long)) => m2)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Long)](Time.seconds(1L)) {
        override def extractTimestamp(element: (String, Long, Long)): Long = element._3
      })
    val newTable = tEnv.fromDataStream(newStream, 'key, 'count, 'time.rowtime)


    val joinedTable = newTable
      .joinLateral("spec(time)", "key = p_key")

    tEnv.toAppendStream[Row](joinedTable).print

    env.execute()
  }
}