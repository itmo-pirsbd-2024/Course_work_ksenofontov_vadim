/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.trying2;



import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.*;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static java.lang.Math.abs;
import static java.lang.Math.sqrt;


/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {
	/**
	 * The accumulator is used to keep a running sum and a count. The {@code getResult} method
	 * computes the average.
	 */
	// pulse <-------------
	private static class AveragePulseAggregate
			implements AggregateFunction<PulseRecord, Tuple3<Long, Long, Long>, AveragePulseRecord> {
		@Override
		public Tuple3<Long, Long, Long> createAccumulator() {
			return new Tuple3<>(0L, 0L, 0L);
		}

		@Override
		public Tuple3<Long, Long, Long> add(PulseRecord value, Tuple3<Long, Long, Long> accumulator) {
			return new Tuple3<>(value.user_id, accumulator.f1 + value.pulse_value, accumulator.f2 + 1L);
		}

		@Override
		public AveragePulseRecord getResult(Tuple3<Long, Long, Long> accumulator) {
			var res = new AveragePulseRecord();
			res.user_id = accumulator.f0;
			res.pulse_value = ((double) accumulator.f1) / accumulator.f2;
			return res;
		}

		@Override
		public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> a, Tuple3<Long, Long, Long> b) {
			return new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
		}
	}
	// -------------> pulse


	// avg speed <-------------
	private static class AverageSpeedAggregate
			implements AggregateFunction<LocationRecord,
			Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer>,
			// user_id, previous timestamp ms, total ms, total move, previous x, previous y, previous z, current x, current y, current z
			AverageSpeedVectorRecord> {
		@Override
		public Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer>createAccumulator() {
			return new Tuple10<>(0L, 0L, 0L, 0., 0, 0, 0, 0, 0, 0);
		}

		@Override
		public Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer>
		add(LocationRecord value, Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer> accumulator) {

			//System.out.println("prev: " + accumulator.f1 + ", cur: " + value.time_stamp_in_ms);

			var current_time_delta = accumulator.f2 + (value.time_stamp_in_ms - accumulator.f1);
			var current_move_delta = sqrt(((double)value.x - accumulator.f7) * ((double)value.x - accumulator.f7) +
					((double)value.y - accumulator.f8) * ((double)value.y - accumulator.f8) +
					((double)value.z - accumulator.f9) * ((double)value.z - accumulator.f9));

			if (accumulator.f1 == 0L) {
				current_time_delta = 0L;
				current_move_delta = 0.;
			}

			return new Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer>(
					value.user_id, value.time_stamp_in_ms, current_time_delta,
					accumulator.f3 + current_move_delta,
					accumulator.f7, accumulator.f8, accumulator.f9,
					value.x, value.y, value.z);
		}

		@Override
		public AverageSpeedVectorRecord getResult(Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer> accumulator) {
			var res = new AverageSpeedVectorRecord();
			res.user_id = accumulator.f0;
			res.average_scalar_speed_in_m_per_s = (accumulator.f3 * 100) / accumulator.f2;

			double last_segment_length = sqrt(((double)accumulator.f7 - accumulator.f4) * ((double)accumulator.f7 - accumulator.f4) +
					((double)accumulator.f8 - accumulator.f5) * ((double)accumulator.f8 - accumulator.f5) +
					((double)accumulator.f9 - accumulator.f6) * ((double)accumulator.f9 - accumulator.f6));

			res.x_delta_normalised = ((double)accumulator.f7 - accumulator.f4) / last_segment_length;
			res.y_delta_normalised = ((double)accumulator.f8 - accumulator.f5) / last_segment_length;
			res.z_delta_normalised = ((double)accumulator.f9 - accumulator.f6) / last_segment_length;

			return res;
		}

		@Override
		public Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer>
		merge(Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer> a,
			  Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer> b) {

			return new Tuple10<Long, Long, Long, Double, Integer, Integer, Integer, Integer, Integer, Integer>(
					a.f0, 0L, 0L, 0., 0, 0, 0, 0, 0, 0
			);
		}
	}
	// -------------> avg speed


	// motion <-------------
	public static class FirstMotionPointExtractor
            implements WindowFunction<MotionPointRecord, MotionPointRecord, Long, Window> {
		@Override
		public void apply(Long aLong, Window window, Iterable<MotionPointRecord> input, Collector<MotionPointRecord> out) throws Exception {
			out.collect(input.iterator().next());
		}
	}
	// -------------> motion


	// sectors <-------------
	static public Integer max_number_of_sectors = 3;

	public static class SectorsFullnessComparator implements Comparator<Long> {

		@Override
		public int compare(Long sec1, Long sec2) {
			int value = sec1.compareTo(sec2);
			//sorting elements from maximal to minimal
            return Integer.compare(value, 0);
		}
	}

	public static class SectorsProcessor extends ProcessFunction<Tuple2<Long, Long>, String> {
		static TreeSet<Long> SectorsSet = new TreeSet<>(new SectorsFullnessComparator());
		static HashMap<Long, HashSet<Long>> fullnessToSectorsMap = new HashMap<>();
		static HashMap<Long, Long> sectorToFullnessMap = new HashMap<>();

		static long current_number_of_stored_sectors = 0;

		void processCandidateToMostFullSectors(Tuple2<Long, Long> sector) {
			if (sectorToFullnessMap.containsKey(sector.f0)) {
				fullnessToSectorsMap.get(sectorToFullnessMap.get(sector.f0)).remove(sector.f0);

				if (fullnessToSectorsMap.get(sectorToFullnessMap.get(sector.f0)).isEmpty()) {
					fullnessToSectorsMap.remove(sectorToFullnessMap.get(sector.f0));
					SectorsSet.remove(sectorToFullnessMap.get(sector.f0));
				}

				sectorToFullnessMap.remove(sector.f0);
				current_number_of_stored_sectors--;
			}

			if (current_number_of_stored_sectors == max_number_of_sectors) {
				var minFullness = SectorsSet.first();

				if (minFullness <= sector.f1) {
					var sectorToRemove = fullnessToSectorsMap.get(minFullness).iterator().next();
					fullnessToSectorsMap.get(minFullness).remove(sectorToRemove);
					sectorToFullnessMap.remove(sectorToRemove);
					current_number_of_stored_sectors--;

					if (fullnessToSectorsMap.get(minFullness).isEmpty()) {
						fullnessToSectorsMap.remove(minFullness);
						SectorsSet.remove(minFullness);
					}
				}
			}

			if (current_number_of_stored_sectors < max_number_of_sectors) {
				sectorToFullnessMap.put(sector.f0, sector.f1);

				if (fullnessToSectorsMap.containsKey(sector.f1) == false) {
					fullnessToSectorsMap.put(sector.f1, new HashSet<>());
					SectorsSet.add(sector.f1);
				}

				fullnessToSectorsMap.get(sector.f1).add(sector.f0);
				current_number_of_stored_sectors++;
			}
		}

		@Override
		public void processElement(
				Tuple2<Long, Long> inputEntry,
				ProcessFunction<Tuple2<Long, Long>, String>.Context context,
				Collector<String> collector) throws Exception {

			processCandidateToMostFullSectors(inputEntry);

			var res = new String();

			var resobj = new MostFilledSectorsRecord();

			for (var fullness : SectorsSet) {
				for (Long sector_with_fullness : fullnessToSectorsMap.get(fullness)) {
					resobj.sectors.add(new Tuple3<Integer, Integer, Long>((int) ((sector_with_fullness << 32) >> 32), (int) (sector_with_fullness >> 32), fullness));
				}
			}

			System.out.println("<");
			for (var obj : resobj.sectors) {
				System.out.println("[ " + obj.f0 + ", " + obj.f1 + " ] : " + obj.f2);
			}
			System.out.println(">");


			/*res.sectors = new Tuple2[]
			var counter = 0;
			for (var fullness : SectorsSet){
				for (var sector_with_fullness : fullnessToSectorsMap.get(fullness)) {
					counter++;
				}
			}

			res.sectors = new Tuple2[counter];

			counter = 0;

			for (var fullness : SectorsSet){
				for (var sector_with_fullness : fullnessToSectorsMap.get(fullness)) {
					res.sectors[counter++] = new Tuple2<>(sector_with_fullness, fullness);
				}
			}*/



	var val_bytes = resobj.serializeToBytes();
	//System.out.println("size of top sectors: " + val_bytes.length);
	res = new String(val_bytes, "UTF-8");

			collector.collect(res);
}
	}
	// -------------> sectors

	///////////
	/*private static class ProcessString1 extends ProcessFunction<String, String> {

		@Override
		public void processElement(
				String val,
				ProcessFunction<String, String>.Context context,
				Collector<String> collector) throws Exception {

			collector.collect(val + '1');
			System.out.println(val + '1');
		}
	}

	private static class ProcessString2 extends ProcessFunction<String, String> {

		@Override
		public void processElement(
				String val,
				ProcessFunction<String, String>.Context context,
				Collector<String> collector) throws Exception {

			collector.collect(val + '2');
			System.out.println(val + '2');
		}
	}*/
	///////////

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// pulse <-------------
		KafkaSource<PulseRecord> pulse_source = KafkaSource.<PulseRecord>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setTopics("pulse")
//				.setStartingOffsets(OffsetsInitializer.earliest())
				.setStartingOffsets(OffsetsInitializer.latest())
				.setDeserializer(new PulseRecord())
				.build();

		DataStream<AveragePulseRecord> processed_pulse = env.fromSource(pulse_source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "pulse")
				.returns(PulseRecord.class)
				.keyBy(PulseRecord::getUserId)
//				.countWindow(10, 1)
				.window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(60), Duration.ofSeconds(1)))
				.aggregate(new AveragePulseAggregate());

		KafkaSink<AveragePulseRecord> pulse_sink = KafkaSink.<AveragePulseRecord>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setRecordSerializer(new AveragePulseRecord())
				.setDeliveryGuarantee(DeliveryGuarantee.NONE)
				.build();

		processed_pulse.sinkTo(pulse_sink);
		// -------------> pulse


		// location
		KafkaSource<LocationRecord> location_source = KafkaSource.<LocationRecord>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setTopics("location")
//				.setStartingOffsets(OffsetsInitializer.earliest())
				.setStartingOffsets(OffsetsInitializer.latest())
				.setDeserializer(new LocationRecord())
				.build();

		// avg speed <-------------
		DataStream<AverageSpeedVectorRecord> average_speed_vector_stream = env.fromSource(location_source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "location")
				.returns(LocationRecord.class)
				.keyBy(LocationRecord::getUserId)
				.window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(1)))
//				.countWindow(10, 1)
				.aggregate(new AverageSpeedAggregate());

		KafkaSink<AverageSpeedVectorRecord> average_speed_sink = KafkaSink.<AverageSpeedVectorRecord>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setRecordSerializer(new AverageSpeedVectorRecord())
				.setDeliveryGuarantee(DeliveryGuarantee.NONE)
				.build();

		average_speed_vector_stream.sinkTo(average_speed_sink);
		// -------------> avg speed

		// motion <-------------
		DataStream<MotionPointRecord> motion_points_stream = env.fromSource(location_source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "location")
				.returns(LocationRecord.class)
				.map(new MapFunction<LocationRecord, MotionPointRecord>() {
					@Override
					public MotionPointRecord map(LocationRecord value) throws Exception {
						var motion_point = new MotionPointRecord();
						motion_point.user_id = value.user_id;
						motion_point.x = value.x;
						motion_point.y = value.y;
						motion_point.z = value.z;

						return motion_point;
					}
				})
				.keyBy(MotionPointRecord::getUserId)
				.window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(10)))
//				.countWindow(10)
				.evictor(CountEvictor.of(1))
				.reduce(new ReduceFunction<MotionPointRecord>() {
					public MotionPointRecord reduce(MotionPointRecord v1, MotionPointRecord v2) {
						return v1;
					}
				});

		KafkaSink<MotionPointRecord> motion_sink = KafkaSink.<MotionPointRecord>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setRecordSerializer(new MotionPointRecord())
				.setDeliveryGuarantee(DeliveryGuarantee.NONE)
				.build();

		motion_points_stream.sinkTo(motion_sink);
		// -------------> motion

		// sectors <-------------
		DataStream<String> sector_fullness_stream = env.fromSource(location_source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "location")
				.returns(LocationRecord.class)
				.map(new MapFunction<LocationRecord, Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Long> map(LocationRecord value) throws Exception {
						return new Tuple2<Long, Long>(value.getSectorId(), 1L);
					}
				})
				.keyBy(t -> t.f0)
				.window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(5), Duration.ofSeconds(5)))
				.sum(1)
				.process(new SectorsProcessor());

		KafkaSink<String> sectors_fullness_sink = KafkaSink.<String>builder()
				.setBootstrapServers("127.0.0.1:9092")
//				.setRecordSerializer(new MostFilledSectorsRecord())

				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic("most_filled_sectors")
						.setKeySerializationSchema(new SimpleStringSchema())
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.NONE)
				.build();

		sector_fullness_stream.sinkTo(sectors_fullness_sink);
		// -------------> sectors


		/*KafkaSource<String> pulse_source = KafkaSource.<String>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setTopics("test")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> processed_pulse = env.fromSource(pulse_source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)), "pulse")
				//.windowAll(SlidingProcessingTimeWindows.of(Duration.ofSeconds(5), Duration.ofSeconds(1)))
				.windowAll(SlidingProcessingTimeWindows.of(Duration.ofSeconds(5), Duration.ofSeconds(5)))
				//.countWindowAll(5)
				.reduce(new ReduceFunction<String>() {
					public String reduce(String v1, String v2) {
						return (v1 + "_" + v2);
					}
				});

		KafkaSink<String> sink = KafkaSink.<String>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic("test2")
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.NONE)
				.build();


		processed_pulse.sinkTo(sink);*/

		/*KafkaSource<String> pulse_source = KafkaSource.<String>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setTopics("test")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> test_processed1 = env.fromSource(pulse_source, WatermarkStrategy.noWatermarks(), "test")
				.process(new ProcessString1());

		DataStream<String> test_processed2 = env.fromSource(pulse_source, WatermarkStrategy.noWatermarks(), "test")
				.process(new ProcessString2());*/

		/*DataStream<Integer> processed_pulse = env.fromData(2, 5, 1,
						3, 5, 4,
						2, 9, 8,
						4, 1, 5,
						9, 3, 1)

				.setParallelism(1)
				.countWindowAll(3, 1)
				.sum("0")
				.process(new PulseProcessor())
				.setParallelism(1);


		KafkaSink<Integer> sink = KafkaSink.<Integer>builder()
				.setBootstrapServers("127.0.0.1:9092")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic("pulse")
						.setKafkaValueSerializer(IntegerSerializer.class)
						.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.NONE)
				.build();


		processed_pulse.sinkTo(sink).setParallelism(1);*/

		/*processCandidateToMostFullSectors(new Tuple2<Long, Long>(10L, 50L));
		System.out.println("SectorsSet: " + SectorsSet);
		System.out.println("fullnessToSectorsMap: " + fullnessToSectorsMap);
		System.out.println("sectorToFullnessMap: " + sectorToFullnessMap);
		System.out.println("current_number_of_stored_sectors: " + current_number_of_stored_sectors);

		processCandidateToMostFullSectors(new Tuple2<Long, Long>(10L, 5L));
		System.out.println("SectorsSet: " + SectorsSet);
		System.out.println("fullnessToSectorsMap: " + fullnessToSectorsMap);
		System.out.println("sectorToFullnessMap: " + sectorToFullnessMap);
		System.out.println("current_number_of_stored_sectors: " + current_number_of_stored_sectors);

		processCandidateToMostFullSectors(new Tuple2<Long, Long>(11L, 2L));
		System.out.println("SectorsSet: " + SectorsSet);
		System.out.println("fullnessToSectorsMap: " + fullnessToSectorsMap);
		System.out.println("sectorToFullnessMap: " + sectorToFullnessMap);
		System.out.println("current_number_of_stored_sectors: " + current_number_of_stored_sectors);

		processCandidateToMostFullSectors(new Tuple2<Long, Long>(12L, 43L));
		System.out.println("SectorsSet: " + SectorsSet);
		System.out.println("fullnessToSectorsMap: " + fullnessToSectorsMap);
		System.out.println("sectorToFullnessMap: " + sectorToFullnessMap);
		System.out.println("current_number_of_stored_sectors: " + current_number_of_stored_sectors);

		processCandidateToMostFullSectors(new Tuple2<Long, Long>(13L, 12L));
		System.out.println("SectorsSet: " + SectorsSet);
		System.out.println("fullnessToSectorsMap: " + fullnessToSectorsMap);
		System.out.println("sectorToFullnessMap: " + sectorToFullnessMap);
		System.out.println("current_number_of_stored_sectors: " + current_number_of_stored_sectors);

		processCandidateToMostFullSectors(new Tuple2<Long, Long>(14L, 2L));
		System.out.println("SectorsSet: " + SectorsSet);
		System.out.println("fullnessToSectorsMap: " + fullnessToSectorsMap);
		System.out.println("sectorToFullnessMap: " + sectorToFullnessMap);
		System.out.println("current_number_of_stored_sectors: " + current_number_of_stored_sectors);

		processCandidateToMostFullSectors(new Tuple2<Long, Long>(14L, 12L));
		System.out.println("SectorsSet: " + SectorsSet);
		System.out.println("fullnessToSectorsMap: " + fullnessToSectorsMap);
		System.out.println("sectorToFullnessMap: " + sectorToFullnessMap);
		System.out.println("current_number_of_stored_sectors: " + current_number_of_stored_sectors);*/

		env.execute("Processor");
	}
}
