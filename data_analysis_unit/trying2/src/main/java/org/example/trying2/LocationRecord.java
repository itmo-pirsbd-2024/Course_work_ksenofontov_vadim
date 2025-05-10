package org.example.trying2;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.time.LocalDateTime;

public class LocationRecord implements KafkaRecordDeserializationSchema<LocationRecord>, KafkaRecordSerializationSchema<LocationRecord> {

    static int x_min = 0; // decimeters
    static int y_min = 0; // decimeters
    static int z_min = 0; // decimeters
    static int sector_side_length = 100 * (10); // meters

    public Long user_id;
    public Integer x;
    public Integer y;
    public Integer z;
    public Long time_stamp_in_ms;

    long getUserId() {
        return user_id;
    }

    long getSectorId() {
        long x_comp = ((long)(x - x_min)) / sector_side_length;
        long y_comp = ((long)(y - y_min)) / sector_side_length;

        return x_comp + (y_comp << 32);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<LocationRecord> out) {

        ByteBuffer key = ByteBuffer.wrap(message.key()); // big-endian by default
        ByteBuffer value = ByteBuffer.wrap(message.value()); // big-endian by default

        var res = new LocationRecord();
        res.user_id = key.getLong();
        res.x = value.getInt();
        res.y = value.getInt();
        res.z = value.getInt();
        res.time_stamp_in_ms = value.getLong();

        out.collect(res);
    }

    @Override
    public TypeInformation<LocationRecord> getProducedType() {
        return null;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(LocationRecord pulseRecord, KafkaSinkContext kafkaSinkContext, Long aLong) {
        var key_bytes = ByteBuffer.allocate(8).putLong(pulseRecord.user_id).array();
        var val_bytes = ByteBuffer.allocate(4)
                .putInt(pulseRecord.x)
                .putInt(pulseRecord.y)
                .putInt(pulseRecord.z)
                .putLong(pulseRecord.time_stamp_in_ms)
                .array();

        return new ProducerRecord<>("", key_bytes, val_bytes);
    }
}