package org.example.trying2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
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
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

public class MotionPointRecord implements KafkaRecordDeserializationSchema<MotionPointRecord>, KafkaRecordSerializationSchema<MotionPointRecord> {
    public Long user_id;
    public Integer x;
    public Integer y;
    public Integer z;

    long getUserId() {
        return user_id;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<MotionPointRecord> out) {

        ByteBuffer key = ByteBuffer.wrap(message.key()); // big-endian by default
        ByteBuffer value = ByteBuffer.wrap(message.value()); // big-endian by default

        var res = new MotionPointRecord();
        res.user_id = key.getLong();
        res.x = value.getInt();
        res.y = value.getInt();
        res.z = value.getInt();

        out.collect(res);
    }

    @Override
    public TypeInformation<MotionPointRecord> getProducedType() {
        return null;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(MotionPointRecord motion_point_record, KafkaSinkContext kafkaSinkContext, Long aLong) {
        var key_bytes = ByteBuffer.allocate(8).putLong(motion_point_record.user_id).array();

        var val_bytes = ByteBuffer.allocate(12)
                .putInt(motion_point_record.x)
                .putInt(motion_point_record.y)
                .putInt(motion_point_record.z)
                .array();

        /*var val_bytes = new byte[1];

        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            String json = ow.writeValueAsString(motion_point_record);
            val_bytes = json.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }*/

        return new ProducerRecord<>("motion_point", key_bytes, val_bytes);
    }
}