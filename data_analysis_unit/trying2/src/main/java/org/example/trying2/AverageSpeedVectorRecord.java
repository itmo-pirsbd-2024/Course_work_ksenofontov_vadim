package org.example.trying2;

import com.fasterxml.jackson.core.JsonProcessingException;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class AverageSpeedVectorRecord implements
        KafkaRecordDeserializationSchema<AverageSpeedVectorRecord>,
        KafkaRecordSerializationSchema<AverageSpeedVectorRecord> {
    public Long user_id;

    public Double x_delta_normalised;
    public Double y_delta_normalised;
    public Double z_delta_normalised;

    public Double average_scalar_speed_in_m_per_s;

    long getUserId() {
        return user_id;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<AverageSpeedVectorRecord> out) {

        ByteBuffer key = ByteBuffer.wrap(message.key()); // big-endian by default
        ByteBuffer value = ByteBuffer.wrap(message.value()); // big-endian by default

        var res = new AverageSpeedVectorRecord();
        res.user_id = key.getLong();

        res.x_delta_normalised = value.getDouble();
        res.y_delta_normalised = value.getDouble();
        res.z_delta_normalised = value.getDouble();

        res.average_scalar_speed_in_m_per_s = value.getDouble();

        out.collect(res);
    }

    @Override
    public TypeInformation<AverageSpeedVectorRecord> getProducedType() {
        return null;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(AverageSpeedVectorRecord speedRecord, KafkaSinkContext kafkaSinkContext, Long aLong) {
        var key_bytes = ByteBuffer.allocate(8).putLong(speedRecord.user_id).array();

        /*var val_bytes = ByteBuffer.allocate(32)
                .putDouble(speedRecord.x_delta_normalised)
                .putDouble(speedRecord.y_delta_normalised)
                .putDouble(speedRecord.z_delta_normalised)
                .putDouble(speedRecord.average_scalar_speed_in_m_per_s)
                .array();*/

        var val_bytes = new byte[1];

        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            String json = ow.writeValueAsString(speedRecord);
            val_bytes = json.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }


        return new ProducerRecord<>("average_speed_vector", key_bytes, val_bytes);
    }
}

