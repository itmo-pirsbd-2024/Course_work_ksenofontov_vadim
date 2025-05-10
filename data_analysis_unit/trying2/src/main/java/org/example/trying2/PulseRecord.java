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

public class PulseRecord implements KafkaRecordDeserializationSchema<PulseRecord>, KafkaRecordSerializationSchema<PulseRecord> {
    public Long user_id;
    public Integer pulse_value;

    long getUserId() {
        return user_id;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<PulseRecord> out) {

        ByteBuffer key = ByteBuffer.wrap(message.key()); // big-endian by default
        ByteBuffer value = ByteBuffer.wrap(message.value()); // big-endian by default

        var res = new PulseRecord();
        res.user_id = key.getLong();
        res.pulse_value = value.getInt();

        out.collect(res);
    }

    @Override
    public TypeInformation<PulseRecord> getProducedType() {
        return null;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(PulseRecord pulseRecord, KafkaSinkContext kafkaSinkContext, Long aLong) {
        var key_bytes = ByteBuffer.allocate(8).putLong(pulseRecord.user_id).array();
        var val_bytes = ByteBuffer.allocate(4).putInt(pulseRecord.pulse_value).array();

        return new ProducerRecord<>("", key_bytes, val_bytes);
    }
}
