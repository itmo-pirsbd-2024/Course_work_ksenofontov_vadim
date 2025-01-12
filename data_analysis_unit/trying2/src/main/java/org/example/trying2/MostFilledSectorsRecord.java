package org.example.trying2;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class MostFilledSectorsRecord implements KafkaRecordDeserializationSchema<MostFilledSectorsRecord>, KafkaRecordSerializationSchema<MostFilledSectorsRecord> {
    public ArrayList<Tuple3<Integer, Integer, Long>> sectors;
//    public Tuple2<Long, Long>[] sectors;

    MostFilledSectorsRecord() {
        sectors = new ArrayList<Tuple3<Integer, Integer, Long>>();
//        sectors = new Tuple2[(int)3];
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<MostFilledSectorsRecord> out) {

        ByteBuffer value = ByteBuffer.wrap(message.value()); // big-endian by default

        var res = new MostFilledSectorsRecord();
        var number_of_provided_sectors = value.getLong();
        for (long i = 0; i < number_of_provided_sectors; i++){
            sectors.addLast(new Tuple3<Integer, Integer, Long>(value.getInt(), value.getInt(), value.getLong()));
        }

        /*sectors = new Tuple2[(int)number_of_provided_sectors];
        for (long i = 0; i < number_of_provided_sectors; i++){
            sectors[(int)i] = new Tuple2<Long, Long>(value.getLong(), value.getLong());
        }*/

        out.collect(res);
    }

    @Override
    public TypeInformation<MostFilledSectorsRecord> getProducedType() {
        return null;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(MostFilledSectorsRecord obj, KafkaSinkContext kafkaSinkContext, Long aLong) {
        // not in use
        var buffer = ByteBuffer.allocate(8 + 2 * 8 * obj.sectors.size());

        buffer.putLong(obj.sectors.size());

        for (int i = 0; i < obj.sectors.size(); i++) {
            buffer.putInt(obj.sectors.get(i).f0).putInt(obj.sectors.get(i).f1).putLong(obj.sectors.get(i).f2);
        }

        /*var buffer = ByteBuffer.allocate(8 + 2 * 8 * obj.sectors.length);

        buffer.putLong(obj.sectors.length);

        for (int i = 0; i < obj.sectors.length; i++) {
            buffer.putLong(obj.sectors[i].f0).putLong(obj.sectors[i].f1);
        }*/

        var val_bytes = buffer.array();

        /*var val_bytes = new byte[1];

        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            String json = ow.writeValueAsString(obj);
            val_bytes = json.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }*/

        var key_bytes = new byte[1];
        key_bytes[0] = 1;

        return new ProducerRecord<>("most_filled_sectors", key_bytes, serializeToBytes());
    }


    public byte[] serializeToBytes() {
        var buffer = ByteBuffer.allocate(2 * 8 * sectors.size());

        for (int i = 0; i < sectors.size(); i++) {
            buffer.putInt(sectors.get(i).f0).putInt(sectors.get(i).f1).putLong(sectors.get(i).f2);
        }

        var val_bytes = buffer.array();

/*        var val_bytes = new byte[0];

        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            String json = ow.writeValueAsString(this);
            val_bytes = json.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }*/


        return val_bytes;
    }
}


