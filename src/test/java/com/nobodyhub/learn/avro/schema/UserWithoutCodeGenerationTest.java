package com.nobodyhub.learn.avro.schema;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author Ryan
 */
public class UserWithoutCodeGenerationTest {
    private Schema schema;
    private GenericRecord user1;
    private GenericRecord user2;

    @Before
    public void setup() throws IOException {
        String filePath = this.getClass().getClassLoader().getResource("schema.avsc").getPath();
        schema = new org.apache.avro.Schema.Parser().parse(new File(filePath));
        user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
    }

    @Test
    public void testSerialize() throws IOException {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, new File("users.avro"));
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
        }
    }

    @Test
    public void testDeserialze() throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File("users.avro"), datumReader)) {
            GenericRecord user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        }
    }
}