package com.nobodyhub.learn.avro.schema;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * @author Ryan
 */
public class UserTest {

    /**
     * with one nullable field being null
     */
    private User user1;
    /**
     * with all-argument constructor
     */
    private User user2;
    /**
     * with builder
     */
    private User user3;
    /**
     * with all fields being null
     * will throw exception
     */
    private User user4;
    /**
     * object itself being null
     * will throw exception
     */
    private User user5;

    @Before
    public void setup() {
        this.user1 = new User();
        this.user1.setName("Yan");
        this.user1.setFavoriteNumber(1019);

        this.user2 = new User("Ben", 7, "red");

        this.user3 = User.newBuilder()
                .setName("He")
                .setFavoriteColor("Blue")
                .setFavoriteNumber(1013)
                .build();

        this.user4 = new User();
        this.user5 = null;

    }

    @Test
    public void testSerialize() throws IOException {
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        try (DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(user1.getSchema(), new File("users.avro"));
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
        }
    }

    @Test(expected = DataFileWriter.AppendWriteException.class)
    public void testSerialize1() throws IOException {
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        File newFile = new File("users.avro");
        try (DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(user4.getSchema(), newFile);
            dataFileWriter.append(user4);
            dataFileWriter.append(user5);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testSerialize2() throws IOException {
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        try (DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(user5.getSchema(), new File("users.avro"));
            dataFileWriter.append(user5);
        }
    }

    @Test
    public void testDeserialize() throws IOException {
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        try (DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(user1.getSchema(), new File("users.avro"));
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
        }

        DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
        File file = new File("users.avro");
        try (DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader)) {
            User user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        }
    }
}