package com.example.apachebeamcsvtoavro.util;

import com.example.apachebeamcsvtoavro.model.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

public class AvroReader {
    public static void main(String[] args) throws IOException {
        // human readable
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File("output.avro"), datumReader);
        Schema avroSchema = dataFileReader.getSchema();
        System.out.println(avroSchema); // this will print the schema for you
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println(user);

        }
    }
}
