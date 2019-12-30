package com.example.apachebeamcsvtoavro.converter;

import com.example.apachebeamcsvtoavro.options.Options;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.List;

@Component
public class CsvToAvroConverter implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(CsvToAvroConverter.class);
    private static final List<String> acceptedTypes = Arrays.asList(
            new String[]{"string", "boolean", "int", "long", "float", "double"});


    @Override
    public void run(String... args) throws Exception {
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(Options.class);

        runCsvToAvro(options);
    }

    public static void runCsvToAvro(Options options)
            throws IOException, IllegalArgumentException {
        FileSystems.setDefaultPipelineOptions(options);

        // Get Avro Schema
        String schemaJson = getSchema(options.getAvroSchema());
        Schema schema = new Schema.Parser().parse(schemaJson);

        // Check schema field types before starting the Dataflow job
        checkFieldTypes(schema);

        // Create the Pipeline object with the options we defined above.
        Pipeline pipeline = Pipeline.create(options);

        // Convert CSV to Avro
       PCollection<GenericRecord> avro = pipeline.apply("Read CSV files", TextIO.read().from(options.getInputFile()))
                .apply("Convert CSV to Avro formatted data",
                        ParDo.of(new ConvertCsvToAvro(schemaJson, options.getCsvDelimiter())))
                .setCoder(AvroCoder.of(GenericRecord.class, schema));

       // write into avro file
        avro.apply("Write Avro formatted data", AvroIO.writeGenericRecords(schemaJson).withoutSharding()
                        .to(options.getOutput()).withCodec(CodecFactory.snappyCodec()).withSuffix(".avro"));

        // Run the pipeline.
        pipeline.run().waitUntilFinish();
    }

    public static void checkFieldTypes(Schema schema) throws IllegalArgumentException {
        for (Schema.Field field : schema.getFields()) {
            String fieldType = field.schema().getType().getName().toLowerCase();
            if (!acceptedTypes.contains(fieldType)) {
                LOG.error("Data transformation doesn't support: " + fieldType);
                throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
            }
        }
    }

    public static String getSchema(String schemaPath) throws IOException {
        ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(
                schemaPath, false));

        try (InputStream stream = Channels.newInputStream(chan)) {
            BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            StringBuilder dataBuilder = new StringBuilder();

            String line;
            while ((line = streamReader.readLine()) != null) {
                dataBuilder.append(line);
            }

            return dataBuilder.toString();
        }
    }

    public static class ConvertCsvToAvro extends DoFn<String, GenericRecord> {

        private String delimiter;
        private String schemaJson;

        public ConvertCsvToAvro(String schemaJson, String delimiter) {
            this.schemaJson = schemaJson;
            this.delimiter = delimiter;
        }

        @ProcessElement
        public void processElement(ProcessContext ctx) throws IllegalArgumentException {
            // Split CSV row into using delimiter
            String[] rowValues = ctx.element().split(delimiter);

            Schema schema = new Schema.Parser().parse(schemaJson);

            // Create Avro Generic Record
            GenericRecord genericRecord = new GenericData.Record(schema);
            List<Schema.Field> fields = schema.getFields();

            for (int index = 0; index < fields.size(); ++index) {
                Schema.Field field = fields.get(index);
                String fieldType = field.schema().getType().getName().toLowerCase();

                switch (fieldType) {
                    case "string":
                        genericRecord.put(field.name(), rowValues[index]);
                        break;
                    case "boolean":
                        genericRecord.put(field.name(), Boolean.valueOf(rowValues[index]));
                        break;
                    case "int":
                        genericRecord.put(field.name(), Integer.valueOf(rowValues[index]));
                        break;
                    case "long":
                        genericRecord.put(field.name(), Long.valueOf(rowValues[index]));
                        break;
                    case "float":
                        genericRecord.put(field.name(), Float.valueOf(rowValues[index]));
                        break;
                    case "double":
                        genericRecord.put(field.name(), Double.valueOf(rowValues[index]));
                        break;
                    default:
                        LOG.error("Data transformation doesn't support: " + fieldType);
                        throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
                }
            }
            ctx.output(genericRecord);
        }
    }
}
