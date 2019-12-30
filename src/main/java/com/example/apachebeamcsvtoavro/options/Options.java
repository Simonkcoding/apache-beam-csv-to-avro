package com.example.apachebeamcsvtoavro.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface Options extends PipelineOptions {

    @Description(
            "Set inuptFile required parameter to a file path or glob. A local path and Google Cloud "
                    + "Storage path are both supported.")
    @Default.String("input.csv")
    String getInputFile();

    void setInputFile(String value);

    @Description(
            "Set output required parameter to define output path. A local path and Google Cloud Storage"
                    + " path are both supported.")
    @Default.String("output")
    String getOutput();

    void setOutput(String value);

    @Description(
            "Set avroSchema required parameter to specify location of the schema. A local path and "
                    + "Google Cloud Storage path are supported.")
    @Default.String("schema.avsc")
    String getAvroSchema();

    void setAvroSchema(String value);

    @Description(
            "Set csvDelimiter optional parameter to specify the CSV delimiter. Default delimiter is set"
                    + " to a comma.")
    @Default.String(",")
    String getCsvDelimiter();

    void setCsvDelimiter(String delimiter);
}
