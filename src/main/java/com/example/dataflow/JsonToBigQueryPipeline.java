package com.example.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import model.ValidRecord;
import transforms.DeduplicateTransform;
import transforms.NullCheckTransform;
import transforms.ParseJsonTransform;
import transforms.ToTableRowTransform;

public class JsonToBigQueryPipeline {

    public static void main(String[] args) {
        com.example.dataflow.options.JsonToBigQueryOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(JsonToBigQueryOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        TableSchema schema = SchemaLoader.loadSchema(options.getSchemaPath(), options);

        PCollection<String> rawJson =
                pipeline.apply("ReadJsonFromGCS", TextIO.read().from(options.getInputPath()));

        PCollectionTuple parsed = rawJson.apply("ParseInputJson", new ParseJsonTransform());

        PCollection<ValidRecord> validRecords = parsed.get(ParseJsonTransform.VALID_TAG);

        PCollection<ValidRecord> checked =
                validRecords.apply("NullCheck", new NullCheckTransform(options.getRequiredFields()));

        PCollection<ValidRecord> deduped =
                checked.apply("Deduplicate", new DeduplicateTransform(options.getDedupFields()));

        PCollection<TableRow> rows =
                deduped.apply("ConvertToTableRow", new ToTableRowTransform());

        rows.apply("WriteToBigQuery",
                BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }
}