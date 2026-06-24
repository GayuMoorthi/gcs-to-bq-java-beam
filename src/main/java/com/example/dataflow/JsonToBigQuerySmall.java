package com.example.dataflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class JsonToBigQuerySmall {

    public interface MyOptions extends PipelineOptions {
        @Description("Input JSON file path, example gs://bucket/input/*.json")
        @Validation.Required
        String getInputPath();

        void setInputPath(String value);

        @Description("Output BigQuery table, example project:dataset.table")
        @Validation.Required
        String getOutputTable();

        void setOutputTable(String value);
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<String> jsonLines = p.apply("Read JSON File",
                TextIO.read().from(options.getInputPath()));

        PCollection<TableRow> rows = jsonLines.apply("Convert JSON to TableRow",
                ParDo.of(new JsonToTableRowFn()));

        rows.apply("Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withSchema(getSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run().waitUntilFinish();
    }

    static class JsonToTableRowFn extends DoFn<String, TableRow> {
        private transient ObjectMapper mapper;

        @Setup
        public void setup() {
            mapper = new ObjectMapper();
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String json = c.element();

            JsonNode node = mapper.readTree(json);

            TableRow row = new TableRow()
                    .set("id", node.get("id").asText())
                    .set("name", node.get("name").asText())
                    .set("age", node.get("age").asInt());

            c.output(row);
        }
    }

    static TableSchema getSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("id").setType("STRING"),
                new TableFieldSchema().setName("name").setType("STRING"),
                new TableFieldSchema().setName("age").setType("INTEGER")));
    }
}
