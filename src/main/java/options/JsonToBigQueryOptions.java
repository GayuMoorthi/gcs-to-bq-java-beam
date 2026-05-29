package options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Default;

public interface JsonToBigQueryOptions extends PipelineOptions {

    @Description("Input file pattern, e.g. gs://bucket/path/*.json")
    @Validation.Required
    String getInputPath();
    void setInputPath(String value);

    @Description("BigQuery table spec, e.g. project:dataset.table")
    @Validation.Required
    String getOutputTable();
    void setOutputTable(String value);

    @Description("Schema file path, local or GCS, containing BigQuery TableSchema JSON")
    @Validation.Required
    String getSchemaPath();
    void setSchemaPath(String value);

    @Description("Comma-separated required fields")
    @Default.String("id")
    String getRequiredFields();
    void setRequiredFields(String value);

    @Description("Comma-separated dedup key fields")
    @Default.String("id")
    String getDedupFields();
    void setDedupFields(String value);

    @Description("Dead letter table spec, optional")
    String getDeadLetterTable();
    void setDeadLetterTable(String value);
}