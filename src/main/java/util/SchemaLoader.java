package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.options.PipelineOptions;

public class SchemaLoader {

    public static TableSchema loadSchema(String path, PipelineOptions options) {
        try {
            String json;
            if (path.startsWith("gs://")) {
                GcsOptions gcsOptions = options.as(GcsOptions.class);
                GcsUtil gcsUtil = gcsOptions.getGcsUtil();
                InputStream is = Channels.newInputStream(gcsUtil.open(path));
                json = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));
            } else {
                InputStream is = SchemaLoader.class.getResourceAsStream(path);
                if (is == null) {
                    is = new java.io.FileInputStream(path);
                }
                json = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));
            }
            return new ObjectMapper().readValue(json, TableSchema.class);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load schema from path: " + path, e);
        }
    }
}