package transforms;

import com.fasterxml.jackson.databind.JsonNode;

import model.ValidRecord;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class DeduplicateTransform extends PTransform<PCollection<ValidRecord>, PCollection<ValidRecord>> {

    private final List<String> dedupFields;

    public DeduplicateTransform(String dedupFieldsCsv) {
        this.dedupFields = Arrays.stream(dedupFieldsCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    @Override
    public PCollection<ValidRecord> expand(PCollection<ValidRecord> input) {
        return input
                .apply("BuildDedupKey", WithKeys.of((SerializableFunction<ValidRecord, String>) this::buildKey))
                .setCoder(org.apache.beam.sdk.coders.KvCoder.of(
                        org.apache.beam.sdk.coders.StringUtf8Coder.of(),
                        org.apache.beam.sdk.coders.SerializableCoder.of(ValidRecord.class)))
                .apply("GroupByDedupKey", GroupByKey.create())
                .apply("TakeFirstRecord", MapElements.into(TypeDescriptors.kvs(
                        TypeDescriptors.strings(),
                        TypeDescriptors.iterables(TypeDescriptors.objects())))
                        .via(kv -> kv))
                .apply("ExtractFirstRecord", MapElements.via(new FirstRecordFn()));
    }

    private String buildKey(ValidRecord record) {
        JsonNode node = record.getJsonNode();
        return dedupFields.stream()
                .map(f -> {
                    JsonNode v = node.get(f);
                    return v == null || v.isNull() ? "" : v.asText();
                })
                .collect(Collectors.joining("|"));
    }

    static class FirstRecordFn extends SimpleFunction<KV<String, Iterable<ValidRecord>>, ValidRecord> {
        @Override
        public ValidRecord apply(KV<String, Iterable<ValidRecord>> input) {
            return input.getValue().iterator().next();
        }
    }
}