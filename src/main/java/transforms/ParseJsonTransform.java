package transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.ValidRecord;

public class ParseJsonTransform extends PTransform<PCollection<String>, PCollectionTuple> {

    public static final TupleTag<ValidRecord> VALID_TAG = new TupleTag<ValidRecord>() {};
    public static final TupleTag<String> ERROR_TAG = new TupleTag<String>() {};

    @Override
    public PCollectionTuple expand(PCollection<String> input) {
        return input.apply("ParseJson",
                ParDo.of(new ParseJsonFn())
                        .withOutputTags(VALID_TAG, TupleTagList.of(ERROR_TAG)));
    }

    static class ParseJsonFn extends DoFn<String, ValidRecord> {
        private transient ObjectMapper mapper;

        @Setup
        public void setup() {
            mapper = new ObjectMapper();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element();
            try {
                JsonNode node = mapper.readTree(json);
                c.output(new ValidRecord(json, node));
            } catch (Exception e) {
                c.output(ERROR_TAG, json);
            }
        }
    }
}