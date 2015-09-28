package org.apache.nifi.processors.avro;

/**
 * Created by joe on 2015.09.11..
 */
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.LongHolder;
import org.apache.nifi.util.ObjectHolder;
import org.apache.nifi.util.StopWatch;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"avro", "lookup", "enrich", "constant", "extend", "append", "cache"})
@CapabilityDescription(
        "Enriches the Avro content according to enrich strategy")
public class EnrichAvro extends AbstractProcessor {

    public static final AllowableValue LOOKUP_DISTRIBUTED_CACHE = new AllowableValue(
            "lookup_cache",
            "Lookup from distributed cache",
            "Lookup strategy"
    );

    public static final AllowableValue CONSTANT_STRATEGY = new AllowableValue(
            "constant",
            "Set constant",
            "Constant strategy, results to set a constant value for the new field"
    );

    public static final PropertyDescriptor ENRICH_STRATEGY = new PropertyDescriptor.Builder()
            .name("Enrich strategy")
            .description("Determines the method that will be used to enrich the content.")
            .allowableValues(CONSTANT_STRATEGY, LOOKUP_DISTRIBUTED_CACHE)
            .defaultValue(CONSTANT_STRATEGY.getValue())
            .required(true)
            .build();

    @VisibleForTesting
    static final PropertyDescriptor NEW_FIELD_SCHEMA = new PropertyDescriptor.Builder()
            .name("New field's schema").description("Avro Schema of the new field")
            .addValidator(AvroUtil.SCHEMA_VALIDATOR).expressionLanguageSupported(true)
            .required(true).build();

    @VisibleForTesting
    static final PropertyDescriptor NEW_FIELD_NAME
            = new PropertyDescriptor.Builder()
            .name("New field's name")
            .description("Name of the new field, appended to the original fields.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    @VisibleForTesting
    static final PropertyDescriptor NEW_FIELD_DEFAULT_VALUE
            = new PropertyDescriptor.Builder()
            .name("New field's default value")
            .description("Default value.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    @VisibleForTesting
    static final PropertyDescriptor MAPPING_FILE_PATH
            = new PropertyDescriptor.Builder()
            .name("Mapping file name")
            .description("The mapping file has two columns and contains a mapping from a possible value of lookup field " +
                    "to a new value in each line.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    @VisibleForTesting
    static final PropertyDescriptor LOOKUP_FIELD
            = new PropertyDescriptor.Builder()
            .name("Lookup field")
            .description("In case of lookup enrich strategy the processor gets the value of this field and tries to find" +
                    " a mapping in the mapping file.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Distributed Cache Service")
            .description("The Controller Service that is used to lookup a value")
            .required(false)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES
            = ImmutableList.<PropertyDescriptor>builder()
            .add(ENRICH_STRATEGY)
            .add(NEW_FIELD_NAME)
            .add(NEW_FIELD_SCHEMA)
            .add(DISTRIBUTED_CACHE_SERVICE)
            .add(NEW_FIELD_DEFAULT_VALUE)
            .add(MAPPING_FILE_PATH)
            .add(LOOKUP_FIELD)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            //TODO: description

            .description("TBD")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
                    //TODO: description
            .description("TBD")
            .build();
    static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("notfound")
                    //TODO: description
            .description("TBD")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    protected final Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String enrichStrategy = context.getProperty(ENRICH_STRATEGY).getValue();
        /*if(LOOKUP_LOCAL_FILE.getValue().equals(enrichStrategy)) {
            final String mappingFile = context.getProperty(MAPPING_FILE_PATH).getValue();
            ValidationResult r = StandardValidators.NON_EMPTY_VALIDATOR.validate(MAPPING_FILE_PATH.getName(), mappingFile, context);
            results.add(r);

            if (r.isValid()) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(MAPPING_FILE_PATH.getName(), mappingFile, context));
            }

            results.add(StandardValidators.NON_EMPTY_VALIDATOR.validate(LOOKUP_FIELD.getName(), context.getProperty(LOOKUP_FIELD).getValue(), context));

        } else*/ if (CONSTANT_STRATEGY.getValue().equals(enrichStrategy)) {
            final String constantValue = context.getProperty(NEW_FIELD_DEFAULT_VALUE).getValue();
            results.add(StandardValidators.NON_EMPTY_VALIDATOR.validate(NEW_FIELD_DEFAULT_VALUE.getName(), constantValue, context));
        }
        return results;
    }


    private final AtomicReference<Generator> valueGenerator = new AtomicReference<>(null);

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        String enrichStrategy = context.getProperty(ENRICH_STRATEGY).getValue();
        Generator g = createGenerator(enrichStrategy, context);
        valueGenerator.set(g);
    }

    private Generator createGenerator(String enrichStrategy, ProcessContext context) throws IOException {

        Generator result;
        if (enrichStrategy.equals(CONSTANT_STRATEGY.getValue())) {
            final String constantValue = context.getProperty(NEW_FIELD_DEFAULT_VALUE).getValue();
            result = new Generator<String>() {

                @Override
                public void close() { }

                @Override
                public String generate(GenericRecord record) { return constantValue;  }

                @Override
                public void finish(ProcessSession session) {}
            };
        } /*else if (enrichStrategy.equals(LOOKUP_LOCAL_FILE.getValue())) {
            final String mapFilePath = context.getProperty(MAPPING_FILE_PATH).getValue();
            final File mapFile = new File(mapFilePath);
            final StopWatch stopWatch = new StopWatch(true);
            final Map<String, String> map = Maps.newConcurrentMap();
            map.putAll(readFileToMap(mapFile));

            final String lookupField = context.getProperty(LOOKUP_FIELD).getValue();

            result = new Generator<String>() {
                @Override
                public String generate(GenericRecord record) {
                    String lookupFieldValue = String.valueOf(record.get(lookupField));
                    String mappedValue = map.get(lookupFieldValue);
                    return mappedValue;
                }

                @Override
                public void close() { }

                @Override
                public void initialize() { ;  }

                @Override
                public void finish(ProcessSession session) {}
            };
            stopWatch.stop();
            getLogger().info("Completed loading of mapping file, length is {}.  Elapsed time was {} milliseconds.",
                    new Object[]{map.size(), stopWatch.getDuration(TimeUnit.MILLISECONDS)});
        } */else if (enrichStrategy.equals(LOOKUP_DISTRIBUTED_CACHE.getValue())) {
            final DistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
            final String lookupField = context.getProperty(LOOKUP_FIELD).getValue();
            String schemaProperty = context.getProperty(NEW_FIELD_SCHEMA).getValue();
            final Schema schema;
            schema = AvroUtil.parseSchema(schemaProperty);

            result = new LookupCacheGenerator<Object>(lookupField, cache, new JsonDeserializer(schema));

        } else {
            // impossible
            throw new ProcessException("No generator created for this strategy.");
        }

        return result;
    }

    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {

        final ProcessorLog logger = getLogger();

        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final String newField = context.getProperty(NEW_FIELD_NAME).getValue();
        List<Schema.Field> enrichedSchemaFields = Lists.newArrayList();

        final ObjectHolder<Schema> inputSchema = new ObjectHolder<Schema>(null);
        final ObjectHolder<String> errorMessage = new ObjectHolder<>(null);

        // detect input schema automatically from the input stream
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream rawIn) throws IOException {
                DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(rawIn, new GenericDatumReader<GenericRecord>());

                Schema schema = reader.getSchema();

                if (schema.getType() != Schema.Type.RECORD) {
                    errorMessage.set("This is not an avro record");
                    return;
                }

                inputSchema.set(schema);
            }
        });

        if (errorMessage.get() != null) {
            logger.error("Failed to enrich {}, due to {}. Transferring to failure.", new Object[]{flowFile, errorMessage.get()});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }


        // add the original fields
        for (Schema.Field originalField : inputSchema.get().getFields()) {
            Schema.Field f = new Schema.Field(originalField.name(), originalField.schema(), originalField.doc(), originalField.defaultValue());
            enrichedSchemaFields.add(f);
        }

        // create the schema for the new field
        String schemaProperty = context.getProperty(NEW_FIELD_SCHEMA)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        final Schema newFieldSchema;
        try {
            newFieldSchema = AvroUtil.parseSchema(schemaProperty);
        } catch (RuntimeException e) {
            getLogger().error("Unable to parse schema: " + schemaProperty);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // add new field to the schema
        enrichedSchemaFields.add(new Schema.Field(newField, newFieldSchema, null, null));

        // create the enriched schema
        final Schema enrichedSchema = Schema.createRecord(inputSchema.get().getName(), inputSchema.get().getDoc(), inputSchema.get().getNamespace(), false);
        enrichedSchema.setFields(enrichedSchemaFields);

        DatumWriter<GenericData.Record> datumWriter = (DatumWriter<GenericData.Record>) GenericData.get().createDatumWriter(inputSchema.get());

        try (final DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(datumWriter);
             final Generator fieldValueGenerator = valueGenerator.get()) {

            writer.setCodec(CodecFactory.snappyCodec());

            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn);
                         final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
                         final DataFileWriter<GenericData.Record> w = writer.create(enrichedSchema, rawOut)) {

                        // use the same record for efficiency
                        GenericRecord record = null;

                        while (reader.hasNext()) {

                            record = reader.next(record);

                            GenericData.Record result = new GenericData.Record(enrichedSchema);
                            for (Schema.Field originalField : inputSchema.get().getFields()) {
                                String inputFieldName = originalField.name();
                                Schema.Field f = inputSchema.get().getField(inputFieldName);
                                result.put(inputFieldName, record.get(f.pos()));
                            }
                            Object value = fieldValueGenerator.generate(record);

                            if (value != null) {
                                //TODO : update counter
                                result.put(newField, value);
                                w.append(result);
                            } else {
                                //TODO: transfer to failure
                            }

                        }
                    }
                }
            });

            session.transfer(flowFile, REL_SUCCESS);
            fieldValueGenerator.finish(session);

        } catch (IOException e) {
            logger.error("Unable to write avro content to {}", new Object[] {flowFile}, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    // create default value JsonNodes from objects
    private static JsonNode toJsonNode(String objectStr) {
        try {
            return new ObjectMapper().readTree(objectStr);
        } catch (IOException e) {
            throw new SchemaBuilderException(e);
        }
    }

    interface Generator<T> extends Closeable {
        T generate(GenericRecord record);
        void finish(ProcessSession session);
    }

    class LookupCacheGenerator<V> implements Generator<V> {

        private final String lookupField;
        private final DistributedMapCacheClient cache;
        private final LongHolder foundInCache = new LongHolder(0);
        private final LongHolder notFoundInCache = new LongHolder(0);
        private final Serializer<String> keySerializer = new StringSerializer();
        private Deserializer<V> valueDeserializer;

        public LookupCacheGenerator(String lookupField, DistributedMapCacheClient cache, Deserializer<V> valueDeserializer) {
            this.lookupField = lookupField;
            this.cache = cache;
            this.valueDeserializer = valueDeserializer;
            getLogger().info("Lookup cache generator initialized.");
        }

        @Override
        public V generate(GenericRecord record) {
            String lookupFieldValue = String.valueOf(record.get(lookupField));
            getLogger().info("Lookup value : " + lookupFieldValue);
            try {
                final V mappedValue = cache.get(lookupFieldValue, keySerializer, valueDeserializer);
                getLogger().info("Found in cache :" + mappedValue);
                if (mappedValue != null) {
                    foundInCache.incrementAndGet();
                    return mappedValue;
                }
            } catch (IOException e) {
                getLogger().error("Unable to get cache value for key {}", new Object[] {lookupFieldValue}, e);
            }

            notFoundInCache.incrementAndGet();
            return null;
        }

        @Override
        public void finish(ProcessSession session) {
            session.adjustCounter("Found in cache", foundInCache.get(), false);
            session.adjustCounter("Not found in cache", notFoundInCache.get(), false);
        }

        @Override
        public void close() {

        }
    }

    private static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class JsonDeserializer implements Deserializer<Object> {

        private final Schema schema;

        public JsonDeserializer(Schema schema) {
            this.schema = schema;
            this.reader = new GenericDatumReader<Object>(schema);;
        }

        private final DatumReader<Object> reader;

        @Override
        public Object deserialize(final byte[] input) throws DeserializationException, IOException {
            ByteArrayInputStream bis = new ByteArrayInputStream(input);
            DataInputStream din = new DataInputStream(bis);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            Object datum = null;
            datum = reader.read(null, decoder);
            return datum;
        }
    }

    Map<String, String> readFileToMap(File f) throws IOException {
        HashMap<String, String> map = Maps.newHashMap();

        String line;
        BufferedReader reader = new BufferedReader(new FileReader(f));
        while ((line = reader.readLine()) != null)
        {
            String[] parts = line.split("[ \\t\\f]", 2);
            String key = parts[0];
            String value = parts[1];
            map.put(key, value);
        }
        return map;
    }
}


