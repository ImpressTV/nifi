package org.apache.nifi.processors.avro;

import org.apache.avro.Schema;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

/**
 * Created by joe on 2015.09.18..
 */
public class AvroUtil {
    public  static Schema parseSchema(String literal) {
        try {
            return new Schema.Parser().parse(literal);
        } catch (RuntimeException e) {
            throw new RuntimeException(
                    "Failed to parse schema: " + literal, e);
        }
    }

    protected static final Validator SCHEMA_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String literal, ValidationContext context) {
            String error = null;
            try {
                parseSchema(literal);
            } catch (RuntimeException e) {
                error = e.getMessage();
            }

            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(literal)
                    .explanation(error)
                    .valid(error == null)
                    .build();
        }
    };
}
