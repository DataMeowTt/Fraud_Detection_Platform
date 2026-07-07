package com.frauddetection.common.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

public class SchemaValidator {

    private final JsonSchema schema;

    public SchemaValidator(String schemaFilePath) throws IOException {
        String content = Files.readString(Paths.get(schemaFilePath));
        this.schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(content);
    }

    public Set<ValidationMessage> validate(JsonNode node) {
        return schema.validate(node);
    }
}
