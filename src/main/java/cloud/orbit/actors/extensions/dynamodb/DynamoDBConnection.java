/*
 Copyright (C) 2016 Electronic Arts Inc.  All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:

 1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
 2.  Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.
 3.  Neither the name of Electronic Arts, Inc. ("EA") nor the names of
     its contributors may be used to endorse or promote products derived
     from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY ELECTRONIC ARTS AND ITS CONTRIBUTORS "AS IS" AND ANY
 EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL ELECTRONIC ARTS OR ITS CONTRIBUTORS BE LIABLE FOR ANY
 DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package cloud.orbit.actors.extensions.dynamodb;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import cloud.orbit.actors.extensions.json.ActorReferenceModule;
import cloud.orbit.actors.runtime.DefaultDescriptorFactory;
import cloud.orbit.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class DynamoDBConnection
{
    // Do not change these placeholder values.  They are stored to DynamoDB in place of empty string/byte[] values,
    // which DynamoDB does not allow.
    final static private String EMPTY_STRING_PLACEHOLDER = "OrbitEmptyString.2f748e4e-c8ef-4129-8dbc-206fe8e72e64";
    final static public byte[] EMPTY_BYTE_ARRAY_PLACEHOLDER = "OrbitEmptyByteArray.a643e4a1-96dc-45b3-9606-479bae6bb3f2".getBytes();

    // Dynamo
    private AmazonDynamoDBAsyncClient dynamoClient;
    private DynamoDB dynamoDB;

    private ObjectMapper mapper;

    private UUID connectionId;

    public DynamoDBConnection(final DynamoDBConfiguration dynamoDBConfiguration)
    {
        connectionId = UUID.randomUUID();
        initializeDynamoDB(dynamoDBConfiguration);
        initializeMapper();
    }

    public AmazonDynamoDBAsyncClient getDynamoClient()
    {
        return dynamoClient;
    }

    public DynamoDB getDynamoDB()
    {
        return dynamoDB;
    }

    public ObjectMapper getMapper()
    {
        return mapper;
    }

    private void initializeMapper()
    {
        final SimpleModule serializersModule = createSerializersForMapper();

        mapper = new ObjectMapper();

        mapper.registerModule(new ActorReferenceModule(DefaultDescriptorFactory.get()));
        mapper.registerModule(serializersModule);

        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * DynamoDB does not allow empty values for certain types.
     * We must save placeholder values to it in these cases.
     */
    private SimpleModule createSerializersForMapper()
    {
        SimpleModule module = new SimpleModule();

        module.addSerializer(String.class, new JsonSerializer<String>()
        {
            @Override
            public void serialize(final String stringValue, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException, JsonProcessingException
            {
                if (stringValue.equals(""))
                {
                    jsonGenerator.writeString(EMPTY_STRING_PLACEHOLDER);
                }
                else
                {
                    jsonGenerator.writeString(stringValue);
                }
            }
        });

        module.addDeserializer(String.class, new JsonDeserializer<String>()
        {
            @Override
            public String deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException
            {
                final String value = jsonParser.getValueAsString();
                return (value.equals(EMPTY_STRING_PLACEHOLDER) ? "" : value);
            }
        });

        module.addSerializer(byte[].class, new JsonSerializer<byte[]>()
        {
            @Override
            public void serialize(final byte[] byteArrayValue, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException, JsonProcessingException
            {
                if (byteArrayValue.length > 0)
                {
                    jsonGenerator.writeBinary(byteArrayValue);
                }
                else
                {
                    jsonGenerator.writeBinary(EMPTY_BYTE_ARRAY_PLACEHOLDER);
                }
            }
        });

        module.addDeserializer(byte[].class, new JsonDeserializer<byte[]>()
        {
            @Override
            public byte[] deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException
            {
                final byte[] value = jsonParser.getBinaryValue();

                if (Arrays.equals(value, EMPTY_BYTE_ARRAY_PLACEHOLDER))
                {
                    return new byte[0];
                }

                return value;
            }
        });

        return module;
    }

    private void initializeDynamoDB(final DynamoDBConfiguration dynamoDBConfiguration)
    {
        switch (dynamoDBConfiguration.getCredentialType())
        {
            case BASIC_CREDENTIALS:
                dynamoClient = new AmazonDynamoDBAsyncClient(new BasicAWSCredentials(dynamoDBConfiguration.getAccessKey(), dynamoDBConfiguration.getSecretKey()));
                break;

            case BASIC_SESSION_CREDENTIALS:
                dynamoClient = new AmazonDynamoDBAsyncClient(new BasicSessionCredentials(dynamoDBConfiguration.getAccessKey(), dynamoDBConfiguration.getSecretKey(), dynamoDBConfiguration.getSessionToken()));
                break;

            case DEFAULT_PROVIDER_CHAIN:
            default:
                dynamoClient = new AmazonDynamoDBAsyncClient(new DefaultAWSCredentialsProviderChain());
                break;
        }

        String awsRegion = StringUtils.defaultIfBlank(dynamoDBConfiguration.getRegion(), AWSConfigValue.getRegion());
        if (StringUtils.isNotBlank(awsRegion))
        {
            dynamoClient.setRegion(Region.getRegion(Regions.fromName(awsRegion)));
        }

        if (StringUtils.isNotBlank(dynamoDBConfiguration.getEndpoint()))
        {
            dynamoClient.setEndpoint(dynamoDBConfiguration.getEndpoint());
        }

        dynamoDB = new DynamoDB(dynamoClient);
    }

    public UUID getConnectionId()
    {
        return connectionId;
    }
}
