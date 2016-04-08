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

import cloud.orbit.actors.extensions.StorageExtension;
import cloud.orbit.actors.runtime.RemoteReference;
import cloud.orbit.concurrent.Task;
import cloud.orbit.exception.UncheckedException;
import cloud.orbit.util.StringUtils;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class DynamoDBStorageExtension implements StorageExtension
{
    final static public String DOCUMENT_ID_DECORATION_SEPARATOR = "/";

    private String name = "default";

    private DynamoDBConnection dynamoDBConnection;

    private DynamoDBConfiguration dynamoDBConfiguration = new DynamoDBConfiguration();
    private String defaultTableName = "orbit";


    public DynamoDBStorageExtension()
    {
        dynamoDBConfiguration = new DynamoDBConfiguration();
    }

    public DynamoDBStorageExtension(final DynamoDBConfiguration dynamoDBConfiguration)
    {
        this.dynamoDBConfiguration = dynamoDBConfiguration;
    }

    @Override
     public Task<Void> start()
    {
        dynamoDBConnection = new DynamoDBConnection(dynamoDBConfiguration);

        DynamoDBUtils.getTable(dynamoDBConnection, defaultTableName).join();

        return Task.done();
    }

    @Override
    public Task<Void> stop()
    {
        return Task.done();
    }

    @Override
    public Task<Void> clearState(final RemoteReference<?> reference, final Object state)
    {
        final String tableName = getTableName(RemoteReference.getInterfaceClass(reference), state.getClass());
        final String itemId = generateDocumentId(reference, state);

        return DynamoDBUtils.getTable(dynamoDBConnection, tableName)
                .thenAccept(table -> table.deleteItem(DynamoDBUtils.FIELD_NAME_PRIMARY_ID, itemId));

    }

    @Override
    public Task<Boolean> readState(final RemoteReference<?> reference, final Object state)
    {
        final ObjectMapper mapper = dynamoDBConnection.getMapper();
        final String tableName = getTableName(RemoteReference.getInterfaceClass(reference), state.getClass());
        final String itemId = generateDocumentId(reference, state);

        return DynamoDBUtils.getTable(dynamoDBConnection, tableName)
                .thenApply(table -> {
                    GetItemSpec getItemSpec = new GetItemSpec()
                            .withPrimaryKey(DynamoDBUtils.FIELD_NAME_PRIMARY_ID, itemId)
                            .withConsistentRead(true);

                    return table.getItem(getItemSpec);
                })
                .thenApply(item ->
                {
                    if (item != null)
                    {
                        readStateInternal(state, item, mapper);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                });
    }

    @Override
    public Task<Void> writeState(final RemoteReference<?> reference, final Object state)
    {
        final Class<?> referenceType = RemoteReference.getInterfaceClass(reference);
        final String tableName = getTableName(referenceType, state.getClass());
        final String itemId = generateDocumentId(reference, state);

        return DynamoDBUtils.getTable(dynamoDBConnection, tableName)
                .thenAccept(table ->
                {
                    final Item newItem = generatePutItem(reference, state, itemId, dynamoDBConnection.getMapper());

                    table.putItem(newItem);
                });
    }

    @Override
    public String getName()
    {
        return name;
    }

    public String generateDocumentId(final RemoteReference<?> reference, final Object state)
    {
        Class<?> referenceClass = RemoteReference.getInterfaceClass(reference);
        String idDecoration = getIdDecoration(state, referenceClass.getName());

        String documentId = String.format(
                "%s%s%s",
                String.valueOf(RemoteReference.getId(reference)),
                DOCUMENT_ID_DECORATION_SEPARATOR,
                idDecoration);

        return documentId;
    }

    public String getIdDecoration(final Object state, final String defaultIdDecoration)
    {
        if (state != null)
        {
            DynamoDBStateConfiguration dynamoDBStateConfiguration = state.getClass().getAnnotation(DynamoDBStateConfiguration.class);
            if (dynamoDBStateConfiguration != null && StringUtils.isNotBlank(dynamoDBStateConfiguration.idDecorationOverride()))
            {
                return dynamoDBStateConfiguration.idDecorationOverride();
            }
        }

        return defaultIdDecoration;
    }

    public String getTableName(final Class<?> referenceType, final Class<?> stateType)
    {
        DynamoDBStateConfiguration dynamoDBStateConfiguration = stateType.getAnnotation(DynamoDBStateConfiguration.class);
        if (dynamoDBStateConfiguration != null && StringUtils.isNotBlank(dynamoDBStateConfiguration.collection()))
        {
            return dynamoDBStateConfiguration.collection();
        }

        return defaultTableName;
    }

    public void setName(final String name)
    {
        this.name = name;
    }

    public String getDefaultTableName()
    {
        return defaultTableName;
    }

    public void setDefaultTableName(final String defaultTableName)
    {
        this.defaultTableName = defaultTableName;
    }

    protected DynamoDBConnection getDynamoDBConnection()
    {
        return dynamoDBConnection;
    }

    protected void readStateInternal(final Object state, final Item item, final ObjectMapper mapper)
    {
        try
        {
            mapper.readerForUpdating(state).readValue(item.getJSON(DynamoDBUtils.FIELD_NAME_DATA));
        }
        catch (IOException e)
        {
            throw new UncheckedException(e);
        }
    }

    protected Item generatePutItem(final RemoteReference<?> reference, final Object state, final String itemId, final ObjectMapper mapper)
    {
        try
        {
            final Class<?> referenceType = RemoteReference.getInterfaceClass(reference);
            final String serializedState = mapper.writeValueAsString(state);

            return new Item()
                    .withPrimaryKey(DynamoDBUtils.FIELD_NAME_PRIMARY_ID, itemId)
                    .with(DynamoDBUtils.FIELD_NAME_OWNING_ACTOR_TYPE, referenceType.getName())
                    .withJSON(DynamoDBUtils.FIELD_NAME_DATA, serializedState);
        }
        catch (JsonProcessingException e)
        {
            throw new UncheckedException(e);
        }
    }
}
