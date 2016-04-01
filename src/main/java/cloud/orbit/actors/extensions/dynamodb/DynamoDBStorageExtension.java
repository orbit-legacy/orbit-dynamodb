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

import com.amazonaws.services.dynamodbv2.document.Item;
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

    }

    public DynamoDBStorageExtension(DynamoDBConfiguration dynamoDBConfiguration)
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
        final String itemId = generateDocumentId(reference);

        return DynamoDBUtils.getTable(dynamoDBConnection, tableName)
                .thenAccept(table -> table.deleteItem(DynamoDBUtils.FIELD_NAME_PRIMARY_ID, itemId));

    }

    @Override
    public Task<Boolean> readState(final RemoteReference<?> reference, final Object state)
    {
        final ObjectMapper mapper = dynamoDBConnection.getMapper();
        final String tableName = getTableName(RemoteReference.getInterfaceClass(reference), state.getClass());
        final String itemId = generateDocumentId(reference);

        return DynamoDBUtils.getTable(dynamoDBConnection, tableName)
                .thenApply(table -> table.getItem(DynamoDBUtils.FIELD_NAME_PRIMARY_ID, itemId))
                .thenApply(item ->
                {
                    if(item != null)
                    {
                        try
                        {
                            mapper.readerForUpdating(state).readValue(item.getJSON(DynamoDBUtils.FIELD_NAME_DATA));
                            return true;
                        }
                        catch(IOException e)
                        {
                            throw new UncheckedException(e);
                        }
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
        try
        {
            final ObjectMapper mapper = dynamoDBConnection.getMapper();
            final String serializedState = mapper.writeValueAsString(state);

            final String tableName = getTableName(RemoteReference.getInterfaceClass(reference), state.getClass());
            final String itemId = generateDocumentId(reference);

            return DynamoDBUtils.getTable(dynamoDBConnection, tableName)
                    .thenAccept(table ->
                    {
                        final Item newItem = new Item()
                                .withPrimaryKey(DynamoDBUtils.FIELD_NAME_PRIMARY_ID, itemId)
                                .withJSON(DynamoDBUtils.FIELD_NAME_DATA, serializedState);

                        table.putItem(newItem);
                    });

        }
        catch(JsonProcessingException e)
        {
            throw new UncheckedException(e);
        }
    }

    @Override
    public String getName()
    {
        return name;
    }

    public String generateDocumentId(final RemoteReference<?> reference)
    {
        Class<?> referenceClass = RemoteReference.getInterfaceClass(reference);
        String idDecoration = referenceClass.getName();

        String documentId = String.format(
                "%s%s%s",
                String.valueOf(RemoteReference.getId(reference)),
                DOCUMENT_ID_DECORATION_SEPARATOR,
                idDecoration);

        return documentId;
    }

    public String getTableName(final Class<?> referenceType, final Class<?> stateType)
    {
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

    public void setDefaultTableName(String defaultTableName)
    {
        this.defaultTableName = defaultTableName;
    }
}
