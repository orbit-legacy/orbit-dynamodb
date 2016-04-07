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

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

import cloud.orbit.concurrent.Task;
import cloud.orbit.exception.UncheckedException;
import cloud.orbit.util.ExceptionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DynamoDBUtils
{
    final static private int WAITING_FOR_ACTIVE_TABLE_STATUS_MAX_ATTEMPTS = 66;
    final static long WAITING_FOR_ACTIVE_TABLE_STATUS_RETRY_DELAY_MILLIS = 600;
    final static public String FIELD_NAME_PRIMARY_ID = "_id";
    final static public String FIELD_NAME_DATA = "_state";
    final static public String FIELD_NAME_OWNING_ACTOR_TYPE = "_owningType";

    private static ConcurrentMap<String, Table> tableCache = new ConcurrentHashMap<>();

    public static Task<Table> getTable(final DynamoDBConnection dynamoDBConnection, final String tableName)
    {
        final String tableCacheId = generateTableCacheId(dynamoDBConnection, tableName);
        final Table table = tableCache.get(tableCacheId);

        if(table != null)
        {
            return Task.fromValue(table);
        }
        else
        {
            return Task.fromFuture(dynamoDBConnection.getDynamoClient().describeTableAsync(tableName))
                    .thenApply(DescribeTableResult::getTable)
                    .thenCompose(descriptor -> {
                        if (descriptor.getTableStatus().equals(TableStatus.CREATING.name()))
                        {
                            return waitForActiveTableStatus(dynamoDBConnection, tableName);
                        }
                        else
                        {
                            return Task.fromValue(descriptor);
                        }
                    })
                    .thenApply(descriptor -> {
                        final Table retrievedTable = dynamoDBConnection.getDynamoDB().getTable(descriptor.getTableName());
                        tableCache.putIfAbsent(tableCacheId, retrievedTable);
                        return retrievedTable;
                    })
                    .exceptionally(e ->
                    {
                        if (e instanceof ResourceInUseException)
                        {
                            return getTable(dynamoDBConnection, tableName).join();
                        }

                        if (ExceptionUtils.isCauseInChain(ResourceNotFoundException.class, e))
                        {
                            try
                            {
                                createTable(dynamoDBConnection, tableName);
                                return getTable(dynamoDBConnection, tableName).join();
                            }
                            catch (ResourceInUseException resourceInUseException)
                            {
                                return getTable(dynamoDBConnection, tableName).join();
                            }
                            catch (InterruptedException interruptedException)
                            {
                                throw new UncheckedException(interruptedException);
                            }
                        }
                        else
                        {
                            throw new UncheckedException(e);
                        }
                    });
        }
    }

    private static Task<TableDescription> waitForActiveTableStatus(final DynamoDBConnection dynamoDBConnection, final String tableName)
    {
        try
        {
            for (int i = 0; i < WAITING_FOR_ACTIVE_TABLE_STATUS_MAX_ATTEMPTS; i++)
            {
                final DescribeTableResult describe = dynamoDBConnection.getDynamoClient().describeTable(tableName);
                if (describe.getTable().getTableStatus().equals(TableStatus.ACTIVE.name()))
                {
                    return Task.fromValue(describe.getTable());
                }

                Thread.sleep(WAITING_FOR_ACTIVE_TABLE_STATUS_RETRY_DELAY_MILLIS);
            }

        }
        catch (InterruptedException e)
        {
            throw new UncheckedException(e);
        }

        throw new UncheckedException("Hit max retry attempts while waiting for table to become active: " + tableName);
    }

    private static Table createTable(final DynamoDBConnection dynamoDBConnection, final String tableName) throws InterruptedException
    {
        final CreateTableRequest createTableRequest = createCreateTableRequest(tableName);

        final Table table = dynamoDBConnection.getDynamoDB().createTable(createTableRequest);

        table.waitForActive();
        return table;
    }

    private static CreateTableRequest createCreateTableRequest(final String tableName)
    {
        final List<KeySchemaElement> keySchema = new ArrayList<>();
        final List<AttributeDefinition> tableAttributes = new ArrayList<>();

        keySchema.add(new KeySchemaElement(FIELD_NAME_PRIMARY_ID, KeyType.HASH));
        tableAttributes.add(new AttributeDefinition(FIELD_NAME_PRIMARY_ID, ScalarAttributeType.S));


        return new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(tableAttributes)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
    }

    private static String generateTableCacheId(final DynamoDBConnection dynamoDBConnection, final String tableName)
    {
        return dynamoDBConnection.getConnectionId().toString() + "/" + tableName;
    }
}
