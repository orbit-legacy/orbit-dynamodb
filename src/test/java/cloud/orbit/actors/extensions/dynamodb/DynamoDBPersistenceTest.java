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

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.fasterxml.jackson.core.JsonProcessingException;

import cloud.orbit.actors.Actor;
import cloud.orbit.actors.Stage;
import cloud.orbit.actors.extensions.ActorExtension;
import cloud.orbit.actors.test.StorageBaseTest;
import cloud.orbit.actors.test.StorageTest;
import cloud.orbit.actors.test.StorageTestState;
import cloud.orbit.exception.UncheckedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

public class DynamoDBPersistenceTest extends StorageBaseTest
{
    private static final String DEFAULT_TABLE_NAME = "orbit-test";

    private DynamoDBConnection dynamoDBConnection;
    private DynamoDBConfiguration dynamoDBConfiguration;
    private DynamoDBStorageExtension dynamoExtension;


    public DynamoDBPersistenceTest()
    {
        dynamoDBConfiguration = new DynamoDBConfiguration.Builder()
                .withCredentialType(AmazonCredentialType.BASIC_CREDENTIALS)
                .withAccessKey("dummy")
                .withSecretKey("dummy")
                .withEndpoint("http://localhost:35458/")
                .build();

        getStorageExtension();
    }

    @Override
    public Class<? extends StorageTest> getActorInterfaceClass()
    {
        return Hello.class;
    }

    @Override
    public ActorExtension getStorageExtension()
    {
        if(dynamoExtension == null)
        {
            dynamoExtension = new DynamoDBStorageExtension(dynamoDBConfiguration);
            dynamoExtension.setDefaultTableName(DEFAULT_TABLE_NAME);
        }

        return dynamoExtension;
    }

    @Override
    public void initStorage()
    {
        dynamoDBConnection = new DynamoDBConnection(dynamoDBConfiguration);

        closeStorage();
    }

    @Override
    public void closeStorage()
    {
        try
        {
            dynamoDBConnection.getDynamoClient().describeTable(DEFAULT_TABLE_NAME);
            dynamoDBConnection.getDynamoClient().deleteTable(DEFAULT_TABLE_NAME);
        }
        catch(ResourceNotFoundException e)
        {

        }

        dynamoDBConnection.getDynamoDB().createTable(DEFAULT_TABLE_NAME,
                Collections.singletonList(
                        new KeySchemaElement("_id", KeyType.HASH)),
                Collections.singletonList(
                       new AttributeDefinition("_id", ScalarAttributeType.S)),
                new ProvisionedThroughput(1L, 1L));
    }

    public long count(Class<? extends StorageTest> actorInterface)
    {
        final String tableName = dynamoExtension.getTableName(actorInterface, HelloState.class);
        try
        {
            final Table table = dynamoDBConnection.getDynamoDB().getTable(tableName);

            // if we get here, the table exists, it may not be ready yet.
            ensureTableIsActive(tableName);
            ScanSpec scanSpec = new ScanSpec()
                    .withAttributesToGet(DynamoDBUtils.FIELD_NAME_PRIMARY_ID)
                    .withConsistentRead(true);

            final ItemCollection<ScanOutcome> queryResults = table.scan(scanSpec);

            int count = 0;
            Iterator<Item> iterator = queryResults.iterator();

            while (iterator.hasNext())
            {
                iterator.next();
                count++;
            }

            return count;
        }
        catch (ResourceNotFoundException e)
        {
            // Assumption: nonexistent table is deliberate (count could be called before a get/put which
            // creates the table), meaning it has no entries
            return 0;
        }
    }

    @Override
    public StorageTestState readState(final String identity)
    {
        final Table table = dynamoDBConnection.getDynamoDB().getTable(DEFAULT_TABLE_NAME);
        final Item item = table.getItem("_id", identity + "/" + getActorInterfaceClass().getName());

        if (item != null)
        {
            try
            {
                final StorageTestState testState = new HelloState();
                dynamoDBConnection.getMapper().readerForUpdating(testState).readValue(item.getJSON("_state"));
                return testState;
            }
            catch (Exception e)
            {
                throw new UncheckedException(e);
            }
        }
        return null;
    }

    public long count()
    {
        return count(getActorInterfaceClass());
    }

    @Override
    public int heavyTestSize()
    {
        return 100;
    }

    @Test
    public void testPersistingNullValues() throws Exception {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName(null);
        sampleData.setNameList(null);
        sampleData.setNameProperties(null);
        sampleData.setNameSet(null);
        sampleData.setByteArray(null);

        testSampleData(sampleData);
    }

    @Test
    public void testPersistingNullContainedValues() throws Exception {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName(null);

        List<String> nameList = new ArrayList<>();
        nameList.add(null);

        sampleData.setNameList(nameList);

        final Map<String, Object> nameProperties = new HashMap<>();
        nameProperties.put("Jim", null);

        sampleData.setNameProperties(nameProperties);
        sampleData.setNameSet(new HashSet<>(Collections.singletonList(null)));

        testSampleData(sampleData);
    }

    @Test
    public void testPersistingEmptyContainers() throws Exception {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName(null);
        sampleData.setNameList(new ArrayList<>());
        sampleData.setNameProperties(new HashMap<>());
        sampleData.setNameSet(new HashSet<>());
        sampleData.setByteArray(new byte[0]);

        testSampleData(sampleData);
    }

    @Test
    public void testPersistingEmptyStringValues() throws Exception {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName("");

        List<String> nameList = new ArrayList<>();
        nameList.add("");

        sampleData.setNameList(nameList);

        final Map<String, Object> nameProperties = new HashMap<>();
        nameProperties.put("Jim", "");

        sampleData.setNameProperties(nameProperties);
        sampleData.setNameSet(new HashSet<>(Collections.singletonList("")));

        testSampleData(sampleData);
    }

    @Test
    public void testPersistingNonBlankNonEmptyValues() throws Exception {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName("Larry");

        final Map<String, Object> nameProperties = new HashMap<>();
        nameProperties.put("Curly", "one");
        nameProperties.put("Larry", 2);
        nameProperties.put("Moe", "three".getBytes());

        sampleData.setNameProperties(nameProperties);

        sampleData.setNameList(new ArrayList<>(nameProperties.keySet()));
        sampleData.setNameSet(new HashSet<>(nameProperties.keySet()));

        sampleData.setByteArray(sampleData.getName().getBytes());

        testSampleData(sampleData);
    }

    private void testSampleData(HelloDto sampleData) throws JsonProcessingException
    {
        Stage stage = this.createStage();
        final Hello helloActor = Actor.getReference(Hello.class, "sampleData");

        helloActor.setSampleData(sampleData).join();
        final HelloDto loadedSampleData = helloActor.getSampleData(true).join();

        jsonEquals(sampleData, loadedSampleData);
    }

    protected void jsonEquals(Object expect, Object actual)
    {
        try
        {
            assertEquals(
                    dynamoDBConnection.getMapper().writeValueAsString(expect),
                    dynamoDBConnection.getMapper().writeValueAsString(actual));
        }
        catch (JsonProcessingException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void ensureTableIsActive(final String tableName)
    {
        try
        {
            while (true)
            {
                final DescribeTableResult describe = dynamoDBConnection.getDynamoClient().describeTable(tableName);
                if (describe.getTable().getTableStatus().equals(TableStatus.ACTIVE.name()))
                {
                    return;
                }

                Thread.sleep(500);
            }

        }
        catch (InterruptedException e)
        {
            throw new UncheckedException(e);
        }
    }
}
