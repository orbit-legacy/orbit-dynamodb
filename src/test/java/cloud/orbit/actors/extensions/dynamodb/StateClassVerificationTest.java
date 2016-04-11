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

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;

import cloud.orbit.actors.Actor;
import cloud.orbit.actors.Stage;
import cloud.orbit.actors.extensions.ActorExtension;
import cloud.orbit.actors.extensions.StorageExtension;
import cloud.orbit.actors.test.ActorBaseTest;
import cloud.orbit.util.ExceptionUtils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StateClassVerificationTest extends ActorBaseTest
{
    private static final String DEFAULT_TABLE_NAME = "orbit-test";

    private DynamoDBConnection dynamoDBConnection;
    private DynamoDBConfiguration dynamoDBConfiguration;
    private StateWrappingDynamoDBStorageExtension stateWrappingDynamoDBStorageExtension;

    public StateClassVerificationTest()
    {
        dynamoDBConfiguration = new DynamoDBConfiguration.Builder()
                .withCredentialType(AmazonCredentialType.BASIC_CREDENTIALS)
                .withAccessKey("dummy")
                .withSecretKey("dummy")
                .withEndpoint("http://localhost:35458/")
                .build();

        getStorageExtension();
    }

    @Before
    public void setup()
    {
        dynamoDBConnection = new DynamoDBConnection(dynamoDBConfiguration);

        closeStorage();
    }

    @Override
    protected void installExtensions(final Stage stage)
    {
        stage.getExtensions().removeAll(stage.getAllExtensions(StorageExtension.class));
        stage.addExtension(this.getStorageExtension());
    }

    @Override
    public void after()
    {
        super.after();
        closeStorage();
    }

    public ActorExtension getStorageExtension()
    {
        if (stateWrappingDynamoDBStorageExtension == null)
        {
            stateWrappingDynamoDBStorageExtension = new StateWrappingDynamoDBStorageExtension(dynamoDBConfiguration);
            stateWrappingDynamoDBStorageExtension.setDefaultTableName(DEFAULT_TABLE_NAME);
        }

        return stateWrappingDynamoDBStorageExtension;
    }

    public void closeStorage()
    {
        try
        {
            dynamoDBConnection.getDynamoClient().describeTable(getTableName());
            dynamoDBConnection.getDynamoClient().deleteTable(getTableName());
        }
        catch (ResourceNotFoundException e)
        {

        }
    }

    @Test
    public void verifyReadStateClassCheck()
    {
        Stage stage = this.createStage();
        final Hello helloActor = Actor.getReference(Hello.class, "sampleData");

        final HelloDto data = new HelloDto();
        helloActor.setSampleData(data).join();

        stateWrappingDynamoDBStorageExtension.setUseMatchingStateClass(false);

        helloActor.getSampleData(true)
                .thenApply(result -> {
                    fail("Expected IllegalArgumentException was not raised");
                    return null;
                })
                .exceptionally(throwable -> {
                    assertTrue(ExceptionUtils.isCauseInChain(IllegalArgumentException.class, throwable));
                    return null;
                })
                .join();

        helloActor.clear().join();
    }

    @Test
    public void verifyWriteStateClassCheck()
    {
        Stage stage = this.createStage();
        final Hello helloActor = Actor.getReference(Hello.class, "sampleData");

        final HelloDto data = new HelloDto();
        stateWrappingDynamoDBStorageExtension.setUseMatchingStateClass(false);
        helloActor.setSampleData(data)
                .thenApply(result -> {
                    fail("Expected IllegalArgumentException was not raised");
                    return null;
                })
                .exceptionally(throwable -> {
                    assertTrue(ExceptionUtils.isCauseInChain(IllegalArgumentException.class, throwable));
                    return null;
                })
                .join();
    }

    protected String getTableName()
    {
        return DEFAULT_TABLE_NAME;
    }
}
