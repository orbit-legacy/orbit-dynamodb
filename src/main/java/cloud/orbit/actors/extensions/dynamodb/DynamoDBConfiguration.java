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

public class DynamoDBConfiguration
{
    public static class Builder
    {
        private DynamoDBConfiguration dynamoConfig;

        public Builder()
        {
            dynamoConfig = new DynamoDBConfiguration();
        }

        public Builder withCredentialType(final AmazonCredentialType credentialType)
        {
            dynamoConfig.setCredentialType(credentialType);
            return this;
        }

        public Builder withAccessKey(final String accessKey)
        {
            dynamoConfig.setAccessKey(accessKey);
            return this;
        }

        public Builder withSecretKey(final String secretKey)
        {
            dynamoConfig.setSecretKey(secretKey);
            return this;
        }

        public Builder withSessionToken(final String sessionToken)
        {
            dynamoConfig.setSessionToken(sessionToken);
            return this;
        }

        public Builder withRegion(final String region)
        {
            dynamoConfig.setRegion(region);
            return this;
        }

        public Builder withEndpoint(final String endpoint)
        {
            dynamoConfig.setEndpoint(endpoint);
            return this;
        }

        public Builder withMapperConfigurer(final DynamoDBMapperConfigurer mapperConfigurer)
        {
            dynamoConfig.setMapperConfigurer(mapperConfigurer);
            return this;
        }

        public DynamoDBConfiguration build()
        {
            return dynamoConfig;
        }
    }

    private AmazonCredentialType credentialType;
    private String accessKey;
    private String secretKey;
    private String sessionToken;
    private String region;
    private String endpoint;
    private DynamoDBMapperConfigurer mapperConfigurer;

    public AmazonCredentialType getCredentialType()
    {
        return credentialType;
    }

    public void setCredentialType(final AmazonCredentialType credentialType)
    {
        this.credentialType = credentialType;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    public void setAccessKey(final String accessKey)
    {
        this.accessKey = accessKey;
    }

    public String getSecretKey()
    {
        return secretKey;
    }

    public void setSecretKey(final String secretKey)
    {
        this.secretKey = secretKey;
    }

    public String getSessionToken()
    {
        return sessionToken;
    }

    public void setSessionToken(final String sessionToken)
    {
        this.sessionToken = sessionToken;
    }

    public String getRegion()
    {
        return region;
    }

    public void setRegion(final String region)
    {
        this.region = region;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public void setEndpoint(final String endpoint)
    {
        this.endpoint = endpoint;
    }

    public DynamoDBMapperConfigurer getMapperConfigurer() { return mapperConfigurer; }

    public void setMapperConfigurer(DynamoDBMapperConfigurer mapperConfigurer) { this.mapperConfigurer = mapperConfigurer; }
}
