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

import java.util.List;
import java.util.Map;
import java.util.Set;

public class HelloDto
{
    private String name;
    private Set<String> nameSet;
    private List<String> nameList;
    private Map<String, Object> nameProperties;
    private byte[] byteArray;

    public String getName()
    {
        return name;
    }

    public void setName(final String name)
    {
        this.name = name;
    }

    public Set<String> getNameSet()
    {
        return nameSet;
    }

    public void setNameSet(final Set<String> nameSet)
    {
        this.nameSet = nameSet;
    }

    public List<String> getNameList()
    {
        return nameList;
    }

    public void setNameList(final List<String> nameList)
    {
        this.nameList = nameList;
    }

    public Map<String, Object> getNameProperties()
    {
        return nameProperties;
    }

    public void setNameProperties(final Map<String, Object> nameProperties)
    {
        this.nameProperties = nameProperties;
    }

    public byte[] getByteArray()
    {
        return byteArray;
    }

    public void setByteArray(final byte[] byteArray)
    {
        this.byteArray = byteArray;
    }
}
