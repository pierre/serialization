/*
 * Copyright 2010-2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.serialization.smile;

import java.io.ByteArrayOutputStream;

/**
 * Wrapper for Smile events payload. This is useful for Hadoop serialization.
 */
public class SmileOutputStream extends ByteArrayOutputStream
{
    private final String typeName;

    public SmileOutputStream(final String typeName, final int size)
    {
        super(size);
        this.typeName = typeName;
    }

    public String getTypeName()
    {
        return typeName;
    }
}
