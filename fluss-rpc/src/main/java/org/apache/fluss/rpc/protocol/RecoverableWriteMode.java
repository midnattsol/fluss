/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.annotation.Internal;

/** Constants for the optional recoverable mode of PRODUCE_LOG version 2. */
@Internal
public final class RecoverableWriteMode {

    /** No recoverable-write mode was requested; use ordinary append semantics. */
    public static final int NONE = 0;

    /** The first attempt to append a prepared batch. */
    public static final int NEW_SEND = 1;

    /** A retry of the exact prepared batch after an ambiguous outcome. */
    public static final int RETRY = 2;

    private RecoverableWriteMode() {}

    /** Returns whether the supplied mode is a supported recoverable-write mode. */
    public static boolean isValid(int mode) {
        return mode == NEW_SEND || mode == RETRY;
    }
}
