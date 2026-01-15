/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.check1;

import java.util.Objects;

import com.exactpro.th2.common.grpc.Direction;

public class SessionKey {
    private final String bookName;
    private final String sessionAlias;
    private final Direction direction;

    public SessionKey(String bookName, String sessionAlias, Direction direction) {
        this.bookName = bookName;
        this.sessionAlias = sessionAlias;
        this.direction = direction;
    }

    public String getBookName() {
        return bookName;
    }

    public String getSessionAlias() {
        return sessionAlias;
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionKey that = (SessionKey) o;
        return Objects.equals(bookName, that.bookName)
                && Objects.equals(sessionAlias, that.sessionAlias)
                && direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bookName, sessionAlias, direction);
    }

    @Override
    public String toString() {
        return "SessionKey{" +
                "bookName='" + bookName + '\'' +
                ", sessionAlias='" + sessionAlias + '\'' +
                ", direction=" + direction +
                '}';
    }
}
