/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.verifier;

import com.exactpro.sf.services.ICSHIterator;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class VerifyIterator<E> implements ICSHIterator<E>, MessageListener<E> {

    private final BlockingQueue<E> cache;

    public VerifyIterator(Collection<E> messages) {
        this.cache = new LinkedBlockingQueue<>(messages);
    }

    @Override
    public void onMessage(E message) {
        cache.offer(message);
    }

    @Override
    public boolean hasNext(long timeout) throws InterruptedException {
        if(!hasNext()) {
            long endTime = System.currentTimeMillis() + timeout;
            while (cache.peek() == null && endTime > System.currentTimeMillis()) {
                Thread.sleep(1);
            }
        }
        return hasNext();
    }

    @Override
    public boolean hasNext() {
        return !cache.isEmpty();
    }

    @Override
    public E next() {
        return cache.poll();
    }

    @Override
    public void updateCheckPoint() {
        /* do nothing in current implementation */
    }
}
