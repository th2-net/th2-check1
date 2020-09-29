/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.verifier

import io.grpc.stub.StreamObserver
import java.util.concurrent.Executor
import java.util.function.Consumer
import java.util.function.Supplier

class ListenableStreamObserver<T>(private val onNext: Consumer<T>, private val onError: Consumer<Throwable>?, private val executor: Executor) : StreamObserver<T> {
    override fun onNext(p0: T) {
        executor.execute {
            onNext.accept(p0)
        }
    }

    override fun onError(p0: Throwable?) {
        onError?.also {
            p0?.also {
                executor.execute {
                    onError.accept(p0)
                }
            }
        }
    }

    override fun onCompleted() {}

}