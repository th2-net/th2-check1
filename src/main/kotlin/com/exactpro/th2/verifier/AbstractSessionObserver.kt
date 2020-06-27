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
package com.exactpro.th2.verifier

import io.reactivex.observers.DisposableObserver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class AbstractSessionObserver : DisposableObserver<ObservedSession>() {
    override fun onComplete() {
        LOGGER.info("Observing completed for ${this::class.java.simpleName}")
    }

    override fun onError(e: Throwable) {
        LOGGER.error("Error threw for ${this::class.java.simpleName}", e)
    }

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}