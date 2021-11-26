/*
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.entities

import com.exactpro.th2.check1.grpc.ChainID
import com.exactpro.th2.check1.grpc.CheckRuleRequest
import com.exactpro.th2.check1.grpc.CheckSequenceRuleRequest
import com.exactpro.th2.check1.grpc.NoMessageCheckRequest
import com.exactpro.th2.common.grpc.Checkpoint

class RequestAdaptor(val chainId: ChainID?, val checkpoint: Checkpoint?) {

    companion object {
        fun from(request: CheckRuleRequest): RequestAdaptor {
            return request.run {
                RequestAdaptor(
                        if (hasChainId()) chainId else null,
                        if (hasCheckpoint()) checkpoint else null
                )
            }
        }

        fun from(request: CheckSequenceRuleRequest): RequestAdaptor {
            return request.run {
                RequestAdaptor(
                        if (hasChainId()) chainId else null,
                        if (hasCheckpoint()) checkpoint else null
                )
            }
        }

        fun from(request: NoMessageCheckRequest): RequestAdaptor {
            return request.run {
                RequestAdaptor(
                        if (hasChainId()) chainId else null,
                        if (hasCheckpoint()) checkpoint else null
                )
            }
        }
    }
}