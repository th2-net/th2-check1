package com.exactpro.th2.check1.utils

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter

fun ProtoToIMessageConverter.fromProtoPreFilter(protoPreMessageFilter: RootMessageFilter): IMessage =
    fromProtoFilter(protoPreMessageFilter.messageFilter, SequenceCheckRuleTask.PRE_FILTER_MESSAGE_NAME)

fun PreFilter.toRootMessageFilter(): RootMessageFilter = RootMessageFilter.newBuilder()
    .setMessageType(SequenceCheckRuleTask.PRE_FILTER_MESSAGE_NAME)
    .setMessageFilter(toMessageFilter())
    .also {
        if (hasMetadataFilter()) {
            it.metadataFilter = metadataFilter
        }
    }
    .build()

fun PreFilter.toMessageFilter(): MessageFilter = MessageFilter.newBuilder()
    .putAllFields(fieldsMap)
    .build()