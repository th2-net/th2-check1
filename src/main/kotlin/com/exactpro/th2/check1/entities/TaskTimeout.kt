package com.exactpro.th2.check1.entities

data class TaskTimeout(val messageTimeout: Long? = null, val timeout: Long) {
    constructor(timeout: Long) : this(null, timeout)
}