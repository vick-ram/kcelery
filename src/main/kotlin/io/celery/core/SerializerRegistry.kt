package io.celery.core

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.get
import kotlin.reflect.KClass

class SerializerRegistry {
    private val serializers = ConcurrentHashMap<String, KSerializer<*>>()

    fun <T : Any> register(type: KClass<T>, serializer: KSerializer<out Any>) {
        serializers[type.qualifiedName!!] = serializer
    }

    fun serialize(value: Any?): JsonElement {
        if (value == null) return JsonNull
        val serializer = serializers[value::class.qualifiedName]
        return if (serializer != null) {
            @Suppress("UNCHECKED_CAST")
            Json.encodeToJsonElement(serializer as KSerializer<Any?>, value)
        } else {
            // Fallback to toString
            JsonPrimitive(value.toString())
        }
    }
}