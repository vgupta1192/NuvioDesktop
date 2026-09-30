package com.nuvio.app.features.jellyfin

import com.nuvio.app.core.storage.DesktopStorage
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.util.concurrent.TimeUnit

internal actual object JellyfinPlatform {
    private val client by lazy {
        OkHttpClient.Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .followRedirects(true)
            .build()
    }

    actual suspend fun httpCall(
        method: String,
        url: String,
        headers: Map<String, String>,
        body: String?,
    ): JellyfinHttpResponse? = withContext(Dispatchers.IO) {
        runCatching {
            val builder = Request.Builder().url(url)
            headers.forEach { (name, value) -> builder.header(name, value) }
            when (method.uppercase()) {
                "POST" -> builder.post((body.orEmpty()).toRequestBody("application/json".toMediaType()))
                "GET" -> builder.get()
                else -> builder.method(method.uppercase(), null)
            }
            client.newCall(builder.build()).execute().use { response ->
                JellyfinHttpResponse(
                    status = response.code,
                    body = response.body?.string().orEmpty(),
                )
            }
        }.getOrNull()
    }

    private val store by lazy { DesktopStorage.store("nuvio_jellyfin") }

    actual fun loadString(key: String): String? = store.getString(key)

    actual fun saveString(key: String, value: String?) {
        store.putString(key, value)
    }
}
