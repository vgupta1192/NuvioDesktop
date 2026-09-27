package com.nuvio.app.features.livetv

import com.nuvio.app.core.storage.DesktopStorage
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.BufferedInputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream

internal actual object LiveTvPlatform {
    private val client by lazy {
        OkHttpClient.Builder()
            .connectTimeout(20, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .followRedirects(true)
            .build()
    }
    private val clockFormat = DateTimeFormatter.ofPattern("HH:mm")

    actual suspend fun fetchGuideText(cacheKey: String, url: String, forceRefresh: Boolean, maxAgeMs: Long): String? =
        withContext(Dispatchers.IO) {
            val dir = DesktopStorage.cacheDir.resolve("livetv-epg").also { Files.createDirectories(it) }
            val safeKey = cacheKey.replace(Regex("[^A-Za-z0-9_-]"), "_")
            val cacheFile = dir.resolve("$safeKey.xml")
            val cached = cacheFile.takeIf { Files.isRegularFile(it) }
            if (!forceRefresh && cached != null) {
                val age = System.currentTimeMillis() - Files.getLastModifiedTime(cached).toMillis()
                if (age in 0 until maxAgeMs) {
                    return@withContext runCatching { Files.readString(cached) }.getOrNull()
                }
            }
            val downloaded = runCatching {
                val request = Request.Builder()
                    .url(url)
                    .header("User-Agent", "NuvioDesktop-LiveTV")
                    .build()
                client.newCall(request).execute().use { response ->
                    if (!response.isSuccessful) return@use null
                    val body = response.body ?: return@use null
                    val input = BufferedInputStream(body.byteStream(), 64 * 1024)
                    input.mark(2)
                    val b1 = input.read()
                    val b2 = input.read()
                    input.reset()
                    val stream: InputStream = if (b1 == 0x1F && b2 == 0x8B) GZIPInputStream(input, 64 * 1024) else input
                    stream.bufferedReader(Charsets.UTF_8).use { it.readText() }
                }
            }.getOrNull()?.takeIf { it.isNotBlank() }
            if (downloaded != null) {
                runCatching {
                    val temp = dir.resolve("$safeKey.tmp")
                    Files.writeString(temp, downloaded)
                    Files.move(temp, cacheFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
                }
                return@withContext downloaded
            }
            // Offline or source down: an old guide still beats none.
            cached?.let { runCatching { Files.readString(it) }.getOrNull() }
        }

    actual fun formatClock(epochMs: Long): String =
        runCatching { clockFormat.format(Instant.ofEpochMilli(epochMs).atZone(ZoneId.systemDefault())) }
            .getOrDefault("--:--")
}

internal actual object LiveTvStorage {
    private val store = DesktopStorage.store("nuvio_livetv")

    actual fun loadString(key: String): String? = store.getString(key)

    actual fun saveString(key: String, value: String?) {
        store.putString(key, value)
    }
}
