package com.nuvio.app.features.jellyfin

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/**
 * Minimal Jellyseerr/Overseerr REST client (API v1, X-Api-Key header). Requests created here are
 * forwarded by Seerr to Radarr/Sonarr; once the download finishes and Jellyfin scans it in, the
 * title simply shows up in its Jellyfin library.
 */
internal object SeerrClient {
    private val json = Json { ignoreUnknownKeys = true }

    internal enum class RequestOutcome { Created, AlreadyRequested, Unauthorized, Failed }

    fun normalizeUrl(raw: String): String? {
        var url = raw.trim()
        if (url.isBlank()) return null
        if (!url.startsWith("http://") && !url.startsWith("https://")) url = "https://$url"
        return url.trimEnd('/').takeIf { it.removePrefix("https://").removePrefix("http://").isNotBlank() }
    }

    private fun headers(apiKey: String): Map<String, String> = mapOf(
        "X-Api-Key" to apiKey,
        "Accept" to "application/json",
    )

    suspend fun testConnection(baseUrl: String, apiKey: String): Boolean {
        val response = JellyfinPlatform.httpCall(
            method = "GET",
            url = "$baseUrl/api/v1/auth/me",
            headers = headers(apiKey),
            body = null,
        )
        return response != null && response.status == 200
    }

    suspend fun search(baseUrl: String, apiKey: String, query: String, limit: Int = 12): List<SeerrSearchResult>? {
        if (query.isBlank()) return null
        val response = JellyfinPlatform.httpCall(
            method = "GET",
            url = "$baseUrl/api/v1/search?query=" + JellyfinClient.encodeQueryValue(query) + "&page=1",
            headers = headers(apiKey),
            body = null,
        ) ?: return null
        if (response.status !in 200..299) return null
        val root = runCatching { json.parseToJsonElement(response.body).jsonObject }.getOrNull() ?: return null
        val array = root["results"] as? JsonArray ?: return null
        return array.mapNotNull { element ->
            val obj = element as? JsonObject ?: return@mapNotNull null
            val tmdbId = obj.int("id") ?: return@mapNotNull null
            val mediaType = obj.string("mediaType") ?: return@mapNotNull null
            if (mediaType != "movie" && mediaType != "tv") return@mapNotNull null
            val title = obj.string("title") ?: obj.string("name") ?: obj.string("originalTitle")
                ?: return@mapNotNull null
            val mediaInfo = obj["mediaInfo"] as? JsonObject
            SeerrSearchResult(
                tmdbId = tmdbId,
                mediaType = mediaType,
                title = title,
                overview = obj.string("overview")?.takeIf { it.isNotBlank() },
                releaseYear = (obj.string("releaseDate") ?: obj.string("firstAirDate"))
                    ?.take(4)
                    ?.toIntOrNull(),
                posterUrl = obj.string("posterPath")
                    ?.takeIf { it.isNotBlank() }
                    ?.let { path -> "https://image.tmdb.org/t/p/w500$path" },
                isRequested = mediaInfo?.boolean("requested") == true,
                // Jellyseerr status enum: 1 unknown, 2 pending, 3 processing, 4 partially available, 5 available
                isAvailable = (mediaInfo?.int("status") ?: 0) >= 4,
            )
        }.take(limit)
    }

    suspend fun request(baseUrl: String, apiKey: String, mediaType: String, tmdbId: Int): RequestOutcome {
        val seasons = if (mediaType == "tv") tvSeasonNumbers(baseUrl, apiKey, tmdbId) else null
        val response = JellyfinPlatform.httpCall(
            method = "POST",
            url = "$baseUrl/api/v1/request",
            headers = headers(apiKey) + ("Content-Type" to "application/json"),
            body = buildJsonObject {
                put("mediaType", mediaType)
                put("mediaId", tmdbId)
                // Seerr requires the seasons array for TV; all seasons except specials.
                if (seasons != null) put("seasons", kotlinx.serialization.json.buildJsonArray { seasons.forEach { add(it) } })
            }.toString(),
        ) ?: return RequestOutcome.Failed
        return when (response.status) {
            in 200..299 -> RequestOutcome.Created
            409 -> RequestOutcome.AlreadyRequested
            401, 403 -> RequestOutcome.Unauthorized
            else -> RequestOutcome.Failed
        }
    }

    private suspend fun tvSeasonNumbers(baseUrl: String, apiKey: String, tmdbId: Int): List<Int>? {
        val response = JellyfinPlatform.httpCall(
            method = "GET",
            url = "$baseUrl/api/v1/tv/$tmdbId",
            headers = headers(apiKey),
            body = null,
        ) ?: return null
        if (response.status !in 200..299) return null
        val root = runCatching { json.parseToJsonElement(response.body).jsonObject }.getOrNull() ?: return null
        val seasons = root["seasons"] as? JsonArray ?: return null
        return seasons.mapNotNull { element ->
            (element as? JsonObject)?.int("seasonNumber")?.takeIf { it > 0 }
        }.takeIf { it.isNotEmpty() } ?: listOf(1)
    }

    private fun JsonObject.string(key: String): String? =
        this[key]?.jsonPrimitive?.contentOrNull

    private fun JsonObject.int(key: String): Int? =
        this[key]?.jsonPrimitive?.intOrNull

    private fun JsonObject.boolean(key: String): Boolean? =
        this[key]?.jsonPrimitive?.booleanOrNull
}
