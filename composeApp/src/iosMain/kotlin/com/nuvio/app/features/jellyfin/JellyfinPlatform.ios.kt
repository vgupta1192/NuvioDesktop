package com.nuvio.app.features.jellyfin

// Jellyfin is a desktop-first fork feature; this target only needs the declarations to compile.
internal actual object JellyfinPlatform {
    actual suspend fun httpCall(
        method: String,
        url: String,
        headers: Map<String, String>,
        body: String?,
    ): JellyfinHttpResponse? = null

    private val values = mutableMapOf<String, String>()

    actual fun loadString(key: String): String? = values[key]

    actual fun saveString(key: String, value: String?) {
        if (value == null) values.remove(key) else values[key] = value
    }
}
