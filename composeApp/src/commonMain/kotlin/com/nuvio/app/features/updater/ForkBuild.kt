package com.nuvio.app.features.updater

/**
 * Self-host fork patch: updates come from the fork's CI releases. CI builds the app with version
 * name "<upstream version>-fork.<CI run number>" and names each package
 * NuvioDesktop-<version>-b<run number>.<dmg|deb>, so "newer" = higher run number (fork rebuilds
 * of one upstream version keep the same version name otherwise). The platform's asset selector
 * picks the file this install can use (dmg on macOS, deb/AppImage on Linux, apk on Android).
 */
internal object ForkBuild {
    private const val TAG_PREFIX = "fork-build-"
    private val assetPattern = Regex("-b(\\d+)\\.[A-Za-z]+$")

    val localBuild: Int
        get() = AppUpdaterPlatform.currentVersionName.substringAfter("-fork.", "").toIntOrNull() ?: 0

    fun tag(build: Int): String = "$TAG_PREFIX$build"

    fun isNewer(tag: String): Boolean {
        val remote = tag.removePrefix(TAG_PREFIX).toIntOrNull() ?: return false
        return remote > localBuild
    }

    fun newest(releases: List<GitHubReleaseDto>, selector: AppUpdateAssetSelector): AppUpdate? {
        var best: AppUpdate? = null
        var bestBuild = -1
        for (release in releases) {
            if (release.draft) continue
            val byBuild = release.assets.mapNotNull { asset ->
                val build = assetPattern.find(asset.name)?.groupValues?.get(1)?.toIntOrNull()
                    ?: return@mapNotNull null
                build to asset
            }.groupBy({ it.first }, { it.second })
            for ((build, assets) in byBuild) {
                if (build <= bestBuild) continue
                val asset = selectBestUpdateAsset(
                    assets = assets.map {
                        AppUpdateAssetCandidate(
                            name = it.name,
                            downloadUrl = it.browserDownloadUrl,
                            size = it.size,
                            contentType = it.contentType,
                        )
                    },
                    selector = selector,
                ) ?: continue
                bestBuild = build
                val name = release.name?.takeIf { it.isNotBlank() } ?: release.tagName.orEmpty()
                best = AppUpdate(
                    tag = tag(build),
                    title = "$name (build $build)".trim(),
                    notes = release.body.orEmpty(),
                    releaseUrl = release.htmlUrl,
                    assetName = asset.name,
                    assetUrl = asset.downloadUrl,
                    assetSizeBytes = asset.size,
                )
            }
        }
        return best
    }
}
