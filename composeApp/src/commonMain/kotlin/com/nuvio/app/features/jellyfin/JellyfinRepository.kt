package com.nuvio.app.features.jellyfin

import com.nuvio.app.features.catalog.CatalogTarget
import com.nuvio.app.features.home.HomeCatalogSection
import com.nuvio.app.features.profiles.ProfileRepository
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

object JellyfinRepository {
    private val VIDEO_LIBRARY_TYPES = setOf("movies", "tvshows", "mixed")
    private const val KEY_SERVER = "server_url"
    private const val KEY_SERVER_NAME = "server_name"
    private const val KEY_USER_ID = "user_id"
    private const val KEY_USER_NAME = "user_name"
    private const val KEY_TOKEN = "access_token"

    private const val HIDDEN_LIBRARIES_KEY_PREFIX = "hidden_libraries_p"
    private const val SEERR_URL_KEY = "seerr_url"
    private const val SEERR_API_KEY_KEY = "seerr_api_key"
    private const val SEERR_REQUESTED_KEY = "seerr_requested_ids"

    private const val SEARCH_SECTION_LIMIT = 24
    private const val SEARCH_DEBOUNCE_MS = 350L
    private const val PAGE_SIZE = 60
    private const val MAX_MERGED_SEARCH_ITEMS = 200

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val _uiState = MutableStateFlow(JellyfinUiState())
    val uiState: StateFlow<JellyfinUiState> = _uiState.asStateFlow()

    private var initialized = false
    private var librariesJob: Job? = null
    private var itemsJob: Job? = null
    private var detailJob: Job? = null
    private var seerrJob: Job? = null
    private var lastAppliedProfileId: Int? = null

    /** True when a signed-in session exists; loads the persisted session on first call. */
    val hasSession: Boolean
        get() {
            initialize()
            return _uiState.value.session != null
        }

    /** Stable key for search request caching; null when signed out. */
    val sessionKey: String?
        get() = _uiState.value.session?.sessionKey

    fun initialize() {
        if (initialized) return
        initialized = true
        loadPersistedSession()
        scope.launch {
            ProfileRepository.state
                .map { it.activeProfile?.profileIndex ?: 1 }
                .distinctUntilChanged()
                .collect { profileId -> applyProfile(profileId) }
        }
    }

    /** Hidden libraries are per Nuvio profile; switching profiles swaps the visible set. */
    private fun applyProfile(profileId: Int) {
        if (lastAppliedProfileId == null) {
            lastAppliedProfileId = profileId
            _uiState.update { it.copy(hiddenLibraryIds = loadHiddenLibraryIds(profileId)) }
            return
        }
        if (lastAppliedProfileId == profileId) return
        lastAppliedProfileId = profileId
        _uiState.update { it.copy(hiddenLibraryIds = loadHiddenLibraryIds(profileId)) }
        refresh()
        if (_uiState.value.session != null) loadItems(reset = true)
    }

    private fun loadPersistedSession() {
        val server = JellyfinPlatform.loadString(KEY_SERVER)?.takeIf { it.isNotBlank() } ?: return
        val userId = JellyfinPlatform.loadString(KEY_USER_ID)?.takeIf { it.isNotBlank() } ?: return
        val token = JellyfinPlatform.loadString(KEY_TOKEN)?.takeIf { it.isNotBlank() } ?: return
        val session = JellyfinSession(
            serverUrl = server,
            serverName = JellyfinPlatform.loadString(KEY_SERVER_NAME)?.takeIf { it.isNotBlank() } ?: "Jellyfin",
            userId = userId,
            userName = JellyfinPlatform.loadString(KEY_USER_NAME).orEmpty().ifBlank { "user" },
            accessToken = token,
        )
        _uiState.update {
            it.copy(
                session = session,
                hiddenLibraryIds = loadHiddenLibraryIds(currentProfileId()),
                seerrConnected = seerrSettings() != null,
                seerrRequestedIds = loadSeerrRequestedIds(),
            )
        }
        refresh()
    }

    fun signIn(serverUrl: String, username: String, password: String) {
        _uiState.update { it.copy(isLoadingSession = true, sessionError = null) }
        scope.launch {
            val result = try {
                Result.success(JellyfinClient.authenticate(serverUrl, username, password))
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                Result.failure<JellyfinSession>(error)
            }
            result.fold(
                onSuccess = { session ->
                    JellyfinPlatform.saveString(KEY_SERVER, session.serverUrl)
                    JellyfinPlatform.saveString(KEY_SERVER_NAME, session.serverName)
                    JellyfinPlatform.saveString(KEY_USER_ID, session.userId)
                    JellyfinPlatform.saveString(KEY_USER_NAME, session.userName)
                    JellyfinPlatform.saveString(KEY_TOKEN, session.accessToken)
                    _uiState.update {
                        JellyfinUiState(
                            session = session,
                            isLoadingSession = false,
                            hiddenLibraryIds = loadHiddenLibraryIds(currentProfileId()),
                            seerrConnected = seerrSettings() != null,
                            seerrRequestedIds = loadSeerrRequestedIds(),
                        )
                    }
                    refresh()
                },
                onFailure = { error ->
                    _uiState.update {
                        it.copy(isLoadingSession = false, sessionError = error.message ?: "Sign-in failed")
                    }
                },
            )
        }
    }

    fun signOut() {
        librariesJob?.cancel()
        itemsJob?.cancel()
        detailJob?.cancel()
        listOf(KEY_SERVER, KEY_SERVER_NAME, KEY_USER_ID, KEY_USER_NAME, KEY_TOKEN).forEach { key ->
            JellyfinPlatform.saveString(key, null)
        }
        _uiState.value = JellyfinUiState()
    }

    fun refresh() {
        val session = _uiState.value.session ?: return
        librariesJob?.cancel()
        librariesJob = scope.launch {
            val libraries = try {
                JellyfinClient.getLibraries(session)
            } catch (error: CancellationException) {
                throw error
            } catch (_: Throwable) {
                emptyList()
            }
            var selectionChanged = false
            _uiState.update { state ->
                val visible = libraries.filter { it.id !in state.hiddenLibraryIds }
                val selected = visible.firstOrNull { it.id == state.selectedLibraryId }
                    ?: visible.firstOrNull { lib -> lib.collectionType != null && lib.collectionType in VIDEO_LIBRARY_TYPES }
                    ?: visible.firstOrNull()
                selectionChanged = selected?.id != state.selectedLibraryId
                state.copy(
                    libraries = libraries,
                    selectedLibraryId = selected?.id,
                )
            }
            if (selectionChanged || _uiState.value.items.isEmpty()) loadItems(reset = true)
        }
    }

    fun selectLibrary(libraryId: String) {
        val current = _uiState.value
        if (current.selectedLibraryId == libraryId) return
        _uiState.update { it.copy(selectedLibraryId = libraryId, searchQuery = "", items = emptyList(), totalItemCount = 0, itemsError = null) }
        loadItems(reset = true)
    }

    fun setSearchQuery(query: String) {
        _uiState.update { it.copy(searchQuery = query) }
        loadItems(reset = true, debounce = true)
    }

    fun setSortLatestFirst(latest: Boolean) {
        val current = _uiState.value
        if (current.sortLatestFirst == latest) return
        _uiState.update { it.copy(sortLatestFirst = latest) }
        loadItems(reset = true)
    }

    fun loadMore() {
        val state = _uiState.value
        val session = state.session ?: return
        if (state.isLoadingItems || !state.canLoadMore) return
        loadItems(reset = false, sessionOverride = session)
    }

    fun retryItems() {
        loadItems(reset = true)
    }

    private fun loadItems(reset: Boolean, debounce: Boolean = false, sessionOverride: JellyfinSession? = null) {
        val session = sessionOverride ?: _uiState.value.session ?: return
        itemsJob?.cancel()
        itemsJob = scope.launch {
            if (debounce) delay(SEARCH_DEBOUNCE_MS)
            val state = _uiState.value
            if (state.session?.sessionKey != session.sessionKey) return@launch
            val searchTerm = state.searchQuery.trim().takeIf { it.isNotBlank() }
            _uiState.update {
                it.copy(
                    isLoadingItems = true,
                    itemsError = null,
                    items = if (reset) emptyList() else it.items,
                    totalItemCount = if (reset) 0 else it.totalItemCount,
                    seerrResults = if (searchTerm == null) emptyList() else it.seerrResults,
                )
            }
            val result = try {
                val page = if (searchTerm == null) {
                    // Video libraries browse recursively by item type: titles inside nested
                    // folders (e.g. a "complete saga" folder holding several movies) show up as
                    // separate playable items instead of one unplayable folder entry.
                    val library = state.libraries.firstOrNull { it.id == state.selectedLibraryId }
                    val browseTypes = browseItemTypes(library?.collectionType)
                    JellyfinClient.getItems(
                        session = session,
                        parentId = state.selectedLibraryId,
                        startIndex = if (reset) 0 else state.items.size,
                        sortBy = if (state.sortLatestFirst) "DateCreated" else "SortName",
                        sortAscending = !state.sortLatestFirst,
                        includeItemTypes = browseTypes,
                        recursive = browseTypes != null,
                    )
                } else {
                    // Search only the visible libraries: hidden ones must not leak results.
                    searchVisibleLibraries(session, state, searchTerm)
                }
                Result.success(page)
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                Result.failure<JellyfinItemPage>(error)
            }
            if (searchTerm != null) refreshSeerrResults(searchTerm)
            result.fold(
                onSuccess = { page ->
                    _uiState.update { current ->
                        if (current.session?.sessionKey != session.sessionKey) return@update current
                        val merged = if (reset) page.items else current.items + page.items
                        val deduped = merged.distinctBy { it.id }
                        current.copy(
                            items = deduped,
                            totalItemCount = maxOf(page.totalRecordCount, deduped.size),
                            isLoadingItems = false,
                            itemsError = null,
                        )
                    }
                },
                onFailure = { error ->
                    _uiState.update { current ->
                        if (current.session?.sessionKey != session.sessionKey) return@update current
                        current.copy(
                            isLoadingItems = false,
                            itemsError = error.message ?: "Could not load items",
                        )
                    }
                },
            )
        }
    }

    /** Selects an item for the detail panel; loads seasons and the first season's episodes for series. */
    fun selectItem(item: JellyfinItem) {
        detailJob?.cancel()
        val loadsChildren = item.isSeries || item.isFolder
        _uiState.update {
            it.copy(
                selectedItemId = item.id,
                selectedDetail = item,
                seasons = if (item.isSeries) it.seasons else emptyList(),
                episodes = if (loadsChildren) it.episodes else emptyList(),
                selectedSeasonId = null,
                isLoadingDetail = loadsChildren,
                detailError = null,
            )
        }
        when {
            item.isSeries -> loadSeriesDetail(item.id)
            item.isFolder -> loadFolderChildren(item.id)
        }
    }

    fun selectItemById(itemId: String) {
        val session = _uiState.value.session ?: return
        val cached = _uiState.value.items.firstOrNull { it.id == itemId }
        if (cached != null) {
            selectItem(cached)
            return
        }
        detailJob?.cancel()
        _uiState.update { it.copy(selectedItemId = itemId, selectedDetail = null, seasons = emptyList(), episodes = emptyList(), selectedSeasonId = null, isLoadingDetail = true, detailError = null) }
        detailJob = scope.launch {
            val result = try {
                Result.success(JellyfinClient.getItem(session, itemId))
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                Result.failure<JellyfinItem?>(error)
            }
            val loaded = result.getOrNull()
            _uiState.update { state ->
                if (state.selectedItemId != itemId) return@update state
                when {
                    loaded != null -> state.copy(
                        selectedDetail = loaded,
                        isLoadingDetail = loaded.isSeries || loaded.isFolder,
                        detailError = null,
                    )
                    result.isFailure -> state.copy(isLoadingDetail = false, detailError = result.exceptionOrNull()?.message ?: "Could not load item")
                    else -> state.copy(isLoadingDetail = false, detailError = "Item not found")
                }
            }
            when {
                loaded?.isSeries == true -> loadSeriesDetail(loaded.id)
                loaded?.isFolder == true -> loadFolderChildren(loaded.id)
            }
        }
    }

    private fun loadSeriesDetail(seriesId: String) {
        val session = _uiState.value.session ?: return
        detailJob = scope.launch {
            val seasonsResult = try {
                Result.success(JellyfinClient.getSeasons(session, seriesId))
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                Result.failure<List<JellyfinItem>>(error)
            }
            val seasons = seasonsResult.getOrNull().orEmpty()
            _uiState.update { state ->
                if (state.selectedItemId != seriesId) return@update state
                if (seasonsResult.isFailure && seasons.isEmpty()) {
                    state.copy(isLoadingDetail = false, detailError = seasonsResult.exceptionOrNull()?.message ?: "Could not load seasons")
                } else {
                    state.copy(seasons = seasons, selectedSeasonId = seasons.firstOrNull()?.id, isLoadingDetail = false, detailError = null)
                }
            }
            val seasonId = _uiState.value.seasons.firstOrNull()?.id
            if (seasonId != null) {
                loadEpisodes(seriesId, seasonId)
            }
        }
    }

    fun selectSeason(seasonId: String) {
        val state = _uiState.value
        val seriesId = state.selectedDetail?.id ?: return
        if (state.selectedSeasonId == seasonId) return
        _uiState.update { it.copy(selectedSeasonId = seasonId, episodes = emptyList()) }
        loadEpisodes(seriesId, seasonId)
    }

    private fun loadEpisodes(seriesId: String, seasonId: String) {
        val session = _uiState.value.session ?: return
        detailJob = scope.launch {
            // loading flag: reuse isLoadingDetail for the episodes list
            _uiState.update { it.copy(isLoadingDetail = true) }
            val result = try {
                Result.success(JellyfinClient.getEpisodes(session, seriesId, seasonId))
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                Result.failure<List<JellyfinItem>>(error)
            }
            val episodes = result.getOrNull().orEmpty()
            _uiState.update { state ->
                if (state.selectedDetail?.id != seriesId || state.selectedSeasonId != seasonId) return@update state
                state.copy(
                    episodes = episodes,
                    isLoadingDetail = false,
                    detailError = if (result.isFailure && episodes.isEmpty()) {
                        result.exceptionOrNull()?.message ?: "Could not load episodes"
                    } else {
                        null
                    },
                )
            }
        }
    }

    // ---- playback / image URL helpers (used by MainAppContent when building PlayerLaunch) ----

    fun streamUrlFor(item: JellyfinItem): String? =
        _uiState.value.session?.let { session -> JellyfinClient.streamUrl(session, item) }

    fun posterUrlFor(item: JellyfinItem, maxWidth: Int = 480): String? =
        _uiState.value.session?.let { session -> JellyfinClient.primaryImageUrl(session, item, maxWidth) }

    fun backdropUrlFor(item: JellyfinItem): String? =
        _uiState.value.session?.let { session -> JellyfinClient.backdropImageUrl(session, item) }

    /** Lists the playable files inside a nested folder (movies/adult "saga" folders, like episodes for shows). */
    private fun loadFolderChildren(folderId: String) {
        val session = _uiState.value.session ?: return
        detailJob = scope.launch {
            _uiState.update { it.copy(isLoadingDetail = true) }
            val result = try {
                Result.success(
                    JellyfinClient.getItems(
                        session = session,
                        parentId = folderId,
                        startIndex = 0,
                        limit = 500,
                        sortBy = "SortName",
                        sortAscending = true,
                        includeItemTypes = "Movie,Video,Episode",
                        recursive = true,
                    ),
                )
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                Result.failure<JellyfinItemPage>(error)
            }
            val children = result.getOrNull()?.items.orEmpty()
            _uiState.update { state ->
                if (state.selectedDetail?.id != folderId) return@update state
                state.copy(
                    episodes = children,
                    isLoadingDetail = false,
                    detailError = if (result.isFailure && children.isEmpty()) {
                        result.exceptionOrNull()?.message ?: "Could not load the folder contents"
                    } else {
                        null
                    },
                )
            }
        }
    }

    // ---- Jellyseerr request integration ----

    fun connectSeerr(url: String, apiKey: String) {
        val normalized = SeerrClient.normalizeUrl(url) ?: run {
            _uiState.update { it.copy(seerrStatusMessage = "Enter a valid Seerr address") }
            return
        }
        if (apiKey.isBlank()) {
            _uiState.update { it.copy(seerrStatusMessage = "Enter the Seerr API key (Seerr Settings → API Key)") }
            return
        }
        _uiState.update { it.copy(seerrStatusMessage = "Checking Seerr…") }
        scope.launch {
            val ok = SeerrClient.testConnection(normalized, apiKey)
            _uiState.update { state ->
                if (ok) {
                    JellyfinPlatform.saveString(SEERR_URL_KEY, normalized)
                    JellyfinPlatform.saveString(SEERR_API_KEY_KEY, apiKey)
                    state.copy(seerrConnected = true, seerrStatusMessage = null)
                } else {
                    state.copy(seerrStatusMessage = "Could not reach Seerr or the API key was rejected")
                }
            }
            if (ok && _uiState.value.searchQuery.isNotBlank()) {
                refreshSeerrResults(_uiState.value.searchQuery)
            }
        }
    }

    fun disconnectSeerr() {
        JellyfinPlatform.saveString(SEERR_URL_KEY, null)
        JellyfinPlatform.saveString(SEERR_API_KEY_KEY, null)
        _uiState.update { it.copy(seerrConnected = false, seerrResults = emptyList(), seerrStatusMessage = null) }
    }

    fun requestViaSeerr(result: SeerrSearchResult) {
        val settings = seerrSettings() ?: return
        val (url, key) = settings
        _uiState.update { it.copy(seerrStatusMessage = "Requesting \"${result.title}\"…") }
        scope.launch {
            val outcome = SeerrClient.request(url, key, result.mediaType, result.tmdbId)
            _uiState.update { state ->
                when (outcome) {
                    SeerrClient.RequestOutcome.Created, SeerrClient.RequestOutcome.AlreadyRequested -> {
                        val updatedIds = state.seerrRequestedIds + result.tmdbId
                        JellyfinPlatform.saveString(SEERR_REQUESTED_KEY, updatedIds.joinToString("\n") { it.toString() })
                        state.copy(
                            seerrRequestedIds = updatedIds,
                            seerrStatusMessage = if (outcome == SeerrClient.RequestOutcome.Created) {
                                "Requested \"${result.title}\" — Radarr/Sonarr will download it; it shows up here once ready"
                            } else {
                                "\"${result.title}\" was already requested"
                            },
                        )
                    }
                    SeerrClient.RequestOutcome.Unauthorized ->
                        state.copy(seerrStatusMessage = "Seerr rejected the API key")
                    SeerrClient.RequestOutcome.Failed ->
                        state.copy(seerrStatusMessage = "Seerr request failed — try again")
                }
            }
        }
    }

    private fun refreshSeerrResults(query: String) {
        val settings = seerrSettings() ?: run {
            _uiState.update { it.copy(seerrResults = emptyList()) }
            return
        }
        val (url, key) = settings
        seerrJob?.cancel()
        seerrJob = scope.launch {
            val results = SeerrClient.search(url, key, query)
            _uiState.update { state ->
                val knownTmdb = state.items.mapNotNull { it.tmdbId }.filter { it.isNotBlank() }.toSet()
                val filtered = results.orEmpty().filterNot { result ->
                    result.isAvailable || result.tmdbId.toString() in knownTmdb
                }
                state.copy(seerrResults = filtered)
            }
        }
    }

    private fun seerrSettings(): Pair<String, String>? {
        val url = JellyfinPlatform.loadString(SEERR_URL_KEY)?.takeIf { it.isNotBlank() } ?: return null
        val key = JellyfinPlatform.loadString(SEERR_API_KEY_KEY)?.takeIf { it.isNotBlank() } ?: return null
        return url to key
    }

    private fun loadSeerrRequestedIds(): Set<Int> =
        JellyfinPlatform.loadString(SEERR_REQUESTED_KEY)
            ?.split('\n')
            ?.mapNotNull { it.trim().toIntOrNull() }
            ?.toSet()
            .orEmpty()

    // ---- library visibility (hide/show in the Jellyfin screen) ----

    fun toggleLibraryHidden(libraryId: String) {
        val current = _uiState.value
        val updated = if (libraryId in current.hiddenLibraryIds) {
            current.hiddenLibraryIds - libraryId
        } else {
            current.hiddenLibraryIds + libraryId
        }
        JellyfinPlatform.saveString(hiddenLibrariesKey(currentProfileId()), updated.joinToString("\n"))
        _uiState.update { it.copy(hiddenLibraryIds = updated) }
        // Hiding the library being browsed falls back to the first visible one.
        if (current.selectedLibraryId == libraryId && libraryId in updated) {
            val nextLibrary = current.libraries.firstOrNull { it.id !in updated }
            _uiState.update {
                it.copy(
                    selectedLibraryId = nextLibrary?.id,
                    searchQuery = "",
                    items = emptyList(),
                    totalItemCount = 0,
                    itemsError = null,
                )
            }
            loadItems(reset = true)
        }
    }

    /**
     * Item types used when browsing a library, per collection type: movies (incl. adult movies
     * libraries) flatten to Movie, tv shows to Series, mixed to both. Unknown library types keep
     * the legacy non-recursive direct-children listing.
     */
    private fun browseItemTypes(collectionType: String?): String? = when (collectionType?.lowercase()) {
        "movies" -> "Movie,Folder"
        "tvshows" -> "Series,Folder"
        "mixed" -> "Movie,Series,Folder"
        else -> null
    }

    private fun loadHiddenLibraryIds(profileId: Int): Set<String> =
        JellyfinPlatform.loadString(hiddenLibrariesKey(profileId))
            ?.split('\n')
            ?.filter { it.isNotBlank() }
            ?.toSet()
            .orEmpty()

    private fun hiddenLibrariesKey(profileId: Int): String = "$HIDDEN_LIBRARIES_KEY_PREFIX$profileId"

    private fun currentProfileId(): Int = runCatching { ProfileRepository.activeProfileId }.getOrDefault(1)

    /** Server-side search across every VISIBLE library (parallel), merged and de-duplicated. */
    private suspend fun searchVisibleLibraries(
        session: JellyfinSession,
        state: JellyfinUiState,
        searchTerm: String,
    ): JellyfinItemPage {
        val visibleIds = state.libraries
            .filter { it.id !in state.hiddenLibraryIds }
            .map { it.id }
        if (visibleIds.isEmpty()) return JellyfinItemPage(emptyList(), 0)
        val perLibrary = PAGE_SIZE
        val pages = coroutineScope {
            visibleIds.map { libraryId ->
                async {
                    runCatching {
                        JellyfinClient.getItems(
                            session = session,
                            parentId = libraryId,
                            startIndex = 0,
                            limit = perLibrary,
                            sortBy = "SortName",
                            searchTerm = searchTerm,
                            includeItemTypes = "Movie,Series,Episode",
                            recursive = true,
                        )
                    }.getOrNull()
                }
            }.awaitAll()
        }
        val merged = pages
            .filterNotNull()
            .flatMap { it.items }
            .distinctBy { it.id }
            .sortedBy { it.name.lowercase() }
        return JellyfinItemPage(merged.take(MAX_MERGED_SEARCH_ITEMS), merged.size.coerceAtMost(MAX_MERGED_SEARCH_ITEMS))
    }

    // ---- global Search integration ----

    /**
     * Runs a server-side search for the app's Search screen. Returns null when there are no
     * matches so the caller can leave the section out (instead of treating it as an error).
     */
    suspend fun searchPreviews(query: String): List<com.nuvio.app.features.home.MetaPreview>? {
        val session = _uiState.value.session ?: return null
        if (query.isBlank()) return null
        val state = _uiState.value
        val visibleIds = state.libraries.filter { it.id !in state.hiddenLibraryIds }.map { it.id }
        if (visibleIds.isEmpty()) return null
        val pages = try {
            coroutineScope {
                visibleIds.map { libraryId ->
                    async {
                        runCatching {
                            JellyfinClient.getItems(
                                session = session,
                                parentId = libraryId,
                                startIndex = 0,
                                limit = SEARCH_SECTION_LIMIT,
                                sortBy = "SortName",
                                searchTerm = query.trim(),
                                includeItemTypes = "Movie,Series,Episode",
                                recursive = true,
                            )
                        }.getOrNull()
                    }
                }.awaitAll()
            }
        } catch (error: CancellationException) {
            throw error
        } catch (_: Throwable) {
            return null
        }
        val page = JellyfinItemPage(
            items = pages.filterNotNull().flatMap { it.items }.distinctBy { it.id },
            totalRecordCount = 0,
        )
        val previews = page.items.map { item ->
            item.toMetaPreview(
                posterUrl = JellyfinClient.primaryImageUrl(session, item),
                backdropUrl = JellyfinClient.backdropImageUrl(session, item),
            )
        }
        return previews.takeIf { it.isNotEmpty() }
    }

    suspend fun searchSection(query: String): HomeCatalogSection? {
        val session = _uiState.value.session ?: return null
        val previews = searchPreviews(query) ?: return null
        return HomeCatalogSection(
            key = "jellyfin:search:${query.lowercase()}",
            title = "Jellyfin · ${session.serverName}",
            subtitle = session.userName,
            addonName = session.serverName,
            target = CatalogTarget.Addon(
                manifestUrl = session.serverUrl,
                contentType = "movie",
                catalogId = "jellyfin",
                supportsPagination = false,
            ),
            items = previews,
            availableItemCount = previews.size,
            hasMore = false,
        )
    }

}
