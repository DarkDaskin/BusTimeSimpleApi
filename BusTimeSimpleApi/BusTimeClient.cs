using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.RateLimiting;
using AngleSharp;
using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using AngleSharp.Io.Network;
using BusTimeSimpleApi.Models;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.Extensions.Options;

namespace BusTimeSimpleApi;

public class BusTimeClient
{
    private const string BaseAddress = "https://ru.busti.me/";

    private static readonly string CacheDirectoryPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "BusTime");
    private static readonly string CitiesJsonPath = Path.Combine(CacheDirectoryPath, "cities.json");
    private static readonly string StationsDirectoryPath = Path.Combine(CacheDirectoryPath, "stations");
    private static readonly string StationMappingsDirectoryPath = Path.Combine(CacheDirectoryPath, "station-mappings");

    private readonly IBrowsingContext _browsingContext;
    private readonly JsonSerializerOptions _jsonSerializerOptions;
    private readonly ILogger<BusTimeClient> _logger;
    private readonly HashSet<string> _existingCities = new();
    private readonly SemaphoreSlim _existingCitiesSemaphore = new(1, 1);
    private readonly Dictionary<int, ForecastRequestInfo> _forecastRequestInfos = new();
    private readonly SemaphoreSlim _forecastRequestInfosSemaphore = new(1, 1);
    private volatile bool _cityUpdateInProgress, _stationUpdateInProgress;

    public BusTimeClient(RateLimiter rateLimiter, IOptions<JsonOptions> jsonOptions, ILogger<BusTimeClient> logger)
    {
        var httpClient = new HttpClient(new ClientSideRateLimitedHandler(rateLimiter))
        {
            DefaultRequestHeaders =
            {
                // Maintain cities sorting made for Russia.
                AcceptLanguage = { new("ru") },
                UserAgent = { new("BusTimeSimpleApi", "1.0") },
            }
        };
        _browsingContext = BrowsingContext.New(Configuration.Default.With(new HttpClientRequester(httpClient)).WithDefaultLoader());
        _jsonSerializerOptions = jsonOptions.Value.SerializerOptions;
        _logger = logger;
    }

    public string CitiesJsonLocation => CitiesJsonPath;

    public async Task UpdateCitiesAsync()
    {
        _logger.LogInformation("Updating city list...");

        if (_cityUpdateInProgress)
        {
            LogUpdateCancelled();
            return;
        }
        _cityUpdateInProgress = true;

        var document = await _browsingContext.OpenAsync($"{BaseAddress}");
        var items = document.QuerySelectorAll<IHtmlAnchorElement>(".accordion .item");
        var cities = items.Select(item => new City(
            Code: ExtractCode(item.Href),
            Name: item.Title?.Split(',').First() ?? "",
            Country: item.Ancestors<IHtmlDivElement>().SingleOrDefault(ancestor => ancestor.ClassList.Contains("content"))?
                .PreviousElementSibling?.FindChild<IText>()?.Text.Trim() ?? ""))
            .ToArray();

        await _existingCitiesSemaphore.WaitAsync();
        try
        {
            UpdateExistingCities(cities);
        }
        finally
        {
            _existingCitiesSemaphore.Release();
        }

        EnsureDirectory(CitiesJsonPath);
        await using var jsonStream = OpenWrite(CitiesJsonPath);
        await JsonSerializer.SerializeAsync(jsonStream, cities, _jsonSerializerOptions);

        _cityUpdateInProgress = false;

        _logger.LogInformation("City list updated with {Count} entries.", cities.Length);
    }

    private void LogUpdateCancelled() => _logger.LogWarning("Update cancelled due to another update being in progress.");

    private static string ExtractCode(string url) => url.Split('/', StringSplitOptions.RemoveEmptyEntries).Last();

    private static void EnsureDirectory(string path) => Directory.CreateDirectory(Path.GetDirectoryName(path)!);

    private static FileStream OpenWrite(string path) => File.Open(path, FileMode.Create);

    private void UpdateExistingCities(IEnumerable<City> cities)
    {
        _existingCities.Clear();
        foreach (var city in cities)
            _existingCities.Add(city.Code);
    }

    public async ValueTask<bool> CityExistsAsync(string cityCode)
    {
        if (_existingCities.Count == 0)
        {
            if (File.Exists(CitiesJsonPath))
            {
                await using var jsonStream = File.OpenRead(CitiesJsonPath);
                var cities = (await JsonSerializer.DeserializeAsync<IEnumerable<City>>(jsonStream, _jsonSerializerOptions))!;

                await _existingCitiesSemaphore.WaitAsync();
                try
                {
                    UpdateExistingCities(cities);
                }
                finally
                {
                    _existingCitiesSemaphore.Release();
                }
            }
            else
                await UpdateCitiesAsync();
        }

        return _existingCities.Contains(cityCode);
    }

    public string GetStationJsonLocation(string cityCode) => Path.Combine(StationsDirectoryPath, $"{cityCode}.json");

    private string GetStationMappingJsonLocation(string cityCode) => Path.Combine(StationMappingsDirectoryPath, $"{cityCode}.json");

    public async Task UpdateStationsAsync(bool fullUpdate = false)
    {
        _logger.LogInformation("Updating station list ({UpdateType} update)...", fullUpdate ? "full" : "delta");

        if (_stationUpdateInProgress)
        {
            LogUpdateCancelled();
            return;
        }
        _stationUpdateInProgress = true;

        var cities = await ReadCitiesAsync();

        foreach (var city in cities)
            await UpdateStationsAsync(city.Code, fullUpdate);

        _stationUpdateInProgress = false;

        _logger.LogInformation("Station list updated for {Count} cities.", cities.Count);
    }

    private async Task<IReadOnlyCollection<City>> ReadCitiesAsync()
    {
        if (!File.Exists(CitiesJsonPath))
            await UpdateCitiesAsync();

        await using var jsonStream = File.OpenRead(CitiesJsonPath);
        return (await JsonSerializer.DeserializeAsync<IReadOnlyCollection<City>>(jsonStream, _jsonSerializerOptions))!;
    }

    public async Task UpdateStationsAsync(string cityCode, bool fullUpdate = false)
    {
        _logger.LogInformation("Updating station list for '{CityCode}' ({UpdateType} update)...", cityCode, fullUpdate ? "full" : "delta");

        var stationListDocument = await _browsingContext.OpenAsync($"{BaseAddress}{cityCode}/stop/");
        var stationListItems = stationListDocument.QuerySelectorAll<IHtmlAnchorElement>(".four .item");
        var stationGroups = stationListItems.Select(item => new StationGroup(
            Code: ExtractCode(item.Href), 
            Name: item.TextContent.Trim()))
            .DistinctBy(stationGroup => stationGroup.Code).ToArray();

        ILookup<string, Station>? existingStationsByCode = null;
        Dictionary<int, int>? existingStationMappings = null;
        if (!fullUpdate)
        {
            var existingStations = await ReadStationsAsync(cityCode);
            existingStationsByCode = existingStations?.ToLookup(station => station.Code);
            existingStationMappings = await ReadStationMappingsAsync(cityCode);
        }

        var allStations = new List<Station>();
        var stationMappings = new Dictionary<int, int>();
        var stationGroupsProcessedCount = 0;
        const int reportMilestoneSeconds = 30;
        var reportStopwatch = Stopwatch.StartNew();
        foreach (var stationGroup in stationGroups)
        {
            var existingStationsInGroup = existingStationsByCode?[stationGroup.Code].ToArray() ?? [];
            var hasMappings = existingStationMappings != null &&
                              existingStationsInGroup.All(station => existingStationMappings.ContainsKey(station.Id));
            var requiresUpdate = fullUpdate || !existingStationsInGroup.Any() || !hasMappings;
            if (requiresUpdate)
            {
                var updateReason = fullUpdate ? "full update" :
                    !existingStationsInGroup.Any() ? "station is new" :
                    !hasMappings ? "missing mappings" : "unknown";
                _logger.LogDebug("Updating stations for group '{StationCode}' (reason: {UpdateReason})...", stationGroup.Code, updateReason);

                var stationGroupDocument = await _browsingContext.OpenAsync($"{BaseAddress}{cityCode}/stop/{stationGroup.Code}/");
                var stationArrows = stationGroupDocument.QuerySelectorAll("h3 .fa-arrow-right");
                var stationRoutes = stationGroupDocument.QuerySelectorAll("h3").First(h3 => h3.TextContent.Contains("Маршруты"))
                    .ParentElement?.QuerySelectorAll<IHtmlAnchorElement>("a") ?? [];
                var stationType = stationRoutes.All(route => route.Href.Contains("tramway")) ? StationType.Tram : StationType.Regular;
                var addedStationCount = 0;
                var currentGroupStations = new List<Station>();
                var oldUiForecastTablesByStationId = new Dictionary<int, ForecastTableMatch>();
                foreach (var arrow in stationArrows)
                {
                    var column = arrow.Ancestors<IHtmlDivElement>().First(ancestor => ancestor.ClassList.Contains("column"));
                    var station = new Station(
                        Id: int.Parse(ExtractCode((column.QuerySelector(".fa-desktop")?.ParentElement as IHtmlAnchorElement)?.Href ?? "0")),
                        Code: stationGroup.Code,
                        Name: stationGroup.Name,
                        Direction: arrow.ParentElement!.TextContent,
                        Type: stationType);
                    currentGroupStations.Add(station);
                    allStations.Add(station);
                    addedStationCount++;

                    var oldUiForecastTable = column.QuerySelector<IHtmlTableElement>("table")!;
                    oldUiForecastTablesByStationId.Add(station.Id, new ForecastTableMatch(oldUiForecastTable));
                }

                var unmappedStations = new List<Station>();
                if (currentGroupStations.Count > 1)
                {
                    foreach (var station in currentGroupStations)
                    {
                        if (!fullUpdate && existingStationMappings != null && existingStationMappings.TryGetValue(station.Id, out var mappedStationId))
                        {
                            if (!AddStationMapping(station.Id, mappedStationId))
                                unmappedStations.Add(station);

                            continue;
                        }

                        _logger.LogDebug("Determining mapping for station {StationId}...", station.Id);

                        var closestMatch = await GetClosestStationMatchByForecast(cityCode, station.Id, oldUiForecastTablesByStationId);
                        if (closestMatch.Score > 0)
                        {
                            if (closestMatch.Count == 1)
                            {
                                mappedStationId = closestMatch.MappedStationId!.Value;
                                oldUiForecastTablesByStationId[mappedStationId].StationId = station.Id;
                                if (!AddStationMapping(station.Id, mappedStationId))
                                    unmappedStations.Add(station);
                            }
                            else
                            {
                                _logger.LogWarning("Unable to determine station mapping for station {StationId}. Ambuguous forecast table match.", station.Id);
                                unmappedStations.Add(station);
                            }
                        }
                        else
                            unmappedStations.Add(station);
                    }
                }
                else
                    unmappedStations.Add(currentGroupStations[0]);

                // If only one station is left unmapped, map it automatically.
                if (unmappedStations.Count == 1)
                {
                    var mappedStationId = oldUiForecastTablesByStationId.Single(kv => kv.Value.StationId == null).Key;
                    AddStationMapping(unmappedStations[0].Id, mappedStationId);
                }
                else
                    foreach (var station in unmappedStations)
                        _logger.LogWarning("Unable to determine station mapping for station {StationId}. No forecast table matches.", station.Id);

                _logger.LogDebug("Stations for group '{StationCode}' updated with {Count} entries.", stationGroup.Code, addedStationCount);
            }
            else
            {
                Debug.Assert(existingStationsByCode != null, nameof(existingStationsByCode) + " != null");
                Debug.Assert(existingStationMappings != null, nameof(existingStationMappings) + " != null");
                foreach (var station in existingStationsByCode[stationGroup.Code])
                {
                    allStations.Add(station);
                    stationMappings.Add(station.Id, existingStationMappings[station.Id]);
                }
            }

            stationGroupsProcessedCount++;
            if (reportStopwatch.Elapsed.TotalSeconds > reportMilestoneSeconds)
            {
                _logger.LogInformation("Station list update for '{CityCode}' done for {Count}/{TotalCount} station groups.", cityCode, stationGroupsProcessedCount, stationGroups.Length);
                reportStopwatch.Restart();
            }
        }

        // Waits have to be done outside this method as sepaphores are not reentrant.
        await _forecastRequestInfosSemaphore.WaitAsync();
        try
        {
            UpdateForecastRequestInfos(cityCode, allStations, stationMappings, true);
        }
        finally
        {
            _forecastRequestInfosSemaphore.Release();
        }

        {
            var path = GetStationJsonLocation(cityCode);
            EnsureDirectory(path);
            await using var jsonStream = OpenWrite(path);
            await JsonSerializer.SerializeAsync(jsonStream, allStations, _jsonSerializerOptions);
        }

        {
            var path = GetStationMappingJsonLocation(cityCode);
            EnsureDirectory(path);
            await using var jsonStream = OpenWrite(path);
            await JsonSerializer.SerializeAsync(jsonStream, stationMappings, _jsonSerializerOptions);
        }

        _logger.LogInformation("Station list for '{CityCode}' updated with {Count} entries.", cityCode, allStations.Count);


        bool AddStationMapping(int stationId, int mappedStationId)
        {
            bool success;
            // ReSharper disable once AssignmentInConditionalExpression
            if (success = stationMappings.TryAdd(stationId, mappedStationId))
                _logger.LogDebug("Station {StationId} maps to {MappedStationId} in old UI.", stationId, mappedStationId);
            else
                _logger.LogWarning("Unable to determine station mapping for station {StationId}. Duplicate station ID.", stationId);
            return success;
        }
    }

    private async Task<IReadOnlyCollection<Station>?> ReadStationsAsync(string cityCode)
    {
        var path = GetStationJsonLocation(cityCode);
        if (!File.Exists(path))
            return null;

        await using var jsonStream = File.OpenRead(path);
        return (await JsonSerializer.DeserializeAsync<IReadOnlyCollection<Station>>(jsonStream, _jsonSerializerOptions))!;
    }

    private async Task<Dictionary<int, int>?> ReadStationMappingsAsync(string cityCode)
    {
        var path = GetStationMappingJsonLocation(cityCode);
        if (!File.Exists(path))
            return null;

        await using var jsonStream = File.OpenRead(path);
        return (await JsonSerializer.DeserializeAsync<Dictionary<int, int>>(jsonStream, _jsonSerializerOptions))!;
    }

    private async Task<StationMatchInfo> GetClosestStationMatchByForecast(string cityCode, int stationId,
        Dictionary<int, ForecastTableMatch> oldUiForecastTablesByStationId)
    {
        var stationDocument = await _browsingContext.OpenAsync($"{BaseAddress}{cityCode}/stop/id/{stationId}/");
        var newUiForecastRows = stationDocument.QuerySelectorAll<IHtmlTableRowElement>("tbody.stop_result tr");
        var newUiForecasts = newUiForecastRows.Select(row => new ForecastCompareInfo(
                Time: TimeOnly.Parse(row.Cells[0].TextContent.Trim()),
                Route: Regex.Match(row.Cells[1].TextContent, @"(?<route>\S+)(?:\s+\+)?").Groups["route"].Value))
            .ToArray();
        foreach (var oldForecastTableMatch in oldUiForecastTablesByStationId.Values)
        {
            // Fuzzy compare forecast tables to determine closest match.
            const int toleranceMinutes = 2;
            var oldUiForecasts = oldForecastTableMatch.Table.Bodies[0].Rows
                .SelectMany(row => row.Cells[1].QuerySelectorAll<IHtmlAnchorElement>("a").Select(anchor =>
                    new ForecastCompareInfo(
                        Time: TimeOnly.Parse(row.Cells[0].TextContent),
                        Route: anchor.TextContent.Trim())));
            foreach (var oldUiForecast in oldUiForecasts)
            {
                if (newUiForecasts.Any(newUiForecast =>
                        // Old UI has transport type prefixes, new UI omits them on initial load.
                        oldUiForecast.Route.Contains(newUiForecast.Route, StringComparison.CurrentCultureIgnoreCase) &&
                        // Times may skew as pages were loaded at slightly different times, so do fuzzy compare.
                        Math.Abs(oldUiForecast.Time.Ticks - newUiForecast.Time.Ticks) / TimeSpan.TicksPerMinute <
                        toleranceMinutes))
                {
                    oldForecastTableMatch.Scores.TryAdd(stationId, 0);
                    oldForecastTableMatch.Scores[stationId]++;
                }
            }
        }

        var closestMatch = oldUiForecastTablesByStationId.ToLookup(kv => kv.Value.Scores.GetValueOrDefault(stationId, 0), kv => kv.Key)
            .MaxBy(g => g.Key);
        var matchCount = closestMatch?.Count() ?? 0;
        return new StationMatchInfo(
            MappedStationId: (closestMatch == null || matchCount != 1) ? null : closestMatch.Single(),
            Score: closestMatch?.Key ?? 0,
            Count: matchCount,
            HasForecasts: newUiForecasts.Any()
        );
    }

    private async Task UpdateForecastRequestInfosAsync()
    {
        var stationJsonFiles = Directory.EnumerateFiles(StationsDirectoryPath, "*.json");
        foreach (var stationJsonFile in stationJsonFiles)
        {
            var cityCode = Path.GetFileNameWithoutExtension(stationJsonFile);

            await using var jsonStream = File.OpenRead(stationJsonFile);
            var stations = (await JsonSerializer.DeserializeAsync<IEnumerable<Station>>(jsonStream, _jsonSerializerOptions))!;


            Dictionary<int, int> stationMappings;
            var stationMappingFile = GetStationMappingJsonLocation(cityCode);
            if (File.Exists(stationMappingFile))
            {
                await using var mappingJsonStream = File.OpenRead(stationMappingFile);
                stationMappings = (await JsonSerializer.DeserializeAsync<Dictionary<int, int>>(mappingJsonStream, _jsonSerializerOptions))!;
            }
            else
                stationMappings = [];

            UpdateForecastRequestInfos(cityCode, stations, stationMappings, false);
        }
    }

    private void UpdateForecastRequestInfos(string cityCode, IEnumerable<Station> stations, Dictionary<int, int> stationMappings, bool removeOld)
    {
        if (removeOld)
        {
            var idsToRemove = _forecastRequestInfos
                .Where(kv => kv.Value.CityCode == cityCode)
                .Select(kv => kv.Key)
                .ToArray();
            foreach (var id in idsToRemove)
                _forecastRequestInfos.Remove(id);
        }

        foreach (var station in stations)
        {
            int? mappedStationId = stationMappings.TryGetValue(station.Id, out var v) ? v : null;
            if (!_forecastRequestInfos.TryAdd(station.Id, new ForecastRequestInfo(cityCode, station.Code, mappedStationId)))
                _logger.LogWarning("Duplicate station ID {StationId}", station.Id);
        }
    }

    public async Task<IEnumerable<Forecast>?> GetForecastsAsync(int stationId)
    {
        _logger.LogInformation("Getting forecast for station '{StationId}'...", stationId);

        IDocument stationGroupDocument;
        ForecastRequestInfo requestInfo;

        await _forecastRequestInfosSemaphore.WaitAsync();
        try
        {
            if (_forecastRequestInfos.Count == 0)
                await UpdateForecastRequestInfosAsync();

            if (!_forecastRequestInfos.TryGetValue(stationId, out requestInfo))
                return null;

            stationGroupDocument = await _browsingContext.OpenAsync($"{BaseAddress}{requestInfo.CityCode}/stop/{requestInfo.StationCode}/");
        }
        finally
        {
            _forecastRequestInfosSemaphore.Release();
        }

        if (requestInfo.MappedStationId == null)
        {
            _logger.LogDebug("Determining mapping for station {StationId}...", stationId);

            var oldUiForecastTablesByStationId = stationGroupDocument.QuerySelectorAll<IHtmlTableElement>("table").ToDictionary(
                table => int.Parse(ExtractCode(table.PreviousElementSibling?.QuerySelector<IHtmlAnchorElement>("a")?.Href ?? "0")),
                table => new ForecastTableMatch(table));
            var closestMatch = await GetClosestStationMatchByForecast(requestInfo.CityCode, stationId, oldUiForecastTablesByStationId);
            if (closestMatch is { Score: > 0, Count: 1 })
            {
                await _forecastRequestInfosSemaphore.WaitAsync();
                try
                {
                    var mappedStationId = closestMatch.MappedStationId!.Value;
                    requestInfo = requestInfo with { MappedStationId = mappedStationId };
                    _forecastRequestInfos[stationId] = requestInfo;

                    Dictionary<int, int> stationMappings;
                    var newMappingCount = 1;
                    var path = GetStationMappingJsonLocation(requestInfo.CityCode);
                    if (File.Exists(path))
                    {
                        await using var jsonStream = File.OpenRead(path);
                        stationMappings = (await JsonSerializer.DeserializeAsync<Dictionary<int, int>>(jsonStream, _jsonSerializerOptions))!;
                    }
                    else
                        stationMappings = [];

                    stationMappings.Add(stationId, mappedStationId);

                    // Fill remaining one mapping in station group, if any.
                    var mappingsForGroup = oldUiForecastTablesByStationId.Keys
                        .Select(otherStationId => (
                            stationId: otherStationId,
                            requestInfo: _forecastRequestInfos.TryGetValue(otherStationId, out var otherRequestInfo)
                                ? otherRequestInfo
                                : default(ForecastRequestInfo?)))
                        .Where(p => p.requestInfo != null)
                        .ToDictionary(p => p.stationId, p => p.requestInfo!.Value);
                    var missingMappingsForGroup = mappingsForGroup.Where(kv => kv.Value.MappedStationId == null).ToArray();
                    if (missingMappingsForGroup.Length == 1)
                    {
                        var (otherStationId, otherRequestInfo) = missingMappingsForGroup.Single();
                        var remainingMappedStationIds = mappingsForGroup.Keys.Except(mappingsForGroup.Values
                                .Where(fri => fri.MappedStationId != null).Select(fri => fri.MappedStationId!.Value))
                            .ToArray();
                        if (remainingMappedStationIds.Length == 1)
                        {
                            var remainingMappedStationId = remainingMappedStationIds.Single();
                            otherRequestInfo = otherRequestInfo with { MappedStationId = remainingMappedStationId };
                            _forecastRequestInfos[otherStationId] = otherRequestInfo;
                            stationMappings.Add(otherStationId, remainingMappedStationId);
                            newMappingCount++;
                        }
                    }

                    await using (var jsonStream = File.OpenRead(path))
                        await JsonSerializer.SerializeAsync(jsonStream, stationMappings, _jsonSerializerOptions);

                    _logger.LogDebug("Found {Count} new mappings.", newMappingCount);
                }
                finally
                {
                    _forecastRequestInfosSemaphore.Release();
                }

            }
            else if (!closestMatch.HasForecasts)
                return [];
            else
            {
                _logger.LogWarning("No mapping found for station {StationId}.", stationId);

                return null;
            }
        }

        var forecastContainer = GetForecastContainer(stationGroupDocument, stationId);
        var mappedForecastContainer = GetForecastContainer(stationGroupDocument, requestInfo.MappedStationId.Value);
        var stationName = stationGroupDocument.QuerySelector("h1")?.FindChild<IText>()?.TextContent.Trim() ?? "";
        var direction = forecastContainer?.QuerySelector("h3")?.TextContent.Trim() ?? "";
        var forecastRows = mappedForecastContainer?.QuerySelectorAll<IHtmlTableRowElement>("table tbody tr") ?? [];
        var forecasts = new List<Forecast>();
        var stationsByRoute = new Dictionary<string, List<StationVehicleInfo>>();
        foreach (var forecastRow in forecastRows)
        {
            var time = TimeOnly.Parse(forecastRow.Cells[0].TextContent);
            var dateTime = time >= TimeOnly.FromDateTime(DateTime.Now).AddHours(-1)
                ? DateTime.Today + time.ToTimeSpan()
                : DateTime.Today + time.ToTimeSpan() + TimeSpan.FromDays(1);
            var routeNumberAnchors = forecastRow.Cells[1].QuerySelectorAll<IHtmlAnchorElement>("a");
            foreach (var routeNumberAnchor in routeNumberAnchors)
            {
                var routeCode = ExtractCode(routeNumberAnchor.Href);
                var match = Regex.Match(routeCode, @"([\w-]+-)?(?<type>bus(?:-taxi)?(?:-intercity)?|trolleybus|tramway|metro)-(?<route>[\w-]+)");
                var type = TransportType.Unknown;
                if (match.Success)
                {
                    type = match.Groups["type"].Value switch
                    {
                        "bus" => TransportType.Bus,
                        "trolleybus" => TransportType.Trolleybus,
                        "tramway" => TransportType.Tram,
                        "metro" => TransportType.Metro,
                        "bus-taxi" => TransportType.TaxiBus,
                        "bus-intercity" => TransportType.IntercityBus,
                        _ => TransportType.Unknown
                    };
                }
                var routeNumber = routeNumberAnchor.TextContent.Trim();
                routeNumber = type switch
                {
                    TransportType.Trolleybus when routeNumber.StartsWith("Т") => routeNumber[1..],
                    TransportType.Tram when routeNumber.StartsWith("ТВ") => routeNumber[2..],
                    TransportType.TaxiBus when routeNumber.StartsWith("МТ") => routeNumber[2..],
                    TransportType.IntercityBus when routeNumber.StartsWith("МА") => routeNumber[2..],
                    _ => routeNumber
                };

                if (!stationsByRoute.TryGetValue(routeCode, out var stations))
                {
                    _logger.LogDebug("Getting locations for route '{RouteCode}'...", routeCode);

                    stations = [];
                    var routeDocument = await _browsingContext.OpenAsync($"{BaseAddress}{requestInfo.CityCode}/{routeCode}/");
                    var locationTable = routeDocument.QuerySelectorAll<IHtmlTableElement>(".two table").FirstOrDefault(table => 
                        GetStationRowIndex(table, null, direction) - GetStationRowIndex(table, requestInfo.StationCode, stationName) == 1);
                    if (locationTable != null)
                    {
                        foreach (var row in locationTable.Rows)
                        {
                            var rowStationName = GetStationName(row);
                            var rowStationAnchor = row.QuerySelector<IHtmlAnchorElement>("a");
                            var rowStationCode = rowStationAnchor == null ? null : ExtractCode(rowStationAnchor.Href);
                            var hasVehicle = row.QuerySelector("img") != null;
                            stations.Add(new StationVehicleInfo(rowStationName, rowStationCode, hasVehicle));
                        }
                        stationsByRoute.Add(routeCode, stations);

                        _logger.LogDebug("Locations for route '{RouteCode}' loaded with {Count} entries.", routeCode, stations.Count);
                    }
                    else
                        _logger.LogWarning("Locations for route '{RouteCode}' could not be determined.", routeCode);
                }

                string? lastStation = null, nextStation = null;
                var currentStationIndex = stations.FindIndex(
                    station => station.StationCode == requestInfo.StationCode || station.StationName == stationName);
                for (var i = currentStationIndex; i >= 0; i--)
                {
                    var station = stations[i];
                    if (station.HasVehicle)
                    {
                        lastStation = station.StationName;
                        if (i < stations.Count - 1)
                            nextStation = stations[i + 1].StationName;
                        break;
                    }
                }

                forecasts.Add(new Forecast(dateTime, routeNumber, type, lastStation, nextStation));
            }
        }

        _logger.LogInformation("Forecast for station '{StationId}' completed with {Count} entries.", stationId, forecasts.Count);

        return forecasts;


        static IElement? GetForecastContainer(IParentNode root, int stationId) =>
            root.QuerySelector($"a[href$='/{stationId}/']")?.Ancestors<IHtmlDivElement>()
                .First(ancestor => ancestor.ClassList.Contains(["column", "wide"]));

        static int GetStationRowIndex(IHtmlTableElement table, string? stationCode, string stationName)
        {
            foreach (var row in table.Rows)
            {
                if (stationCode != null)
                {
                    var stationAnchor = row.QuerySelector<IHtmlAnchorElement>("a");
                    if (stationAnchor != null && ExtractCode(stationAnchor.Href) == stationCode)
                        return row.Index;
                }

                if (GetStationName(row) == stationName)
                    return row.Index;
            }
            return -1;
        }

        static string GetStationName(IHtmlTableRowElement row) => row.QuerySelector("a, b")?.TextContent.Trim() ?? "";
    }


    private readonly record struct StationGroup(string Code, string Name);

    private record ForecastTableMatch(IHtmlTableElement Table)
    {
        public readonly Dictionary<int, int> Scores = new();
        public int? StationId;
    }

    private readonly record struct ForecastCompareInfo(TimeOnly Time, string Route);

    private readonly record struct StationMatchInfo(int? MappedStationId, int Score, int Count, bool HasForecasts);

    private readonly record struct ForecastRequestInfo(string CityCode, string StationCode, int? MappedStationId);

    private readonly record struct StationVehicleInfo(string StationName, string? StationCode, bool HasVehicle);
}