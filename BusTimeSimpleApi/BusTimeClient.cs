﻿using System.Diagnostics;
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

    private readonly IBrowsingContext _browsingContext;
    private readonly JsonSerializerOptions _jsonSerializerOptions;
    private readonly ILogger<BusTimeClient> _logger;
    private readonly HashSet<string> _existingCities = new();
    private readonly SemaphoreSlim _existingCitiesSemaphore = new(1, 1);
    private readonly Dictionary<int, ForecastRequestInfo> _forecastRequestInfos = new();
    private readonly SemaphoreSlim _forecastRequestInfosSemaphore = new(1, 1);

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

        _logger.LogInformation("City list updated with {Count} entries.", cities.Length);
    }

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

    public async Task UpdateStationsAsync(bool fullUpdate = false)
    {
        _logger.LogInformation("Updating station list ({UpdateType} update)...", fullUpdate ? "full" : "delta");

        var cities = await ReadCitiesAsync();

        foreach (var city in cities)
            await UpdateStationsAsync(city.Code, fullUpdate);

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
            Name: item.TextContent.Trim())).ToArray();

        ILookup<string, Station>? existingStationsByCode = null;
        if (!fullUpdate)
        {
            var existingStations = await ReadStationsAsync(cityCode);
            existingStationsByCode = existingStations?.ToLookup(station => station.Code);
        }

        var allStations = new List<Station>();
        var stationGroupsProcessedCount = 0;
        const int reportMilestone = 20;
        foreach (var stationGroup in stationGroups)
        {
            var requiresUpdate = fullUpdate || !(existingStationsByCode?.Contains(stationGroup.Code) ?? false);
            if (requiresUpdate)
            {
                _logger.LogDebug("Updating stations for group '{StationCode}'...", stationGroup.Code);

                var stationGroupDocument = await _browsingContext.OpenAsync($"{BaseAddress}{cityCode}/stop/{stationGroup.Code}/");
                var stationArrows = stationGroupDocument.QuerySelectorAll("h3 .fa-arrow-right");
                var stationRoutes = stationGroupDocument.QuerySelectorAll("h3").First(h3 => h3.TextContent.Contains("Маршруты"))
                    .ParentElement?.QuerySelectorAll<IHtmlAnchorElement>("a") ?? [];
                var stationType = stationRoutes.All(route => route.Href.Contains("tramway")) ? StationType.Tram : StationType.Regular;
                var addedStationCount = 0;
                foreach (var arrow in stationArrows)
                {
                    var column = arrow.Ancestors<IHtmlDivElement>().First(ancestor => ancestor.ClassList.Contains("column"));
                    var station = new Station(
                        Id: int.Parse(ExtractCode((column.QuerySelector(".fa-desktop")?.ParentElement as IHtmlAnchorElement)?.Href ?? "0")),
                        Code: stationGroup.Code,
                        Name: stationGroup.Name,
                        Direction: arrow.ParentElement!.TextContent,
                        Type: stationType);
                    allStations.Add(station);
                    addedStationCount++;
                }

                _logger.LogDebug("Stations for group '{StationCode}' updated with {Count} entries.", stationGroup.Code, addedStationCount);
            }
            else
            {
                Debug.Assert(existingStationsByCode != null, nameof(existingStationsByCode) + " != null");
                allStations.AddRange(existingStationsByCode[stationGroup.Code]);
            }

            stationGroupsProcessedCount++;
            if (stationGroupsProcessedCount % reportMilestone == 0) 
                _logger.LogInformation("Station list update for '{CityCode}' done for {Count}/{TotalCount} station groups.", cityCode, stationGroupsProcessedCount, stationGroups.Length);
            
        }

        // Waits have to be done outside this method as sepaphores are not reentrant.
        await _forecastRequestInfosSemaphore.WaitAsync();
        try
        {
            UpdateForecastRequestInfos(cityCode, allStations, true);
        }
        finally
        {
            _forecastRequestInfosSemaphore.Release();
        }

        var path = GetStationJsonLocation(cityCode);
        EnsureDirectory(path);
        await using var jsonStream = OpenWrite(path);
        await JsonSerializer.SerializeAsync(jsonStream, allStations, _jsonSerializerOptions);

        _logger.LogInformation("Station list for '{CityCode}' updated with {Count} entries.", cityCode, allStations.Count);
    }

    private async Task<IReadOnlyCollection<Station>?> ReadStationsAsync(string cityCode)
    {
        var stationsJsonLocation = GetStationJsonLocation(cityCode);
        if (!File.Exists(stationsJsonLocation))
            return null;

        await using var jsonStream = File.OpenRead(stationsJsonLocation);
        return (await JsonSerializer.DeserializeAsync<IReadOnlyCollection<Station>>(jsonStream, _jsonSerializerOptions))!;
    }

    private async Task UpdateForecastRequestInfosAsync()
    {
        var stationJsonFiles = Directory.EnumerateFiles(StationsDirectoryPath, "*.json");
        foreach (var stationJsonFile in stationJsonFiles)
        {
            await using var jsonStream = File.OpenRead(stationJsonFile);
            var stations = (await JsonSerializer.DeserializeAsync<IEnumerable<Station>>(jsonStream, _jsonSerializerOptions))!;

            UpdateForecastRequestInfos(Path.GetFileNameWithoutExtension(stationJsonFile), stations, false);
        }
    }

    private void UpdateForecastRequestInfos(string cityCode, IEnumerable<Station> stations, bool removeOld)
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
            if (!_forecastRequestInfos.TryAdd(station.Id, new ForecastRequestInfo(cityCode, station.Code)))
                _logger.LogWarning("Duplicate station ID {StationId}", station.Id);
    }

    public async Task<IEnumerable<Forecast>?> GetForecastsAsync(int stationId)
    {
        _logger.LogInformation("Getting forecast for station '{StationId}'...", stationId);

        IDocument document;

        await _forecastRequestInfosSemaphore.WaitAsync();
        try
        {
            if (_forecastRequestInfos.Count == 0)
                await UpdateForecastRequestInfosAsync();

            if (!_forecastRequestInfos.TryGetValue(stationId, out var requestInfo))
                return null;

            document = await _browsingContext.OpenAsync($"{BaseAddress}{requestInfo.CityCode}/stop/{requestInfo.StationCode}/");
        }
        finally
        {
            _forecastRequestInfosSemaphore.Release();
        }

        var rows = document.QuerySelector($"a[href$='/{stationId}/']")?.Ancestors<IHtmlDivElement>()
            .First(ancestor => ancestor.ClassList.Contains(["column", "wide"]))
            .QuerySelectorAll<IHtmlTableRowElement>("table tbody tr") ?? [];
        var forecasts = new List<Forecast>();
        foreach (var row in rows)
        {
            var time = TimeOnly.Parse(row.Cells[0].TextContent);
            var dateTime = time >= TimeOnly.FromDateTime(DateTime.Now).AddHours(-1)
                ? DateTime.Today + time.ToTimeSpan()
                : DateTime.Today + time.ToTimeSpan() + TimeSpan.FromDays(1);
            var routeNumberAnchors = row.Cells[1].QuerySelectorAll<IHtmlAnchorElement>("a");
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
                forecasts.Add(new Forecast(dateTime, routeNumber, type));
            }
        }

        _logger.LogInformation("Forecast for station '{StationId}' completed with {Count} entries.", stationId, forecasts.Count);

        return forecasts;
    }


    private readonly record struct StationGroup(string Code, string Name);

    private readonly record struct ForecastRequestInfo(string CityCode, string StationCode);
}