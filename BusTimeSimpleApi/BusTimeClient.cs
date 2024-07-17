﻿using System.Text.Json;
using System.Threading.RateLimiting;
using AngleSharp;
using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using BusTimeSimpleApi.Models;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.Extensions.Options;

namespace BusTimeSimpleApi;

public class BusTimeClient(RateLimiter rateLimiter, IOptions<JsonOptions> jsonOptions)
{
    private const string BaseAddress = "https://ru.busti.me/";

    private static readonly string CacheDirectoryPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "BusTime");
    private static readonly string CitiesJsonPath = Path.Combine(CacheDirectoryPath, "cities.json");
    private static readonly string StationsDirectoryPath = Path.Combine(CacheDirectoryPath, "stations");

    private readonly JsonSerializerOptions _jsonSerializerOptions = jsonOptions.Value.SerializerOptions;
    private readonly IBrowsingContext _browsingContext = BrowsingContext.New(Configuration.Default.WithDefaultLoader()
        .WithRequesters(new ClientSideRateLimitedHandler(rateLimiter)));
    private readonly HashSet<string> _existingCities = new();
    private readonly SemaphoreSlim _existingCitiesSemaphore = new(1, 1);
    private readonly Dictionary<int, ForecastRequestInfo> _forecastRequestInfos = new();
    private readonly SemaphoreSlim _forecastRequestInfosSemaphore = new(1, 1);

    public string CitiesJsonLocation => CitiesJsonPath;

    public async Task UpdateCitiesAsync()
    {
        var document = await _browsingContext.OpenAsync($"{BaseAddress}");
        var items = document.QuerySelectorAll<IHtmlAnchorElement>("#main_container .item");
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
        await using var jsonStream = File.OpenWrite(CitiesJsonPath);
        await JsonSerializer.SerializeAsync(jsonStream, cities, _jsonSerializerOptions);
    }

    private static string ExtractCode(string url) => url.Split('/', StringSplitOptions.RemoveEmptyEntries).Last();

    private static void EnsureDirectory(string path) => Directory.CreateDirectory(Path.GetDirectoryName(path)!);

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

    public async Task UpdateStationsAsync()
    {
        var cities = await ReadCitiesAsync();

        foreach (var city in cities)
            await UpdateStationsAsync(city.Code);
    }

    private async Task<IEnumerable<City>> ReadCitiesAsync()
    {
        if (!File.Exists(CitiesJsonPath))
            await UpdateCitiesAsync();

        await using var jsonStream = File.OpenRead(CitiesJsonPath);
        return (await JsonSerializer.DeserializeAsync<IEnumerable<City>>(jsonStream, _jsonSerializerOptions))!;
    }

    public async Task UpdateStationsAsync(string cityCode)
    {
        var stationListDocument = await _browsingContext.OpenAsync($"{BaseAddress}{cityCode}/stop/");
        var stationListItems = stationListDocument.QuerySelectorAll<IHtmlAnchorElement>("#main_container .item");
        var stationGroups = stationListItems.Select(item => new StationGroup
        {
            Code = ExtractCode(item.Href),
            Name = item.TextContent.Trim(),
        });

        var allStations = new List<Station>();
        foreach (var stationGroup in stationGroups)
        {
            var stationGroupDocument = await _browsingContext.OpenAsync($"{BaseAddress}{cityCode}/stop/{stationGroup.Code}/");
            var stationArrows = stationGroupDocument.QuerySelectorAll("h3 .fa-arrow-right");
            foreach (var arrow in stationArrows)
            {
                var column = arrow.Ancestors<IHtmlDivElement>().First(ancestor => ancestor.ClassList.Contains("column"));
                var station = new Station(
                    Id: int.Parse(ExtractCode((column.QuerySelector(".fa-desktop")?.ParentElement as IHtmlAnchorElement)?.Href ?? "0")),
                    Code: stationGroup.Code, 
                    Name: stationGroup.Name, 
                    Direction: arrow.ParentElement!.TextContent);
                allStations.Add(station);
            }
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
        await using var jsonStream = File.OpenWrite(path);
        await JsonSerializer.SerializeAsync(jsonStream, allStations, _jsonSerializerOptions);
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
                _forecastRequestInfos.Add(station.Id, new ForecastRequestInfo(cityCode, station.Code));
    }

    public async Task<IEnumerable<Forecast>?> GetForecastsAsync(int stationId)
    {
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
            var dateTime = time >= TimeOnly.FromDateTime(DateTime.Now)
                ? DateTime.Today + time.ToTimeSpan()
                : DateTime.Today + time.ToTimeSpan() + TimeSpan.FromDays(1);
            var routeNumbers = row.Cells[1].QuerySelectorAll<IHtmlAnchorElement>("a");
            foreach (var routeNumber in routeNumbers)
            {
                var parts = ExtractCode(routeNumber.Href).Split('-', 2);
                var type = parts[0] switch
                {
                    "bus" => TransportType.Bus,
                    "trolleybus" => TransportType.Trolleybus,
                    "tramway" => TransportType.Tram,
                    "metro" => TransportType.Metro, // TODO: confirm it works correctly
                    _ => TransportType.Unknown
                };
                forecasts.Add(new Forecast(dateTime, parts[1], type));
            }
        }
        return forecasts;
    }


    private readonly record struct StationGroup(string Code, string Name);

    private readonly record struct ForecastRequestInfo(string CityCode, string StationCode);
}