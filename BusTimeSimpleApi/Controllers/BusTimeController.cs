using BusTimeSimpleApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace BusTimeSimpleApi.Controllers;

[ApiController]
[Produces(ContentType)]
public class BusTimeController(BusTimeClient busTimeClient) : ControllerBase
{
    private const string ContentType = "application/json";
    
    [HttpGet("cities")]
    [ProducesResponseType<IEnumerable<City>>(StatusCodes.Status200OK)]
    [ResponseCache(CacheProfileName = "Static")]
    public async Task<IActionResult> GetCities()
    {
        if (!System.IO.File.Exists(busTimeClient.CitiesJsonLocation))
            await busTimeClient.UpdateCitiesAsync();

        return PhysicalFile(busTimeClient.CitiesJsonLocation, ContentType);
    }

    [HttpGet("cities/{cityCode}/stations")]
    [ProducesResponseType<IEnumerable<Station>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ResponseCache(CacheProfileName = "Static")]
    public async Task<IActionResult> GetStations(string cityCode)
    {
        if (!await busTimeClient.CityExistsAsync(cityCode))
            return NotFound();

        var stationJsonLocation = busTimeClient.GetStationJsonLocation(cityCode);
        if (!System.IO.File.Exists(stationJsonLocation))
            await busTimeClient.UpdateStationsAsync(cityCode);

        return PhysicalFile(stationJsonLocation, ContentType);
    }

    [HttpGet("stations/{stationId}/forecast")]
    [ProducesResponseType<IEnumerable<Forecast>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ResponseCache(CacheProfileName = "Dynamic")]
    public async Task<IActionResult> GetForecast(int stationId)
    {
        var forecasts = await busTimeClient.GetForecastsAsync(stationId);
        return forecasts == null ? NotFound() : Ok(forecasts);
    }
}
