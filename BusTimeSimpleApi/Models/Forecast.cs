namespace BusTimeSimpleApi.Models;

public record Forecast(DateTime Time, string RouteNumber, TransportType Type, string? LastStation, string? NextStation);