namespace BusTimeSimpleApi;

public record BusTimeClientOptions(int MaxDesertedDays)
{
    public BusTimeClientOptions() : this(7) { }
}