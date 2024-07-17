using System.Threading.RateLimiting;

namespace BusTimeSimpleApi;

public class ClientSideRateLimitedHandler(RateLimiter limiter) : DelegatingHandler(new HttpClientHandler()), IAsyncDisposable
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        using var lease = await limiter.AcquireAsync(cancellationToken: cancellationToken);

        return await base.SendAsync(request, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await limiter.DisposeAsync().ConfigureAwait(false);

        Dispose(false);
        GC.SuppressFinalize(this);
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        if (disposing)
            limiter.Dispose();
    }
}