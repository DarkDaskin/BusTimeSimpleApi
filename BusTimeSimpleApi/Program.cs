using System.Security.Claims;
using System.Security.Principal;
using System.Text.Json;
using System.Threading.RateLimiting;
using BusTimeSimpleApi;
using Hangfire;
using Microsoft.AspNetCore.Authentication.Negotiate;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

// Mitigate https://github.com/advisories/GHSA-5crp-9r3c-p9vr
JsonConvert.DefaultSettings = () => new JsonSerializerSettings { MaxDepth = 128 };

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddSingleton<BusTimeClient>();

var rateLimitsConfiguration = builder.Configuration.GetSection("RateLimits");
builder.Services.AddSingleton<RateLimiter>(new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
{
    Window = TimeSpan.FromSeconds(double.TryParse(rateLimitsConfiguration["Window"], out var window) ? window : 1), 
    PermitLimit = int.TryParse(rateLimitsConfiguration["Limit"], out var limit) ? limit : 1,
    QueueLimit = int.MaxValue,
    QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
    AutoReplenishment = true,
}));

builder.Services.AddCors(o => o.AddDefaultPolicy(co => co.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));

builder.Services.AddResponseCaching();

var cacheConfiguration = builder.Configuration.GetSection("CacheProfiles");
builder.Services.AddControllers(o =>
{
    foreach (var cacheProfileConfiguration in cacheConfiguration.GetChildren())
    {
        o.CacheProfiles.Add(cacheProfileConfiguration.Key, new CacheProfile
        {
            Location = Enum.TryParse(cacheProfileConfiguration["Location"], out ResponseCacheLocation location) ? location : ResponseCacheLocation.Any,
            Duration = int.TryParse(cacheProfileConfiguration["Duration"], out var duration) ? duration : 0,
        });        
    }
}).AddJsonOptions(o => o.JsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);

builder.Services.AddAuthentication().AddNegotiate();

builder.Services.AddAuthorization(o => o.AddPolicy("Admin", ao =>
    ao.AddAuthenticationSchemes(NegotiateDefaults.AuthenticationScheme).RequireAuthenticatedUser()));

// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddHangfire(o => o.UseInMemoryStorage());
builder.Services.AddHangfireServer();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
}

app.UseSwagger();
app.UseSwaggerUI();

app.UseHttpsRedirection();

app.UseCors();

app.UseResponseCaching();

app.UseAuthorization();

app.MapControllers();
app.MapHangfireDashboard().RequireAuthorization("Admin");

app.MapGet("", context =>
{ 
    context.Response.Redirect("swagger/");
    return Task.CompletedTask;
});

var recurringJobManager = app.Services.GetRequiredService<IRecurringJobManager>();
var scheduleConfiguration = app.Configuration.GetSection("UpdateSchedule");
recurringJobManager.AddOrUpdate<BusTimeClient>("update-cities", client => client.UpdateCitiesAsync(), scheduleConfiguration["Cities"]);
recurringJobManager.AddOrUpdate<BusTimeClient>("update-stations-delta", client => client.UpdateStationsAsync(false), scheduleConfiguration["StationsDelta"]);
recurringJobManager.AddOrUpdate<BusTimeClient>("update-stations-full", client => client.UpdateStationsAsync(true), scheduleConfiguration["StationsFull"]);

app.Run();
