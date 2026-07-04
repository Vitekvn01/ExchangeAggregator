using ExchangeAggregator.Api;
using ExchangeAggregator.Api.Configuration;
using ExchangeAggregator.Api.Pipeline;
using ExchangeAggregator.Core.Interfaces;
using ExchangeAggregator.Core.Metrics;
using ExchangeAggregator.Infrastructure.Data;
using ExchangeAggregator.Infrastructure.Deduplication;
using ExchangeAggregator.Infrastructure.Normalization;
using ExchangeAggregator.Infrastructure.Parsers;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<AggregatorOptions>(builder.Configuration.GetSection("Aggregator"));

builder.Services.AddDbContextFactory<TickDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.AddSingleton<TickMetrics>();

builder.Services.AddSingleton<ITickParser, BinanceTickParser>();
builder.Services.AddSingleton<ITickParser, KrakenTickParser>();
builder.Services.AddSingleton<ITickParser, CoinbaseTickParser>();

builder.Services.AddSingleton<ITickNormalizer, TickNormalizer>();

builder.Services.AddSingleton<IDeduplicator>(_ => new Deduplicator(TimeSpan.FromMinutes(5)));

builder.Services.AddSingleton<ITickStore, TickStore>();

builder.Services.AddSingleton<TickProcessingPipeline>();

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();