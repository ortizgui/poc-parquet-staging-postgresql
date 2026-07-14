using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.SQS;
using PocWorker.Services;

var builder = Host.CreateApplicationBuilder(args);

var awsConfig = builder.Configuration.GetSection("AWS");
var serviceUrl = awsConfig["ServiceURL"] ?? "http://localhost:4566";
var region = awsConfig["Region"] ?? "us-east-1";
var s3Config = new AmazonS3Config
{
    ServiceURL = serviceUrl,
    AuthenticationRegion = region,
    ForcePathStyle = true
};

var sqsConfig = new AmazonSQSConfig
{
    ServiceURL = serviceUrl,
    AuthenticationRegion = region
};

builder.Services.AddSingleton<IAmazonS3>(_ => new AmazonS3Client(new BasicAWSCredentials("test", "test"), s3Config));
builder.Services.AddSingleton<IAmazonSQS>(_ => new AmazonSQSClient(new BasicAWSCredentials("test", "test"), sqsConfig));

builder.Services.AddSingleton<DatabaseService>();
builder.Services.AddSingleton<ParquetProcessor>();
builder.Services.AddHostedService<SqsConsumerService>();

var host = builder.Build();
host.Run();
