namespace PocWorker.Models;

public class S3EventNotification
{
    public List<S3EventRecord> Records { get; set; } = [];
}

public class S3EventRecord
{
    public string EventName { get; set; } = "";
    public S3BucketInfo S3 { get; set; } = new();
}

public class S3BucketInfo
{
    public S3BucketDetail Bucket { get; set; } = new();
    public S3ObjectDetail Object { get; set; } = new();
}

public class S3BucketDetail
{
    public string Name { get; set; } = "";
}

public class S3ObjectDetail
{
    public string Key { get; set; } = "";
}
