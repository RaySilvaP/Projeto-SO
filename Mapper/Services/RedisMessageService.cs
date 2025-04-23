using StackExchange.Redis;

namespace Mapper.Services;

public class RedisMessageService
{
    readonly string _redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost";
    const string MAPPER_ID_KEY = "mapper:id";
    const string MAPPER_SET_KEY = "mappers";
    readonly ConnectionMultiplexer _redis;
    readonly IDatabase _db;
    readonly ISubscriber _pub;
    public long MapperId { get; private set; }


    public RedisMessageService()
    {
        _redis = ConnectionMultiplexer.Connect(_redisConnection);
        _db = _redis.GetDatabase();
        _pub = _redis.GetSubscriber();
        MapperId = _db.StringIncrement(MAPPER_ID_KEY);
        _db.SetAdd(MAPPER_SET_KEY, MapperId);
    }

    public RedisValue PopTask()
    {
        return _db.ListRightPop("map_queue");
    }

    public void PublishCompletedTask(RedisValue task)
    {
        RedisChannel pattern = new RedisChannel("task_done", RedisChannel.PatternMode.Pattern);
        _pub.Publish(pattern, task);
    }
}
