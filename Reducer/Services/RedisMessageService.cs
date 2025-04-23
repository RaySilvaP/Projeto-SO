using StackExchange.Redis;

namespace Reducer.Services;

public class RedisMessageService
{
    readonly string _redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost";
    const string REDUCER_ID_KEY = "reducer:id";
    const string REDUCERS_SET_KEY = "reducers";
    readonly ConnectionMultiplexer _redis;
    readonly IDatabase _db;
    readonly ISubscriber _pub;
    public long ReducerId { get; private set; }

    public RedisMessageService()
    {
        _redis = ConnectionMultiplexer.Connect(_redisConnection);
        _db = _redis.GetDatabase();
        _pub = _redis.GetSubscriber();
        ReducerId = _db.StringIncrement(REDUCER_ID_KEY);
        _db.SetAdd(REDUCERS_SET_KEY, ReducerId);
    }

    public RedisValue PopTask()
    {
        return _db.ListLeftPop($"reducer_{ReducerId}_queue");
    }

    public void PublishCompletedTask(RedisValue task)
    {
        RedisChannel pattern = new RedisChannel("task_done", RedisChannel.PatternMode.Pattern);
        _pub.Publish(pattern, task);
    }

}
