using StackExchange.Redis;

namespace Reducer.Services;

public class RedisMessageService
{
    const string REDUCER_ID_KEY = "reducer:id";
    const string REDUCERS_SET_KEY = "reducers";
    readonly ConnectionMultiplexer _redis;
    readonly IDatabase _db;
    readonly ISubscriber _pub;
    public long Id { get; private set; }

    public RedisMessageService()
    {
        _redis = ConnectionMultiplexer.Connect("localhost");
        _db = _redis.GetDatabase();
        _pub = _redis.GetSubscriber();
        Id = _db.StringIncrement(REDUCER_ID_KEY);
        _db.SetAdd(REDUCERS_SET_KEY, REDUCER_ID_KEY);
    }

    public RedisValue PopTask()
    {
        return _db.ListLeftPop($"reducer_{Id}_queue");
    }

    public void PublishCompletedTask(RedisValue task)
    {
        RedisChannel pattern = new RedisChannel("task_done", RedisChannel.PatternMode.Pattern);
        _pub.Publish(pattern, task);
    }

}
