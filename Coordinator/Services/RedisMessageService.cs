using StackExchange.Redis;

namespace Coordinator.Services;

public class RedisMessageService
{
    const string REDUCER_TASK_KEY = "reducer:task";
    const string MAPPER_ID_KEY = "mapper:id";
    const string REDUCER_ID_KEY = "reducer:id";
    const string MAPPERS_SET_KEY = "mappers";
    const string REDUCERS_SET_KEY = "reducers";
    readonly ConnectionMultiplexer _redis;
    readonly IDatabase _db;
    readonly ISubscriber _sub;

    public RedisMessageService()
    {
        _redis = ConnectionMultiplexer.Connect("localhost");
        _db = _redis.GetDatabase();
        _sub = _redis.GetSubscriber();
        CleanWokers();
    }

    public long GetMappersCount()
        => _db.SetLength(MAPPERS_SET_KEY);

    public long GetReducersCount()
        => _db.SetLength(REDUCERS_SET_KEY);

    public Task QueueTask(RedisKey queue, RedisValue task)
    {
        _db.ListLeftPush(queue, task);
        return GetTaskCompletion(queue, task);
    }

    private Task GetTaskCompletion(RedisKey queue, RedisValue task)
    {
        var completionSource = new TaskCompletionSource<string>();
        RedisChannel channelPattern = new RedisChannel("task_done", RedisChannel.PatternMode.Pattern);

        Action<RedisChannel, RedisValue>? handler = null;
        handler = (channel, message) =>
        {
            if (message == task)
            {
                Console.WriteLine($"{Path.GetFullPath(message.ToString())} processed.");
                completionSource.SetResult(message.ToString());
            }
            if(_db.ListLength(queue) == 0)
                _sub.Unsubscribe(channelPattern, handler);
        };

        _sub.Subscribe(channelPattern, handler);

        return completionSource.Task;
    }

    private void CleanWokers()
    {
        _db.StringSet(MAPPER_ID_KEY, -1);
        _db.StringSet(REDUCER_ID_KEY, -1);
        _db.KeyDelete(MAPPERS_SET_KEY);
        _db.KeyDelete(REDUCERS_SET_KEY);
    }
}
