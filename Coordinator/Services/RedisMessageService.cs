using System.Collections.Concurrent;
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
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _pendingTasks = new();

    public RedisMessageService()
    {
        _redis = ConnectionMultiplexer.Connect("localhost");
        _db = _redis.GetDatabase();
        _sub = _redis.GetSubscriber();
        Subscribe();
        CleanWokers();
    }

    public long GetMappersCount()
        => _db.SetLength(MAPPERS_SET_KEY);

    public long GetReducersCount()
        => _db.SetLength(REDUCERS_SET_KEY);

    public Task QueueTask(RedisKey queue, RedisValue task)
    {
        var tsc = new TaskCompletionSource();
        _pendingTasks[task.ToString()] = tsc;
        _db.ListLeftPush(queue, task);
        return tsc.Task;
    }

    private void Subscribe()
    {
        RedisChannel channelPattern = new RedisChannel("task_done", RedisChannel.PatternMode.Pattern);

        _sub.Subscribe(channelPattern, (channel, message) =>
        {
            if (_pendingTasks.TryRemove(message.ToString(), out var taskSource))
            {
                Console.WriteLine($"{Path.GetFullPath(message.ToString())} processed.");
                taskSource.TrySetResult();
            }
        });
    }

    private void CleanWokers()
    {
        _db.StringSet(MAPPER_ID_KEY, -1);
        _db.StringSet(REDUCER_ID_KEY, -1);
        _db.KeyDelete(MAPPERS_SET_KEY);
        _db.KeyDelete(REDUCERS_SET_KEY);
    }
}
