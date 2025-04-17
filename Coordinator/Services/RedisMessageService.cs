using StackExchange.Redis;

namespace Coordinator.Services;

public class RedisMessageService
{
    readonly ConnectionMultiplexer redis;
    readonly IDatabase db;
    readonly ISubscriber sub;

    public RedisMessageService()
    {
        redis = ConnectionMultiplexer.Connect("localhost");
        db = redis.GetDatabase();
        sub = redis.GetSubscriber();
    }


    public Task QueueTask(RedisKey queue, RedisValue task)
    {
        db.ListLeftPush(queue, task);
        return GetTaskCompletion(queue, task);
    }

    private Task GetTaskCompletion(RedisKey queue, RedisValue task)
    {
        var completionSource = new TaskCompletionSource();
        RedisChannel channelPattern = new RedisChannel("task_done", RedisChannel.PatternMode.Pattern);

        Action<RedisChannel, RedisValue>? handler = null;
        handler = (channel, message) =>
        {
            if (message == task)
            {
                Console.WriteLine("Task completed:" + message);
                completionSource.TrySetResult();
            }

            if(db.ListLength(queue) == 0)
                sub.Unsubscribe(channelPattern, handler);
        };

        sub.Subscribe(channelPattern, handler);

        return completionSource.Task;
    }
}
