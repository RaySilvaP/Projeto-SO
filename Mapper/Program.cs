using StackExchange.Redis;

public class Program
{
    static readonly string ID = Guid.NewGuid().ToString();
    static readonly string TMP_PATH = Environment.GetEnvironmentVariable("TMP_PATH") ?? "../tmp";

    public static void Main(string[] args)
    {
        ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        IDatabase db = redis.GetDatabase();
        ISubscriber pub = redis.GetSubscriber();

        while (true)
        {
            Process(db, pub);
            Thread.Sleep(500);
        }
    }

    static void Process(IDatabase db, ISubscriber pub)
    {
        RedisValue value;
        RedisChannel pattern = new RedisChannel("task_done", RedisChannel.PatternMode.Pattern);
        while ((value = db.ListRightPop("map_queue")).HasValue)
        {
            Map(value.ToString());
            pub.Publish(pattern, value.ToString());
        }
    }
    
    static void Map(string filePath)
    {
        if(!Directory.Exists(TMP_PATH))
            Directory.CreateDirectory(TMP_PATH);

        var outputPath = Path.Combine(TMP_PATH, $"mapper-{ID}.tmp");
        using StreamReader reader = File.OpenText(filePath);
        using StreamWriter writer = new StreamWriter(outputPath, true);
        var words = CountWords(reader);
        foreach(var word in words)
        {
            writer.WriteLine(word);
        }
        Console.WriteLine(outputPath);
    }

    static IEnumerable<string> CountWords(StreamReader reader)
    {
        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            var words = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            foreach (var word in words)
            {
                var keyValue = new KeyValuePair<string, int>(word, 1);
                yield return keyValue.ToString();
            }
        }
    }

}
