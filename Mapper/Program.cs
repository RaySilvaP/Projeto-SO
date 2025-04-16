using StackExchange.Redis;

public class Program
{
    static readonly string ID = Guid.NewGuid().ToString();
    public static void Main(string[] args)
    {
        ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
        IDatabase db = redis.GetDatabase();
        while (true)
        {
            Process(db);
            Thread.Sleep(500);
        }
    }

    static void Process(IDatabase db)
    {
        RedisValue value;
        while ((value = db.ListRightPop("mapQueue")).HasValue)
        {
            Map(value.ToString());
        }
    }
    
    static void Map(string filePath)
    {
        using StreamReader reader = File.OpenText(filePath);
        var words = CountWords(reader);
        var outputPath = $"mapper-{ID}.tmp";
        Console.WriteLine(outputPath);
        if(File.Exists(outputPath))
            File.Delete(outputPath);

        File.WriteAllLines(outputPath, words);
    }

    static IEnumerable<string> CountWords(StreamReader reader)
    {
        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            var words = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            foreach (var word in words)
            {
                yield return $"({word}, 1)";
            }
        }
    }

}
