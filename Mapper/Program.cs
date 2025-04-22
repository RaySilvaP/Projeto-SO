using StackExchange.Redis;
using Mapper.Services;

public class Program
{
    static readonly string _tmpPath = Environment.GetEnvironmentVariable("TMP_PATH") ?? "../tmp";
    static readonly RedisMessageService _messageService = new RedisMessageService();

    public static void Main(string[] args)
    {
        while (true)
        {
            RedisValue task;
            while (!(task = _messageService.PopTask()).IsNull)
            {
                Map(task.ToString());
                _messageService.PublishCompletedTask(task);
            }
            Thread.Sleep(500);
        }
    }

    static void Map(string filePath)
    {
        if(!Directory.Exists(_tmpPath))
            Directory.CreateDirectory(_tmpPath);

        var outputPath = Path.Combine(_tmpPath, $"mapper-{_messageService.MapperId}.tmp");
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
