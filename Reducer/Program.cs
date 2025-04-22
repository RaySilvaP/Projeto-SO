using Reducer.Services;
using StackExchange.Redis;
using System.Text.Json;

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
                Reduce(task.ToString());
                Console.WriteLine(task);
                _messageService.PublishCompletedTask(task);
            }
            Thread.Sleep(500);
        }
    }

    static void Reduce(string filePath)
    {
        if (!Directory.Exists(_tmpPath))
            Directory.CreateDirectory(_tmpPath);


        var dictionary = new Dictionary<string, int>();
        var outputPath = Path.Combine(_tmpPath, $"reducer-{_messageService.ReducerId}-output.json");
        using StreamReader reader = File.OpenText(Path.Combine(_tmpPath, filePath));
        using StreamWriter writer = new StreamWriter(outputPath, true);

        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            var keyValue = JsonSerializer.Deserialize<KeyValuePair<string, List<int>>>(line);
            var keySum = new KeyValuePair<string, int>(keyValue.Key, keyValue.Value.Sum());
            writer.WriteLine(keySum.ToString());
        }
        Console.WriteLine(outputPath);
    }
}
