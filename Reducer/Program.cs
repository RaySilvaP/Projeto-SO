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

        var inputPath = Path.Combine(_tmpPath, filePath);
        var outputPath = Path.Combine(_tmpPath, $"reducer-{_messageService.ReducerId}-output.json");

        using var stream = File.OpenRead(inputPath);
        using StreamWriter writer = new StreamWriter(outputPath, true);

        var dictionary = JsonSerializer.Deserialize<Dictionary<string, List<int>>>(stream);
        if(dictionary == null)
            throw new Exception("Empty input.");
        
        foreach(var entry in dictionary)
        {
            var keySum = new KeyValuePair<string, int>(entry.Key, entry.Value.Sum());
            writer.WriteLine(keySum.ToString());
        }
        Console.WriteLine(outputPath);
    }
}
