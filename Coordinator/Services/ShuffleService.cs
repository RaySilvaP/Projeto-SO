using System.Text.Json;

namespace Coordinator.Services;

public class ShuffleService
{
    readonly string _tmpPath = Environment.GetEnvironmentVariable("TMP_PATH") ?? "../tmp";
    readonly RedisMessageService _messageService;

    public ShuffleService(RedisMessageService messageService)
    {
        _messageService = messageService;
    }
            
    public void CreateReducerFiles()
    {
        string[] mapperFiles = Directory.GetFiles(_tmpPath, "mapper*");
        var dictionary = new Dictionary<string, List<int>>();
        foreach(var file in mapperFiles)
        {
            using StreamReader reader = File.OpenText(file);
            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                var keyValue = ToKeyValuePair(line);
                if(dictionary.ContainsKey(keyValue.Key))
                    dictionary[keyValue.Key].Add(keyValue.Value);
                else
                    dictionary[keyValue.Key] = [keyValue.Value];
            }
            File.Delete(file);
        }
        Shuffle(dictionary);
    }

    void Shuffle(Dictionary<string, List<int>> dictionary)
    {
        if(!Directory.Exists(_tmpPath))
            Directory.CreateDirectory(_tmpPath);

        var reducersCount = _messageService.GetReducersCount();
        foreach(var item in dictionary)
        {
            var index = item.Key.GetHashCode() % reducersCount;
            var outputPath = Path.Combine(_tmpPath, $"reducer-{index}-input.json");
            using StreamWriter writer = new StreamWriter(outputPath, true);
            writer.WriteLine(JsonSerializer.Serialize(item));
        }
        var tasks = new Task[reducersCount];
        for(int i = 0; i < reducersCount; i++)
        {
            tasks[i] = _messageService.QueueTask($"reducer_{i}_queue", $"reducer-{i}-input.json");
        }
        Task.WaitAll(tasks);
    }

    KeyValuePair<string, int> ToKeyValuePair(string text)
    {
        var trimmed = text.Trim('[', ']');
        var values = trimmed.Split(',', StringSplitOptions.RemoveEmptyEntries);
        string key = values[0];
        var value = int.Parse(values[1]);
        return new KeyValuePair<string, int>(key, value);
    }
}
