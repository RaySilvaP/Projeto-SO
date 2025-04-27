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

    public Task[] ShuffleFiles()
    {
        string[] mapperFiles = Directory.GetFiles(_tmpPath, "mapper*");
        var dictionary = new Dictionary<string, List<int>>();
        foreach (var file in mapperFiles)
        {
            using StreamReader reader = File.OpenText(file);
            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                var keyValue = ToKeyValuePair(line);
                if (dictionary.ContainsKey(keyValue.Key))
                    dictionary[keyValue.Key].Add(keyValue.Value);
                else
                    dictionary[keyValue.Key] = [keyValue.Value];
            }
            File.Delete(file);
        }
        return Shuffle(dictionary);
    }

    Task[] Shuffle(Dictionary<string, List<int>> dictionary)
    {
        if (!Directory.Exists(_tmpPath))
            Directory.CreateDirectory(_tmpPath);

        var reducersCount = _messageService.GetReducersCount();
        var reducers = new Dictionary<string, List<int>>[reducersCount];
        foreach (var item in dictionary)
        {
            var wordHash = item.Key.GetHashCode();
            var index = wordHash % reducersCount;
            if (reducers[index] == null)
                reducers[index] = new();

            reducers[index].Add(item.Key, item.Value);
        }

        var tasks = new Task[reducersCount];
        for (int i = 0; i < reducersCount; i++)
        {
            CreateReducerFile(reducers[i], i);
            tasks[i] = _messageService.QueueTask($"reducer_{i}_queue", $"reducer-{i}-input.json");
        }
        return tasks;
    }

    void CreateReducerFile(Dictionary<string, List<int>> dictionary, int reducerId)
    {
        var outputPath = Path.Combine(_tmpPath, $"reducer-{reducerId}-input.json");
        if(File.Exists(outputPath))
            File.Delete(outputPath);

        using StreamWriter writer = new StreamWriter(outputPath, true);
        foreach (var item in dictionary)
        {
            writer.WriteLine(JsonSerializer.Serialize(item));
        }
        Console.WriteLine($"{outputPath} created.");
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
