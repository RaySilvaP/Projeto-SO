using System.Text.Json;

namespace Coordinator.Services;

public class ShuffleService
{
    readonly string _tmpPath = Environment.GetEnvironmentVariable("TMP_PATH") ?? "../tmp";
    readonly RedisMessageService _messageService;
    readonly JsonSerializerOptions _options = new JsonSerializerOptions { WriteIndented = true };

    public ShuffleService(RedisMessageService messageService)
    {
        _messageService = messageService;
    }

    public Task[] ShuffleFiles()
    {
        string[] mapperFiles = Directory.GetFiles(_tmpPath, "mapper*");
        var reducersCount = _messageService.GetReducersCount();
        CleanFiles(reducersCount);
        foreach (var file in mapperFiles)
        {
            Console.WriteLine($"Shuffling {file}...");
            var reducers = new Dictionary<string, List<int>>[reducersCount];
            using StreamReader reader = File.OpenText(file);
            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                var keyValue = ToKeyValuePair(line);
                var index = Math.Abs(keyValue.Key.GetHashCode() % reducersCount);

                if (reducers[index] == null)
                    reducers[index] = new();

                if (reducers[index].ContainsKey(keyValue.Key))
                    reducers[index][keyValue.Key].Add(keyValue.Value);
                else
                    reducers[index][keyValue.Key] = [keyValue.Value];
            }
            File.Delete(file);
            if (!Directory.Exists(_tmpPath))
                Directory.CreateDirectory(_tmpPath);

            for (int i = 0; i < reducers.Length; i++)
            {
                WriteReducerFile(reducers[i], i);
            }
            Console.WriteLine($"{file} shuffled.");
        }
        var tasks = new Task[reducersCount];
        for (int i = 0; i < reducersCount; i++)
        {
            tasks[i] = _messageService.QueueTask($"reducer_{i}_queue", $"reducer-{i}-input.json");
        }
        return tasks;
    }

    void WriteReducerFile(Dictionary<string, List<int>> dictionary, int reducerId)
    {
        var outputPath = Path.Combine(_tmpPath, $"reducer-{reducerId}-input.json");
        if (File.Exists(outputPath))
        {
            using var readStream = File.OpenText(outputPath);
            string? line;
            while ((line = readStream.ReadLine()) != null)
            {
                var keyValue = JsonSerializer.Deserialize<KeyValuePair<string, List<int>>>(line);
                if (dictionary.ContainsKey(keyValue.Key))
                    dictionary[keyValue.Key].AddRange(keyValue.Value);
                else
                    dictionary[keyValue.Key] = keyValue.Value;

            }
            readStream.Close();
            File.Delete(outputPath);
        }

        using var writeStream = new StreamWriter(outputPath, true);
        Console.WriteLine($"Writing to {outputPath}...");
        foreach (var entry in dictionary)
        {
            writeStream.WriteLine(JsonSerializer.Serialize(entry));
        }

    }

    KeyValuePair<string, int> ToKeyValuePair(string text)
    {
        var trimmed = text.Trim('[', ']');
        var values = trimmed.Split(',', StringSplitOptions.RemoveEmptyEntries);
        string key = values[0];
        var value = int.Parse(values[1]);
        return new KeyValuePair<string, int>(key, value);
    }

    void CleanFiles(long reducersCount)
    {
        for(int i = 0; i < reducersCount; i++)
        {
            File.Delete(Path.Combine(_tmpPath, $"reducer-{i}-input.json"));
        }
    }
}
