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
        foreach (var file in mapperFiles)
        {
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
            Console.WriteLine("read");
            if (!Directory.Exists(_tmpPath))
                Directory.CreateDirectory(_tmpPath);

            for (int i = 0; i < reducers.Length; i++)
            {
                WriteReducerFile(reducers[i], i);
            }
            Console.WriteLine($"{file} processed.");
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
            using var readStream = File.OpenRead(outputPath);
            var data = JsonSerializer.Deserialize<Dictionary<string, List<int>>>(readStream);
            if (data != null)
            {
                foreach (var entry in dictionary)
                {
                    if (data.ContainsKey(entry.Key))
                        data[entry.Key].AddRange(entry.Value);
                    else
                        data[entry.Key] = entry.Value;
                }
                readStream.Close();
                using var writeStream = File.OpenWrite(outputPath);
                Console.WriteLine("Writing to reducer " + reducerId);
                JsonSerializer.Serialize(writeStream, data);
            }
        }
        else
        {
            using var stream = File.Create(outputPath);
            Console.WriteLine("Writing to reducer " + reducerId);
            JsonSerializer.Serialize(stream, dictionary);
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
}
