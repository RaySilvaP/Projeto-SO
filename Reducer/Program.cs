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
        if(File.Exists(outputPath))
            File.Delete(outputPath);

        using StreamReader reader = File.OpenText(inputPath);
        using StreamWriter writer = new StreamWriter(outputPath, true);

        Console.WriteLine($"Writing to {outputPath}...");
        string? line;
        while((line = reader.ReadLine()) != null)
        {
            var keyValue = JsonSerializer.Deserialize<KeyValuePair<string, List<int>>>(line);
            var keySum = new KeyValuePair<string, int>(keyValue.Key, keyValue.Value.Sum());
            writer.WriteLine(keySum.ToString());

        }
        File.Delete(inputPath);
    }

    static void WriteOuputFile(StreamWriter writer, FileStream stream)
    {
        byte[] buffer = new byte[8192]; // 8 KB buffer
        int bytesRead;
        bool isFinalBlock = false;

        var state = new JsonReaderState();

        while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
        {
            isFinalBlock = bytesRead < buffer.Length;

            var reader = new Utf8JsonReader(buffer.AsSpan(0, bytesRead), isFinalBlock, state);

            while (reader.Read())
            {
                // Example: detecting dictionary keys and values
                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    var key = reader.GetString() ?? string.Empty;
                    reader.Read(); // Move to array

                    if (reader.TokenType == JsonTokenType.StartArray)
                    {
                        var values = new List<int>();
                        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        {
                            values.Add(reader.GetInt32());
                        }
                        var keySum = new KeyValuePair<string, int>(key, values.Sum());
                        writer.WriteLine(keySum.ToString());
                    }
                }
            }
            state = reader.CurrentState;
        }
    }
}
