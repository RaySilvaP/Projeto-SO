using System.Text.Json;

namespace Coordinator.Services;

public class ShuffleService
{
    readonly string TMP_PATH = Environment.GetEnvironmentVariable("TMP_PATH") ?? "../tmp";

    public void CreateReducerFiles()
    {
        string[] mapperFiles = Directory.GetFiles(TMP_PATH, "mapper*");
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
        }
        Shuffle(dictionary);
    }

    void Shuffle(Dictionary<string, List<int>> dictionary)
    {
        foreach(var item in dictionary)
        {
            Console.WriteLine(JsonSerializer.Serialize(item));
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
