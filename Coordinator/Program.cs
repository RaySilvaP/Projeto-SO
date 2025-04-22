using System.Text;
using Coordinator.Services;

public class Program
{
    static readonly string _tmpPath = Environment.GetEnvironmentVariable("TMP_PATH") ?? "../tmp";
    static readonly string _outputPath = Environment.GetEnvironmentVariable("OUTPUT_PATH") ?? "../";
    const int CHUNKS_AMOUNT = 10;
    static RedisMessageService messageService = null!;
    static ShuffleService shuffleService = null!;

    public static void Main(string[] args)
    {
        messageService = new RedisMessageService();
        shuffleService = new ShuffleService(messageService);
        while (true)
        {
            try
            {
                Console.Write("Insert the full file path: ");
                var filePath = Console.ReadLine() ?? "";
                ProcessFile(filePath);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    static async void ProcessFile(string filePath)
    {
        using StreamReader reader = File.OpenText(filePath);
        var words = GetWordsAsync(reader).GetAsyncEnumerator();
        if (!await words.MoveNextAsync())
            throw new Exception("File has no lines.");

        int chunkLength = (int)Math.Ceiling(reader.BaseStream.Length / (double)CHUNKS_AMOUNT);

        var tasks = new Task[CHUNKS_AMOUNT];
        Console.WriteLine("Spliting file into chunks:");
        for (int i = 0; i < CHUNKS_AMOUNT; i++)
        {
            var outfilePath = Path.GetFullPath($"../tmp/chunk{i}.txt");
            await WriteWords(words, reader.CurrentEncoding, outfilePath, chunkLength);

            tasks[i] = messageService.QueueTask("map_queue", outfilePath);
            Console.WriteLine($"Chunk{i} created.");
        };

        Console.WriteLine("Waiting for map workers...");
        Task.WaitAll(tasks);
        Console.WriteLine("Map completed.");

        Console.WriteLine("Suffling files:");
        var reducerTasks = shuffleService.ShuffleFiles();

        Console.WriteLine("Waiting for reduce workers...");
        Task.WaitAll(reducerTasks);
        Console.WriteLine("Reduce completed.");

        Console.WriteLine("Merging files.");
        var outputPath = MergeFiles();
        Console.WriteLine($"Result: {Path.GetFullPath(outputPath)}");
    }

    static string MergeFiles()
    {
        var reduceOutputs = Directory.GetFiles(_tmpPath, "reducer-*-output.json");
        var dateTime = DateTime.Now;
        var fileName = $"{dateTime.Day}-{dateTime.Month}-{dateTime.Year}_{dateTime.Hour}-{dateTime.Minute}-{dateTime.Second}";
        var outputPath = Path.Combine(_outputPath, $"{fileName}.txt");
        using StreamWriter writer = new StreamWriter(outputPath, true);
        foreach (var output in reduceOutputs)
        {
            using StreamReader reader = File.OpenText(output);
            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                writer.WriteLine(line);
            }
        }
        return outputPath;
    }

    static async Task WriteWords(IAsyncEnumerator<string> words, Encoding encoding, string outputPath, int maxFileSize)
    {
        Console.WriteLine("buffer");
        var buffer = new List<byte>();
        do
        {
            buffer.AddRange(encoding.GetBytes(words.Current + " "));
        }
        while (await words.MoveNextAsync() && maxFileSize - buffer.Count >= encoding.GetByteCount(words.Current));

        Console.WriteLine("Writing");
        await File.WriteAllBytesAsync(outputPath, buffer.ToArray());
    }

    static async IAsyncEnumerable<string> GetWordsAsync(StreamReader reader)
    {
        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            var words = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            foreach (var word in words)
            {
                yield return word;
            }
        }
    }
}
