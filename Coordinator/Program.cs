using System.Text;
using Coordinator.Services;

public class Program
{
    static readonly string _basePath = Environment.GetEnvironmentVariable("BASE_PATH") ?? "../";
    static readonly string _tmpPath = Environment.GetEnvironmentVariable("TMP_PATH") ?? "../tmp";
    static readonly string _outputPath = Environment.GetEnvironmentVariable("OUTPUT_PATH") ?? "../";
    const int CHUNKS_AMOUNT = 10;
    const int LINE_SIZE = 1024;
    static RedisMessageService messageService = null!;
    static ShuffleService shuffleService = null!;

    public static async Task Main(string[] args)
    {
        messageService = new RedisMessageService();
        shuffleService = new ShuffleService(messageService);

        while (true)
        {
            Console.Write("Insert the file name: ");
            var fileName = Console.ReadLine() ?? "";
            var filePath = Path.Combine(_basePath, fileName);
            if (File.Exists(filePath))
            {
                await SplitFile(filePath);

                Console.WriteLine("Suffling files...");
                var reducerTasks = shuffleService.ShuffleFiles();

                Console.WriteLine("Waiting for reduce workers...");
                Task.WaitAll(reducerTasks);
                Console.WriteLine("Reduce completed.");

                Console.WriteLine("Merging files.");
                var outputPath = MergeFiles();
                Console.WriteLine($"Result: {Path.GetFullPath(outputPath)}");
            }
            else
                Console.WriteLine($"File \"{filePath}\" not found.");
        }
    }

    static async Task SplitFile(string filePath)
    {
        if (!Directory.Exists(_tmpPath))
            Directory.CreateDirectory(_tmpPath);

        using StreamReader reader = File.OpenText(filePath);
        await using var words = GetWordsAsync(reader).GetAsyncEnumerator();
        if (!await words.MoveNextAsync())
            throw new Exception("File has no lines.");

        int chunkLength = (int)Math.Ceiling(reader.BaseStream.Length / (double)CHUNKS_AMOUNT);
        var tasks = new Task[CHUNKS_AMOUNT];

        Console.WriteLine("Spliting file into chunks...");
        for (int i = 0; i < CHUNKS_AMOUNT; i++)
        {
            var outfilePath = Path.Combine(_tmpPath, $"chunk{i}.txt");
            await WriteWords(words, reader.CurrentEncoding, outfilePath, chunkLength);

            tasks[i] = messageService.QueueTask("map_queue", outfilePath);
            Console.WriteLine($"Chunk{i} created.");
        };
        Console.WriteLine("Waiting for map workers...");
        Task.WaitAll(tasks);
        Console.WriteLine("Map completed.");
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
            File.Delete(output);
        }
        return outputPath;
    }

    static async Task WriteWords(IAsyncEnumerator<string> words, Encoding encoding, string outputPath, int maxFileSize)
    {
        using var writer = new FileStream(outputPath, FileMode.Create, FileAccess.Write);
        long size = 0;
        int lineCount = 1;
        do
        {
            byte[] wordsBytes;
            if(size / lineCount < LINE_SIZE)
                wordsBytes = encoding.GetBytes(words.Current + ' ');
            else 
            {
                wordsBytes = encoding.GetBytes(words.Current + ' ' + Environment.NewLine);
                lineCount++;
            }

            size += wordsBytes.Length;

            if (size < maxFileSize)
                await writer.WriteAsync(wordsBytes);
            else
                break;
        }
        while (await words.MoveNextAsync());
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
