using System.Text;
using Coordinator.Services;

public class Program
{
    const int CHUNKS_AMOUNT = 10;
    static RedisMessageService messageService = new RedisMessageService();
    static ShuffleService shuffleService = new ShuffleService(messageService);

    public static void Main(string[] args)
    {

        if (File.Exists(args[0]))
        {
            using StreamReader reader = File.OpenText(args[0]);
            using var words = GetWords(reader).GetEnumerator();
            if (!words.MoveNext())
            {
                Console.WriteLine("Invalid text file.");
                return;
            }

            var task = new Task[CHUNKS_AMOUNT];
            int chunkLength = (int)Math.Ceiling(reader.BaseStream.Length / (double)CHUNKS_AMOUNT);
            for (int i = 0; i < CHUNKS_AMOUNT; i++)
            {
                var filePath = Path.GetFullPath($"./chunk{i}.txt");
                WriteWords(words, reader.CurrentEncoding, filePath, chunkLength);
                task[i] = messageService.QueueTask("map_queue", filePath);
                Console.WriteLine($"Chunk{i}");
            };

            Task.WaitAll(task);
            Console.WriteLine("Mapper completed.");

            shuffleService.CreateReducerFiles();
        }
        else
        {
            Console.WriteLine("File not found.");
        }
    }

    static void WriteWords(IEnumerator<string> words, Encoding encoding, string outputPath, int maxFileSize)
    {
        var buffer = new List<byte>();
        do
        {
            buffer.AddRange(encoding.GetBytes(words.Current + " "));
        }
        while (words.MoveNext() && maxFileSize - buffer.Count >= encoding.GetByteCount(words.Current));

        File.WriteAllBytes(outputPath, buffer.ToArray());
    }

    static IEnumerable<string> GetWords(StreamReader reader)
    {
        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            var words = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            foreach (var word in words)
            {
                yield return word;
            }
        }
    }
}
