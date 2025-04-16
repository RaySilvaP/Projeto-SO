using System.Text;

public class Program
{
    public static void Main(string[] args)
    {
        const int CHUNKS_AMOUNT = 10;
        if (File.Exists(args[0]))
        {
            using StreamReader reader = File.OpenText(args[0]);
            using var words = GetWords(reader).GetEnumerator();
            if (!words.MoveNext())
            {
                Console.WriteLine("Invalid text file.");
                return;
            }

            int chunkLength = (int)Math.Ceiling(reader.BaseStream.Length / (double)CHUNKS_AMOUNT);
            for (int i = 0; i < CHUNKS_AMOUNT; i++)
            {
                WriteWords(words, reader.CurrentEncoding, $"./chunk{i}.txt", chunkLength);
            };
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
