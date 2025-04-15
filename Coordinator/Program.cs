public class Program
{
    public static void Main(string[] args)
    {
        if(File.Exists(args[0]))
        {
            SplitFile(args[0]);
        }
        else
        {
            Console.WriteLine("File not found.");
        }
    }

    static void SplitFile(string filePath)
    {
        const int CHUNKS_AMOUNT = 10;

        using StreamReader reader = File.OpenText(filePath);
        int chunkSize = (int)Math.Ceiling(reader.BaseStream.Length / (double)CHUNKS_AMOUNT);
        var lastLine = string.Empty;
        for(int i = 0; i < CHUNKS_AMOUNT; i++)
        {
            var buffer = new List<byte>();
            while(lastLine != null && chunkSize - buffer.Count >= reader.CurrentEncoding.GetByteCount(lastLine))
            {
                buffer.AddRange(reader.CurrentEncoding.GetBytes(lastLine));
                lastLine = reader.ReadLine() + " ";
            }
            File.WriteAllBytes($"./chunk{i}.txt", buffer.ToArray());
        };
    }
}
