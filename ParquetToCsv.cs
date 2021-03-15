using Parquet;
using System.IO;
using System.Linq;

namespace ParquetUtils
{
    public class ParquetToCsv
    {
        public static void Convert(string parquetFile, string outputCsvFile)
        {
            using (var streamReader = File.OpenRead(parquetFile))
            {
                var reader = new ParquetReader(streamReader, null);
                var rows = reader.ReadAsTable();
                var fields = reader.Schema.Fields;
                using (var writer =new StreamWriter( File.OpenWrite(outputCsvFile)))
                {
                    writer.WriteLine(string.Join(",", fields.Select(f => f.Name)));
                    foreach (var row in rows)
                    {
                        writer.WriteLine(string.Join(",", row.Select(NullToString)));                        
                    }
                }
            }
        }
        private static string NullToString(object val)
        {
            return val == null ? string.Empty : val.ToString();
        }
    }
}
