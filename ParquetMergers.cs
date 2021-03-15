using Parquet;
using Parquet.Data;
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace ParquetUtils
{
    public class ParquetMergers
    {
        public static void Merge(IEnumerable<string> filePaths, string outputFile)
        {
            if (filePaths?.Count() == 0)
            {
                throw new Exception("Missing source file paths");
            }
            if (string.IsNullOrWhiteSpace(outputFile))
            {
                throw new Exception("Missing output file");
            }

            var commonFields = ExtractUniqeuFields(filePaths);

            if (commonFields.Count() == 0)
            {
                Console.WriteLine("No fields");
            }

            MergeFileData(filePaths, outputFile, commonFields);

        }

        private static void MergeFileData(IEnumerable<string> filePaths, string outputFile, IEnumerable<DataField> commonFields)
        {
            var schema = new Schema(commonFields.ToArray());

            var columns = new Dictionary<string, List<object>>(commonFields.Count());
            foreach (var field in commonFields)
            {
                columns[field.Name] = new List<object>();
            }

            foreach (var filePath in filePaths)
            {
                using (var stream = File.OpenRead(filePath))
                {
                    var reader = new ParquetReader(stream, null);
                    var fields = reader.Schema.Fields.ToArray();
                    var missingFields = commonFields.Where(f => !fields.Contains(f));
                    var rows = reader.ReadAsTable();
                    foreach (var row in rows)
                    {
                        for (int i = 0; i < fields.Length; i++)
                        {
                            columns[fields[i].Name].Add(row[i]);
                        }
                    }
                    if(missingFields.Count()>0)
                    {
                        foreach (var field in missingFields)
                        {
                            columns[field.Name].AddRange(new object[rows.Count]);
                        }
                    }

                }

            }

            using (var fileStream = File.OpenWrite(outputFile))
            {
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {
                    using (var groupWriter = parquetWriter.CreateRowGroup())
                    {
                        foreach (var field in commonFields)
                        {
                            var dataColumn = new DataColumn(new DataField(field.Name, field.ClrType), GetDataArray(columns[field.Name], field));
                            groupWriter.WriteColumn(dataColumn);
                            //using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                            //{
                            //    foreach (var item in cols)
                            //    {
                            //        groupWriter.WriteColumn(item);

                            //    }
                            //}
                        }

                    }
                }


            }
        }

        private static Array GetDataArray(List<object> items, DataField field)
        {
            if (field.ClrType == typeof(int))
            {
                return items.Select(i => (int)i).ToArray();
            }
             if (field.ClrType == typeof(decimal))
            {
                return items.Select(i => (decimal)i).ToArray();
            }
             if (field.ClrType == typeof(string))
            {
                return items.Select(i => (string)i).ToArray();
            }
            throw new Exception($"Type {field.ClrType.Name} not supported yet");

        }

        private static IEnumerable<DataField> ExtractUniqeuFields(IEnumerable<string> filePaths)
        {
            var commonFields = new List<DataField>();
            foreach (var filePath in filePaths)
            {
                using (var stream = File.OpenRead(filePath))
                {
                    var reader = new ParquetReader(stream, null);
                    //var tbl = reader.ReadAsTable();
                    var fields = reader.Schema.Fields;
                    foreach (var field in fields)
                    {
                        
                        if (!commonFields.Contains(field))
                        {
                            commonFields.Add((DataField) field);
                        }
                    }
                }

            }
            return commonFields;
        }
    }
}
