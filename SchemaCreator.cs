using System.Linq;
using System.IO;
using Parquet;
using Parquet.Data;
using System.Collections.Generic;
using System;

namespace ParquetUtils
{
    public class SchemaCreator
    {
        public static void WriteSchemaParquet(List<ParquetColumnDefinition> definitions, string parquetName)
        {
            var cols = definitions.Select(def => new DataField(def.Name,def.Type));
            GenerateSchemaParquet(cols, parquetName);
        }

        private static void GenerateSchemaParquet(IEnumerable< DataField>fields, string fileName)
        {
            var schema = new Schema(fields.ToArray());

            using (Stream fileStream = File.OpenWrite(@"C:\temp\" + fileName))
            {
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {                    
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

        private static DataColumn GetDataColumn(ParquetColumnDefinition def)
        {
            
            DataColumn col;
            if (def.Type == typeof(int))
            {
                col = new DataColumn(new DataField<int>(def.Name), new int[] { 0 });
            }
            else if (def.Type == typeof(decimal))
            {
                col = new DataColumn(new DataField<decimal>(def.Name), new decimal[] { 0 });

            }
            else if (def.Type == typeof(string))
            {
                col = new DataColumn(new DataField<string>(def.Name), new string[] { "" });
            }
            else
            {
                throw new Exception($"{def.Type.Name} not supported");
            }
            return col;
        }
    }
}
