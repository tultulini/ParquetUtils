using System;
using System.Collections.Generic;

namespace ParquetUtils
{
    class Program
    {
        static void Main(string[] args)
        {
            var colDefs = new List<ParquetColumnDefinition>{
                new ParquetColumnDefinition { Name = "description",
                    Type = typeof(string)
                },
                new ParquetColumnDefinition { Name = "promotion_number",
                    Type = typeof(int) },
                new ParquetColumnDefinition { Name = "reward_report_dept",
                    Type = typeof(int) }
            };
            SchemaCreator.WriteSchemaParquet(colDefs, "testi.parquet");


        }
    }
}
