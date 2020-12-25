using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace CsvHelperExamples
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Full file path of source file");
            string filePath = Console.ReadLine();
            Console.WriteLine("Enter delimiter");
            string delimiter = Console.ReadLine();
            Console.WriteLine("Enter target table name");
            string table = Console.ReadLine();
            var dt = Read(filePath, delimiter);

            SqlConnection con = new SqlConnection("Data Source=.;Integrated Security=SSPI;database=test");
            con.Open();

            using (var bulkCopy = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.KeepIdentity))
            {
                // my DataTable column names match my SQL Column names, so I simply made this loop. However if your column names don't match, just pass in which datatable name matches the SQL column name in Column Mappings
                foreach (DataColumn col in dt.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                bulkCopy.BulkCopyTimeout = 600;
                bulkCopy.DestinationTableName = table;
                bulkCopy.WriteToServer(dt);
            }


            con.Close();

        }



        private static DataTable Read(string filePath, string delimiter)
        {
            DataTable dt = new DataTable();

            List<dynamic> dynObj;
            try

            {
                using (StreamReader reader = File.OpenText(filePath))
                {

                    using (var csv = new CsvReader(reader))
                    {
                        dynObj = csv.GetRecords<dynamic>().ToList();

                        var json = JsonConvert.SerializeObject(dynObj);
                        dt = (DataTable) JsonConvert.DeserializeObject(json, (typeof(DataTable)));
                    }

                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            return dt;
        }
    }
}
    
        
    
        
    

