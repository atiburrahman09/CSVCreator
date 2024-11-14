using CsvHelper;
using CsvHelper.Configuration;
using Dapper;
using Newtonsoft.Json.Linq;
using System.Data.SqlClient;
using System.Globalization;

namespace CsvGenerator
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string baseDirectory = AppContext.BaseDirectory;
            string configFilePath = Path.Combine(baseDirectory, "config.txt"); // Updated path
            configFilePath = Path.GetFullPath(configFilePath);
            string configContent = File.ReadAllText(configFilePath);
            var configJson = JObject.Parse(configContent);
            string connectionString = configJson["ConnectionStrings"]["DefaultConnection"].ToString();

            Console.Write("Enter table name: ");
            string tableName = Console.ReadLine();

            Console.Write("Enter the number of rows to generate: ");
            int rowCount = int.Parse(Console.ReadLine());

            var columns = await GetColumnsAsync(connectionString, tableName);
            var data = GenerateRandomData(columns, rowCount);

            string csvFileName = Path.Combine(Path.GetDirectoryName(configFilePath), $"{tableName}_{rowCount}_data_{DateTime.Now:yyyyMMddHHmm}.csv");
            WriteToCsv(data, columns, csvFileName);

            Console.WriteLine($"CSV file '{csvFileName}' generated successfully!");
        }

        static async Task<dynamic[]> GetColumnsAsync(string connectionString, string tableName)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                var query = $@"
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = @TableName";
                var columns = await connection.QueryAsync(query, new { TableName = tableName });
                return columns.ToArray();
            }
        }

        static dynamic GenerateRandomData(dynamic[] columns, int rowCount)
        {
            var random = new Random();
            var data = new object[rowCount];

            for (int i = 0; i < rowCount; i++)
            {
                var row = new object[columns.Length];
                for (int j = 0; j < columns.Length; j++)
                {
                    string dataType = columns[j].DATA_TYPE;
                    row[j] = GenerateRandomValue(dataType, random);
                }
                data[i] = row;
            }

            return data;
        }

        static object GenerateRandomValue(string dataType, Random random)
        {
            switch (dataType)
            {
                case "int":
                    return random.Next(1, 1000000);
                case "bigint":
                    return (long)random.Next(1, 1000000) * random.Next(1, 1000);
                case "smallint":
                    return (short)random.Next(1, 100000);
                case "tinyint":
                    return (byte)random.Next(1, 100000);
                case "bit":
                    return random.Next(0, 2) == 1;
                case "decimal":
                case "numeric":
                    return (decimal)(random.NextDouble() * 1000000);
                case "float":
                    return random.NextDouble() * 1000000;
                case "real":
                    return (float)(random.NextDouble() * 1000000);
                case "money":
                case "smallmoney":
                    return (decimal)(random.NextDouble() * 1000000);
                case "char":
                case "varchar":
                case "nchar":
                case "nvarchar":
                    return $"RandomString_{random.Next(1, 1000000)}";
                case "date":
                    return DateTime.Now.AddDays(random.Next(-1000000, 1000000)).ToString("yyyy-MM-dd");
                case "datetime":
                case "smalldatetime":
                    return DateTime.Now.AddDays(random.Next(-1000000, 1000000)).AddSeconds(random.Next(0, 86400)).ToString("yyyy-MM-dd HH:mm:ss");
                case "datetime2":
                    return DateTime.Now.AddDays(random.Next(-1000000, 1000000)).AddSeconds(random.Next(0, 86400)).ToString("yyyy-MM-dd HH:mm:ss.fffffff");
                case "time":
                    return DateTime.Now.AddSeconds(random.Next(0, 86400)).ToString("HH:mm:ss");
                case "uniqueidentifier":
                    return Guid.NewGuid();
                case "binary":
                case "varbinary":
                    byte[] buffer = new byte[8];
                    random.NextBytes(buffer);
                    return buffer;
                default:
                    return DBNull.Value;
            }
        }

        static void WriteToCsv(dynamic data, dynamic[] columns, string fileName)
        {
            using (var writer = new StreamWriter(fileName))
            using (var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture)))
            {
                foreach (var column in columns)
                {
                    csv.WriteField(column.COLUMN_NAME);
                }
                csv.NextRecord();

                foreach (var row in data)
                {
                    foreach (var field in row)
                    {
                        csv.WriteField(field);
                    }
                    csv.NextRecord();
                }
            }
        }
    }
}