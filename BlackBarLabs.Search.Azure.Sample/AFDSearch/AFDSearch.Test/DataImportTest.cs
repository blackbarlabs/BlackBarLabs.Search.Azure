using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BlackBarLabs.Search.Azure;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Configuration;
using System.IO;
using System.Reflection;
using AFDSearch.Models;

namespace AFDSearch.Test
{
    [TestClass]
    public class DataImportTest
    {
        private AzureSearchEngine azureSearchEngine;
        private string indexName;
        private string suggesterName;
        private System.IO.Stream csvFile;

        [TestInitialize]
        public void Initialize()
        {
            indexName = ConfigurationManager.AppSettings["SearchServiceIndexName"];
            suggesterName = ConfigurationManager.AppSettings["SearchServiceSuggesterName"];
            var engines = new SearchEngines("SearchServiceName", "SearchServiceApiKey");
            azureSearchEngine = engines.AzureSearchEngine;
            csvFile = GetTestCsvFile();
        }

        private Stream GetTestCsvFile()
        {
            var assembly = Assembly.GetExecutingAssembly();
            const string resourceName = "AFDSearch.Test.airports.csv";
            return assembly.GetManifestResourceStream(resourceName);
        }

        [TestMethod]
        public async Task ImportData()
        {
            var airportIndexTasks = Parse( async (identifier, city, state, name, chart, region, afdlink) =>
            {
                var airport = new Airport()
                {
                    Id = Guid.NewGuid().ToString("N"),
                    Identifier = identifier,
                    City = city,
                    State = state,
                    Name = name,
                    Chart = chart,
                    Region = region,
                    AfdLink = afdlink
                };
                await azureSearchEngine.IndexItemsAsync(indexName, new List<Airport>() {airport},
                    async indName =>
                    {
                        await CreateIndexInternalAsync(indName);
                    });
            });
            await Task.WhenAll(airportIndexTasks);
        }

        private delegate Task AirportInfoDelegate(string identifier, string city, string state, string name, string chart, string region, string afdlink);
        private IEnumerable<Task> Parse(AirportInfoDelegate airportInfoCallback)
        {
            var parser = new Microsoft.VisualBasic.FileIO.TextFieldParser(this.csvFile);
            parser.TextFieldType = Microsoft.VisualBasic.FileIO.FieldType.Delimited;
            parser.SetDelimiters(",");
            while (!parser.EndOfData)
            {
                //Process row
                string[] fields = parser.ReadFields();
                if (7 != fields.Count())
                    continue;
                var identifier = fields[0];
                var city = fields[1];
                var state = fields[2];
                var name = fields[3];
                var chart = fields[4];
                var region = fields[5];
                var afdlink = fields[6];
                
                yield return airportInfoCallback.Invoke(identifier, city, state, name, chart, region, afdlink);
            }
            parser.Close();
        }

        private async Task CreateIndexInternalAsync(string distributorId)
        {
            var result = await azureSearchEngine.CreateIndexAsync(distributorId, field =>
            {
                foreach (var fieldInfo in AirportFieldInfo.SearchFields)
                {
                    field.Invoke(fieldInfo.Name, fieldInfo.Type, fieldInfo.IsKey, fieldInfo.IsSearchable,
                        fieldInfo.IsFilterable, fieldInfo.IsSortable, fieldInfo.IsFacetable, fieldInfo.IsRetrievable);
                }
            },
            (callback =>
            {
                callback.Invoke(suggesterName, new List<string>() {"Identifier", "City", "Name"});
            })
            , 5000);
        }

        public static class AirportFieldInfo
        {
            public static SearchFieldInfo[] SearchFields => new[]
            {
                new SearchFieldInfo { Name = "Id",          Type = typeof(string).ToString(),  IsKey = true,  IsSearchable = false,  IsFilterable = false,  IsSortable = false,  IsFacetable = false,  IsRetrievable = true},
                new SearchFieldInfo { Name = "Identifier",  Type = typeof(string).ToString(),  IsKey = false,  IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false,  IsRetrievable = true},
                new SearchFieldInfo { Name = "City",        Type = typeof(string).ToString(),  IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false,  IsRetrievable = true},
                new SearchFieldInfo { Name = "State",       Type = typeof(string).ToString(),  IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true},
                new SearchFieldInfo { Name = "Name",        Type = typeof(string).ToString(),  IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false,  IsRetrievable = true},
                new SearchFieldInfo { Name = "Chart",       Type = typeof(string).ToString(),  IsKey = false, IsSearchable = false,  IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true},
                new SearchFieldInfo { Name = "Region",      Type = typeof(string).ToString(),  IsKey = false, IsSearchable = false,  IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true},
                new SearchFieldInfo { Name = "AfdLink",     Type = typeof(string).ToString(),  IsKey = false, IsSearchable = false,  IsFilterable = true,  IsSortable = true,  IsFacetable = false,  IsRetrievable = true},
            };
        }

        public static List<string> SuggestionFieldNames => new List<string>() { "Id", "City", "Name" };

    }
}
