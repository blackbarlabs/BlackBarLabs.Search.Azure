using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Hyak.Common;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;

namespace BlackBarLabs.Search.Azure
{
    public class AzureSearchEngine
    {
        private SearchServiceClient searchClient;

        public AzureSearchEngine(SearchServiceClient searchClient)
        {
            this.searchClient = searchClient;
        }

        public delegate void CreateIndexFieldsCallback(CreateFieldCallback createField);
        public delegate void CreateFieldCallback(string fieldName, string fieldType, bool isKey, bool isSearchable, bool isFilterable, bool isSortable, bool isFacetable, bool isRetrievable);
        public async Task<bool> CreateIndexAsync(string indexName, CreateIndexFieldsCallback createIndexFieldsCallback, int creationDelay = 0)
        {
            try
            {
                var fields = new List<Field>();
                createIndexFieldsCallback.Invoke(
                    (name, type, isKey, isSearchable, isFilterable, isSortable, isFacetable, isRetrievable) =>
                    {
                        fields.Add(new Field()
                        {
                            Name = name,
                            Type = GetEdmType(type),
                            IsKey = isKey,
                            IsSearchable = isSearchable,
                            IsFilterable = isFilterable,
                            IsSortable = isSortable,
                            IsFacetable = isFacetable,
                            IsRetrievable = isRetrievable
                        });
                    });

                var definition = new Index()
                {
                    Name = indexName,
                    Fields = fields
                };
                var response = await searchClient.Indexes.CreateAsync(definition);
                await Task.Delay(creationDelay);
                return (response.StatusCode == HttpStatusCode.Created || response.StatusCode == HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error creating index: {ex.Message}\r\n");
            }
        }

        public bool CreateIndex(string indexName, CreateIndexFieldsCallback createIndexFieldsCallback)
        {
            try
            {
                var fields = new List<Field>();
                createIndexFieldsCallback.Invoke(
                    (name, type, isKey, isSearchable, isFilterable, isSortable, isFacetable, isRetrievable) =>
                    {
                        fields.Add(new Field()
                        {
                            Name = name,
                            Type = GetEdmType(type),
                            IsKey = isKey,
                            IsSearchable = isSearchable,
                            IsFilterable = isFilterable,
                            IsSortable = isSortable,
                            IsFacetable = isFacetable,
                            IsRetrievable = isRetrievable
                        });
                    });

                var definition = new Index()
                {
                    Name = indexName,
                    Fields = fields
                };
                var response = searchClient.Indexes.Create(definition);
                return (response.StatusCode == HttpStatusCode.Created || response.StatusCode == HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error creating index: {ex.Message}\r\n");
            }
        }

        private static string GetEdmType(string type)
        {
            // Types of search fields must be in Entity Data Format.  https://msdn.microsoft.com/en-us/library/azure/dn946880.aspx
            switch (type)
            {
                case "System.String":
                    return "Edm.String";
                case "System.Decimal":
                    return "Edm.String";
            }
            return "";
        }

        public async Task<bool> DeleteIndexAsync(string indexName)
        {
            try
            {
                await searchClient.Indexes.DeleteAsync(indexName);
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }

        public bool DeleteIndex(string indexName)
        {
            try
            {
                searchClient.Indexes.Delete(indexName);
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }

        public async Task<bool> IndexItemsAsync<T>(string indexName, List<T> itemList, Action<string> createIndex, int numberOfTimesToRetry = 200)
            where T : class
        {

            if (! searchClient.Indexes.Exists(indexName))
            {
                createIndex.Invoke(indexName);
            }

            //TODO - Use async methods on search api   http://alexmang.com/2015/03/azure-search-is-now-generally-available/

            var indexClient = searchClient.Indexes.GetClient(indexName);
            if (default(SearchIndexClient) == indexClient)
                throw new InvalidOperationException("Index does not exist: " + indexName);

            while (true)
            {
                try
                {
                    var response =
                        await indexClient.Documents.IndexAsync(IndexBatch.Create(itemList.Select(IndexAction.Create)));
                    return (response.StatusCode == HttpStatusCode.OK || response.StatusCode == HttpStatusCode.Created);
                }
                catch (Exception ex)
                {
                    if((typeof(IndexBatchException) != ex.GetType()) && (typeof(CloudException) != ex.GetType()))
                        throw;
                }
                numberOfTimesToRetry--;

                if (numberOfTimesToRetry <= 0)
                    throw new Exception("Indexing of items has exceeded maximum allowable attempts");
            }
        }
        
        public async Task<IEnumerable<TResult>> SearchDocumentsAsync<TResult>(string indexName, string searchText, Func<TResult, TResult> convertFunc, string filter = null)
            where TResult : class, new()
        {
            var indexClient = searchClient.Indexes.GetClient(indexName);

            // Execute search based on search text and optional filter 
            var sp = new SearchParameters();
            if (!string.IsNullOrEmpty(filter))
            {
                sp.Filter = filter;
            }
            var response = await indexClient.Documents.SearchAsync<TResult>(searchText, sp);
            //var products = response.Select((result => result.Document));
            var products = response.Select(item => convertFunc(item.Document));
            return products;
        }
    }
}
