using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Hyak.Common;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Microsoft.Data.Edm.EdmToClrConversion;
using BlackBarLabs.Web;
using BlackBarLabs.Core.Collections;

namespace BlackBarLabs.Search.Azure
{
    public class AzureSearchEngine
    {
        private readonly SearchServiceClient searchClient;

        public AzureSearchEngine(SearchServiceClient searchClient)
        {
            this.searchClient = searchClient;
        }

        public async Task<Field> CreateFieldAsync(string indexName, string fieldName, Type type,
            bool isKey, bool isSearchable, bool isFilterable, bool isSortable, bool isFacetable, bool isRetrievable)
        {
            var field = new Field()
            {
                Name = fieldName,
                Type = GetEdmType(type),
                IsKey = isKey,
                IsSearchable = isSearchable,
                IsFilterable = isFilterable,
                IsSortable = isSortable,
                IsFacetable = isFacetable,
                IsRetrievable = isRetrievable
            };

            try
            {
                var index = await searchClient.Indexes.GetAsync(indexName);

                if (isKey)
                {
                    var keyFields = index.Fields.Where(fld => fld.IsKey).ToArray();
                    if (keyFields.Any())
                        return default(Field);

                    foreach(var fld in keyFields)
                        index.Fields.Remove(fld);
                }
                index.Fields.AddIfNotExisting(field);

                try
                {
                    var response = await searchClient.Indexes.CreateOrUpdateAsync(index);
                    return field;
                } catch(Microsoft.Rest.Azure.CloudException clEx)
                {
                    var indexNew = await searchClient.Indexes.GetAsync(indexName);
                    if(indexNew.ETag != index.ETag)
                        return await CreateFieldAsync(indexName, fieldName, type,
                            isKey, isSearchable, isFilterable, isSortable, isFacetable, isRetrievable);
                    throw clEx;
                }
            }
            catch (Exception ex)
            {
                if (!searchClient.Indexes.Exists(indexName))
                {
                    var index = new Index(indexName, field.ToEnumerable().ToList());
                    await searchClient.Indexes.CreateAsync(index);
                    return await CreateFieldAsync(indexName, fieldName, type, isKey, isSearchable, isFilterable, isSortable, isFacetable, isRetrievable);
                }
                throw;
            }
        }

        public delegate void CreateIndexFieldsCallback(CreateFieldCallback createField);
        public delegate void CreateFieldCallback(string fieldName, Type fieldType, bool isKey, 
            bool isSearchable, bool isFilterable, bool isSortable, bool isFacetable, bool isRetrievable);
        public delegate void CreateSuggesterCallback(string suggesterName, List<string> fieldNames);
        public delegate void CreateIndexSuggesterCallback(CreateSuggesterCallback suggesterCallback);

        public async Task<bool> CreateIndexAsync(string indexName, CreateIndexFieldsCallback createIndexFieldsCallback, 
            CreateIndexSuggesterCallback createSuggesterCallback, int creationDelay = 0)
        {
            return await CreateOrUpdateIndexAsync(indexName, createIndexFieldsCallback, createSuggesterCallback, creationDelay);
        }

        public async Task<bool> CreateOrUpdateIndexAsync(string indexName, CreateIndexFieldsCallback createIndexFieldsCallback, 
            CreateIndexSuggesterCallback createSuggesterCallback, int creationDelay = 0)
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

                var suggester = default(Suggester);
                createSuggesterCallback.Invoke((name, fieldNames) =>
                {
                    suggester = new Suggester
                    {
                        Name = name,
                        SourceFields = fieldNames
                    };
                });

                var definition = new Index()
                {
                    Name = indexName,
                    Fields = fields,
                };
                if (default(Suggester) != suggester)
                    definition.Suggesters.Add(suggester);

                var response = await searchClient.Indexes.CreateOrUpdateAsync(definition);
                await Task.Delay(creationDelay);
                return true;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error creating index: {ex.Message}\r\n");
            }
        }

        public bool CreateIndex(string indexName, CreateIndexFieldsCallback createIndexFieldsCallback, CreateIndexSuggesterCallback createSuggesterCallback)
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

                var suggester = default(Suggester);
                createSuggesterCallback.Invoke((name, names) =>
                {
                    suggester = new Suggester
                    {
                        Name = name,
                        SourceFields = names
                    };
                });

                var definition = new Index()
                {
                    Name = indexName,
                    Fields = fields
                };
                if (default(Suggester) != suggester)
                    definition.Suggesters.Add(suggester);

                var response = searchClient.Indexes.Create(definition);
                return true;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error creating index: {ex.Message}\r\n");
            }
        }
        
        private static DataType GetEdmType(Type type)
        {
            // Types of search fields must be in Entity Data Format.  https://msdn.microsoft.com/en-us/library/azure/dn946880.aspx
            switch (type.FullName)
            {
                case "System.String":
                    return DataType.String;
                case "System.Double":
                    return DataType.Double;
                case "System.Decimal":
                    return DataType.Double;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static Type GetClrType(DataType type)
        {
            if (DataType.Boolean.Equals(type))
                return typeof(bool);
            if (DataType.DateTimeOffset.Equals(type))
                return typeof(DateTimeOffset);
            if (DataType.Double.Equals(type))
                return typeof(double);
            if (DataType.Int32.Equals(type))
                return typeof(int);
            if (DataType.Int64.Equals(type))
                return typeof(long);
            if (DataType.String.Equals(type))
                return typeof(string);
            throw new ArgumentOutOfRangeException();
        }

        public IDictionary<string, Type> FieldsFromSearchIndex(string searchIndexName)
        {
            try
            {
                var index = this.searchClient.Indexes.Get(searchIndexName);
                var types = index.Fields
                    .Select(field => new KeyValuePair<string, Type>(
                        field.Name, GetClrType(field.Type)))
                    .ToDictionary();
                return types;
            }
            catch(Exception ex)
            {
                if (!searchClient.Indexes.Exists(searchIndexName))
                    return new Dictionary<string, Type>();
                throw;
            }
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

        public async Task<bool> MergeOrUploadItemsAsync<T>(string indexName, IEnumerable<T> itemList, Action<string> createIndex, int numberOfTimesToRetry = 3)
            where T : class
        {
            throw new NotImplementedException();
        }

        public async Task<bool> MergeOrUploadItemsToIndexAsync(string indexName,
            IEnumerable<IDictionary<string, object>> itemList,
            Action<string> createIndex,
            int numberOfTimesToRetry = 3)
        {
            if (!searchClient.Indexes.Exists(indexName))
            {
                createIndex.Invoke(indexName);
            }

            var indexClient = searchClient.Indexes.GetClient(indexName);
            if (default(SearchIndexClient) == indexClient)
                throw new InvalidOperationException("Index does not exist: " + indexName);

            var documents = itemList.Select(
                item =>
                {
                    var doc = new Microsoft.Azure.Search.Models.Document();
                    foreach(var itemKvp in item)
                    {
                        doc.Add(itemKvp.Key, itemKvp.Value);
                    }
                    return doc;
                });

            while (numberOfTimesToRetry >= 0)
            {
                try
                {
                    var batch = IndexBatch.Upload(documents);
                    await indexClient.Documents.IndexAsync(batch);
                    return true;
                }
                catch (Exception ex)
                {
                    if ((typeof(IndexBatchException) != ex.GetType()) && (typeof(CloudException) != ex.GetType()))
                        throw;
                }
                numberOfTimesToRetry--;
            }
            throw new Exception("Indexing of items has exceeded maximum allowable attempts");
        }

        public async Task<TResult> GetDocumentById<TResult>(string indexName, string id, Func<TResult, TResult> convertFunc)
            where TResult : class, new()
        {
            var indexClient = searchClient.Indexes.GetClient(indexName);

            try
            {
                var response = await indexClient.Documents.GetAsync<TResult>(id);
                var doc = convertFunc(response);
                return doc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public async Task<bool> DeleteItemsAsync<T>(string indexName, List<T> itemList, int numberOfTimesToRetry = 3)
            where T : class
        {
            var indexClient = searchClient.Indexes.GetClient(indexName);
            if (default(SearchIndexClient) == indexClient)
                throw new InvalidOperationException("Index does not exist: " + indexName);

            while (numberOfTimesToRetry >= 0)
            {
                try
                {
                    var actions =
                        itemList.Select(item => IndexAction.Delete(item));
                    var batch = IndexBatch.Upload(actions);
                    await indexClient.Documents.IndexAsync(batch);
                    return true;
                }
                catch (Exception ex)
                {
                    if ((typeof(IndexBatchException) != ex.GetType()) && (typeof(CloudException) != ex.GetType()))
                        throw;
                }
                numberOfTimesToRetry--;
            }
            throw new Exception("Deletion of items has exceeded maximum allowable attempts");
        }

        public async Task<IEnumerable<TResult>> SearchDocumentsAsync<TResult>(
            string indexName, string searchText,
            string[] facetFields, bool? includeTotalResultCount, int? top, int? skip, string filter,
            Func<TResult, TResult> convertFunc,
            Action<string, Dictionary<string, long?>> facetFunc,
            Action<long?> count)
            where TResult : class, new()
        {
            var indexClient = searchClient.Indexes.GetClient(indexName);

            // Execute search based on search text and optional filter 
            var searchParameters = new SearchParameters();
            if (!string.IsNullOrEmpty(filter))
                searchParameters.Filter = filter;

            if (default(string[]) != facetFields)
                searchParameters.Facets = facetFields;

            if (null != includeTotalResultCount)
                searchParameters.IncludeTotalResultCount = (bool)includeTotalResultCount;

            if (null != top)
                searchParameters.Top = top;

            if (null != skip)
                searchParameters.Skip = skip;

            return await DoSearch(indexClient, searchText, facetFields, searchParameters, convertFunc, facetFunc, count.Invoke);
        }

        private async Task<IEnumerable<TResult>> DoSearch<TResult>(ISearchIndexClient indexClient, string searchText, string[] facetFields, 
            SearchParameters searchParameters, Func<TResult, TResult> convertFunc, Action<string, Dictionary<string, long?>> facetFunc, Action<long?> count)
             where TResult : class, new()
        {
            var response = await indexClient.Documents.SearchAsync(searchText, searchParameters);


            var asdf = response.Results[0].Document.Values;


            return null;

            //var items = response.Results.Select(item => convertFunc(item.Document));
            //if (default(string[]) != facetFields)
            //{
            //    foreach (var facet in response.Facets)
            //    {
            //        var facetValues = facet.Value.ToDictionary(item => item.Value.ToString(), item => item.Count);
            //        facetFunc.Invoke(facet.Key, facetValues);
            //    }
            //}
            //count.Invoke(response.Count);

            //var continuationItems = new List<TResult>() as IEnumerable<TResult>;
            //if (null != response.ContinuationToken)
            //{
            //    continuationItems = await DoSearch(indexClient, response.ContinuationToken, facetFields, convertFunc, facetFunc, count.Invoke);
            //}
            //return items.Concat(continuationItems);
        }

        private async Task<IEnumerable<TResult>> DoSearch<TResult>(ISearchIndexClient indexClient, SearchContinuationToken continuationToken, string[] facetFields,
            Func<TResult, TResult> convertFunc, Action<string, Dictionary<string, long?>> facetFunc, Action<long?> count)
             where TResult : class, new()
        {
            var response = await indexClient.Documents.ContinueSearchAsync<TResult>(continuationToken);
            var items = response.Results.Select(item => convertFunc(item.Document));
            if (default(string[]) != facetFields)
            {
                foreach (var facet in response.Facets)
                {
                    var facetValues = facet.Value.ToDictionary(item => item.Value.ToString(), item => item.Count);
                    facetFunc.Invoke(facet.Key, facetValues);
                }
            }
            count.Invoke(response.Count);

            var continuationItems = new List<TResult>() as IEnumerable<TResult>;
            if (null != response.ContinuationToken)
            {
                continuationItems = await DoSearch(indexClient, response.ContinuationToken, facetFields, convertFunc, facetFunc, count.Invoke);
            }
            return items.Concat(continuationItems);
        }


        public async Task<IEnumerable<T>> SuggestAsync<T>(string indexName, string suggestName, string searchText, int top, bool fuzzy, Func<T, T> convertFunc, string filter = null)
            where T : class, new()
        {
            var indexClient = searchClient.Indexes.GetClient(indexName);
            var suggestParameters = new SuggestParameters()
            {
                UseFuzzyMatching = fuzzy,
                Top = top,
                Filter = filter
            };

            var response = await indexClient.Documents.SuggestAsync<T>(searchText, suggestName, suggestParameters);
            var suggestions = response.Results.Select(item => convertFunc(item.Document));
            return suggestions;
        }

        public async Task<bool> UpdateItemAtomicAsync<T>(string indexName, T item, int numberOfTimesToRetry = 10)
            where T : class
        {
            var indexClient = searchClient.Indexes.GetClient(indexName);
            if (default(SearchIndexClient) == indexClient)
                throw new InvalidOperationException("Index does not exist: " + indexName);

            while (numberOfTimesToRetry >= 0)
            {
                try
                {
                    var itemList = new List<T>() { item };
                    return await MergeOrUploadItemsAsync(indexName, itemList, null);
                }
                catch (Exception ex)
                {
                    if ((typeof(IndexBatchException) != ex.GetType()) && (typeof(CloudException) != ex.GetType()))
                        throw;
                }
                numberOfTimesToRetry--;
            }
            throw new Exception("Indexing of items has exceeded maximum allowable attempts");
        }

        public async Task<bool> UpdateItemAsync(string indexName, IDictionary<string, object> item)
        {
            var indexClient = searchClient.Indexes.GetClient(indexName);
            if (default(SearchIndexClient) == indexClient)
                throw new InvalidOperationException("Index does not exist: " + indexName);

            var itemList = item.ToEnumerable();
            return await MergeOrUploadItemsToIndexAsync(indexName, itemList, null);
        }
    }
}
