﻿using System;
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
        private readonly SearchServiceClient searchClient;

        public AzureSearchEngine(SearchServiceClient searchClient)
        {
            this.searchClient = searchClient;
        }

        public delegate void CreateIndexFieldsCallback(CreateFieldCallback createField);
        public delegate void CreateFieldCallback(string fieldName, string fieldType, bool isKey, bool isSearchable, bool isFilterable, bool isSortable, bool isFacetable, bool isRetrievable);
        public delegate void CreateSuggesterCallback(string suggesterName, List<string> fieldNames);
        public delegate void CreateIndexSuggesterCallback(CreateSuggesterCallback suggesterCallback);
        public async Task<bool> CreateIndexAsync(string indexName, CreateIndexFieldsCallback createIndexFieldsCallback, CreateIndexSuggesterCallback createSuggesterCallback, int creationDelay = 0)
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

                var response = await searchClient.Indexes.CreateAsync(definition);
                await Task.Delay(creationDelay);
                return (response.StatusCode == HttpStatusCode.Created || response.StatusCode == HttpStatusCode.OK);
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
                case "System.Double":
                    return "Edm.Double";
                case "System.Decimal":
                    return "Edm.String";
                default:
                    throw new ArgumentOutOfRangeException();
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

        public async Task<bool> MergeOrUploadItemsAsync<T>(string indexName, List<T> itemList, Action<string> createIndex, int numberOfTimesToRetry = 3)
            where T : class
        {
            if (!searchClient.Indexes.Exists(indexName))
            {
                createIndex.Invoke(indexName);
            }

            var indexClient = searchClient.Indexes.GetClient(indexName);
            if (default(SearchIndexClient) == indexClient)
                throw new InvalidOperationException("Index does not exist: " + indexName);

            while (numberOfTimesToRetry >= 0)
            {
                try
                {
                    var x = new object();
                    var actions =
                        itemList.Select(item => IndexAction.Create(IndexActionType.MergeOrUpload, x));
                    var batch = IndexBatch.Create(actions);
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
                var doc = convertFunc(response.Document);
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
                        itemList.Select(item => IndexAction.Create(IndexActionType.Delete, item));
                    var batch = IndexBatch.Create(actions);
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
            List<string> facetFields, bool? includeTotalResultCount, int? top, int? skip, string filter,
            Func<TResult, TResult> convertFunc,
            Action<string, Dictionary<string, long>> facetFunc,
            Action<long?> count)
            where TResult : class, new()
        {
            var indexClient = searchClient.Indexes.GetClient(indexName);

            // Execute search based on search text and optional filter 
            var searchParameters = new SearchParameters();
            if (!string.IsNullOrEmpty(filter))
                searchParameters.Filter = filter;

            if (default(List<string>) != facetFields)
                searchParameters.Facets = facetFields;

            if (null != includeTotalResultCount)
                searchParameters.IncludeTotalResultCount = (bool)includeTotalResultCount;

            if (null != top)
                searchParameters.Top = top;

            if (null != skip)
                searchParameters.Skip = skip;

            return await DoSearch(indexClient, searchText, facetFields, searchParameters, convertFunc, facetFunc, count.Invoke);
        }

        private async Task<IEnumerable<TResult>> DoSearch<TResult>(SearchIndexClient indexClient, string searchText, List<string> facetFields, 
            SearchParameters searchParameters, Func<TResult, TResult> convertFunc, Action<string, Dictionary<string, long>> facetFunc, Action<long?> count)
             where TResult : class, new()
        {
            var response = await indexClient.Documents.SearchAsync<TResult>(searchText, searchParameters);
            var items = response.Select(item => convertFunc(item.Document));
            if (default(List<string>) != facetFields)
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

        private async Task<IEnumerable<TResult>> DoSearch<TResult>(SearchIndexClient indexClient, SearchContinuationToken continuationToken, List<string> facetFields,
            Func<TResult, TResult> convertFunc, Action<string, Dictionary<string, long>> facetFunc, Action<long?> count)
             where TResult : class, new()
        {
            var response = await indexClient.Documents.ContinueSearchAsync<TResult>(continuationToken);
            var items = response.Select(item => convertFunc(item.Document));
            if (default(List<string>) != facetFields)
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
            var suggestions = response.Select(item => convertFunc(item.Document));
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
    }
}
