using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BlackBarLabs.Search.Azure.Tests
{
    [TestClass]
    public class AzureSearchEngineTests
    { 
        private AzureSearchEngine azureSearchEngine;
        [TestInitialize]
        public void TestInitialize()
        {
            var engines = new SearchEngines("SearchServiceName", "SearchServiceApiKey");
            azureSearchEngine = engines.AzureSearchEngine;
        }

        [TestMethod]
        public async Task CreateIndex()
        {
            var distributorId = Guid.NewGuid().ToString();
            await CreateIndexInternalAsync(distributorId);
            await DeleteIndexInternalAsync(distributorId);
        }

        [TestMethod]
        public async Task DeleteIndex()
        {
            var exception = default(Exception);
            var distributorId = Guid.NewGuid().ToString();
            try
            {
                await CreateIndexInternalAsync(distributorId);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                await DeleteIndexInternalAsync(distributorId);
                if (default(Exception) != exception)
                    throw exception;
            }

        }

        [TestMethod]
        public async Task AddDataToIndex()
        {
            var exception = default(Exception);
            var distributorId = Guid.NewGuid().ToString("N");
            try
            {
                await CreateIndexInternalAsync(distributorId);
                var products = CreateProductList();
                await azureSearchEngine.IndexItemsAsync<Product>(distributorId, products, async indexName =>
                {
                    await CreateIndexInternalAsync(indexName);
                });
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                await DeleteIndexInternalAsync(distributorId);
                if (default(Exception) != exception)
                    throw exception;
            }
        }

        [TestMethod]
        public async Task Search()
        {
            var exception = default(Exception);
            var distributorId = Guid.NewGuid().ToString();
            try
            {
                await CreateIndexInternalAsync(distributorId);
                var products = CreateProductList();
                await azureSearchEngine.IndexItemsAsync<Product>(distributorId, products, async indexName =>
                {
                    await CreateIndexInternalAsync(indexName);
                });
                await Task.Delay(5000);  // Azure Search says the indexing on their side could take some time.  Particularly on a shared search instance.
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "Yellow",
                    product => product);
                var found = false;
                foreach (var doc in foundDocs)
                {
                    if (doc.ProductName.Contains("Yellow"))
                        found = true;
                }
                Assert.IsTrue(found);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                await DeleteIndexInternalAsync(distributorId);
                if (default(Exception) != exception)
                    throw exception;
            }
        }

        private async Task CreateIndexInternalAsync(string distributorId)
        {
            var result = await azureSearchEngine.CreateIndexAsync(distributorId, field =>
            {
                foreach (var fieldInfo in ProductFieldInfo.SearchFields)
                {
                    field.Invoke(fieldInfo.Name, fieldInfo.Type, fieldInfo.IsKey, fieldInfo.IsSearchable, fieldInfo.IsFilterable, fieldInfo.IsSortable, fieldInfo.IsFacetable, fieldInfo.IsRetrievable);
                }
            }, 5000);
            Assert.IsTrue(result);
        }

        private async Task DeleteIndexInternalAsync(string distributorId)
        {
            var result = await azureSearchEngine.DeleteIndexAsync(distributorId);
            Assert.IsTrue(result);
        }

        private static List<Product> CreateProductList()
        {
            var products = new List<Product>
            {
                new Product() {RowKey = "1", Brand = "Coke", ProductName = "Coke Classic", Sku = "123456", Cost = "100"},
                new Product() {RowKey = "2", Brand = "Coke", ProductName = "Sprite", Sku = "123457", Cost = "100"},
                new Product() {RowKey = "3", Brand = "Coke", ProductName = "Diet Coke", Sku = "123458", Cost = "100"},
                new Product() {RowKey = "4", Brand = "Coke", ProductName = "Mello Yellow", Sku = "123459", Cost = "100"}
            };
            return products;
        } 
    }
}
