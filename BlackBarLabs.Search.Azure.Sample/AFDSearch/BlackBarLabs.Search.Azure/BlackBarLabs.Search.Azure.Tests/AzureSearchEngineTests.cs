using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace BlackBarLabs.Search.Azure.Tests
{
    [TestClass]
    public class AzureSearchEngineTests
    { 
        private AzureSearchEngine azureSearchEngine;
        private const string SuggesterName = "sg";

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
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "Yellow", null, false, 50, 0, null,
                    product => product, (s, longs) => { }, l => { var count = l; });
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

        [TestMethod]
        public async Task GetFacets()
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
                var facetFields = new List<string>() {"Brand"};
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "*", facetFields, false, 50, 0, null,
                    product => product, (facetKey, facets) =>
                    {
                        Assert.IsTrue(facetFields.Contains(facetKey));
                        Assert.IsTrue(facets.ContainsKey("Coke"));
                        Assert.IsTrue(facets.ContainsKey("Pepsi"));
                        Assert.IsTrue(facets.ContainsKey("NeHi"));
                        Assert.IsTrue(facets["Coke"] == 4);
                        Assert.IsTrue(facets["Pepsi"] == 3);
                        Assert.IsTrue(facets["NeHi"] == 1);
                    }, l => { var count = l; });
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
        public async Task Filter()
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
                var facetFields = new List<string>() { "Brand" };
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "*", facetFields, false, 50, 0, null,
                    product => product, (facetKey, facets) =>
                    {
                        Assert.IsTrue(facetFields.Contains(facetKey));
                        Assert.IsTrue(facets.ContainsKey("Coke"));
                        Assert.IsTrue(facets.ContainsKey("Pepsi"));
                        Assert.IsTrue(facets.ContainsKey("NeHi"));
                        Assert.IsTrue(facets["Coke"] == 4);
                        Assert.IsTrue(facets["Pepsi"] == 3);
                        Assert.IsTrue(facets["NeHi"] == 1);
                    }, l => { var count = l; });
                var found = false;
                foreach (var doc in foundDocs)
                {
                    if (doc.ProductName.Contains("Yellow"))
                        found = true;
                }
                Assert.IsTrue(found);

                // Apply a filter
                foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "*", facetFields, false, 50, 0, "Brand eq 'Pepsi'",
                    product => product, (facetKey, facets) =>
                    {
                        Assert.IsTrue(facetFields.Contains(facetKey));
                        Assert.IsFalse(facets.ContainsKey("Coke"));
                        Assert.IsTrue(facets.ContainsKey("Pepsi"));
                        Assert.IsFalse(facets.ContainsKey("NeHi"));
                        Assert.IsTrue(facets["Pepsi"] == 3);
                    }, l => { var count = l; });
                found = false;
                foreach (var doc in foundDocs)
                {
                    if (doc.ProductName.Contains("Diet Pepsi"))
                        found = true;
                }
                Assert.IsTrue(found);


                // And a more complicated filter
                foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "*", facetFields, false, 50, 0, "Brand eq 'Pepsi' and Cost ge 200",
                    product => product, (facetKey, facets) =>
                    {
                        Assert.IsTrue(facetFields.Contains(facetKey));
                        Assert.IsFalse(facets.ContainsKey("Coke"));
                        Assert.IsTrue(facets.ContainsKey("Pepsi"));
                        Assert.IsFalse(facets.ContainsKey("NeHi"));
                        Assert.IsTrue(facets["Pepsi"] == 2);
                    }, l => { var count = l; });
                found = false;
                foreach (var doc in foundDocs)
                {
                    if (doc.ProductName.Contains("Diet Pepsi"))
                        found = true;
                }
                Assert.IsTrue(found);

                // And a more complicated filter with top
                foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "*", facetFields, false, 50, 0, "Brand eq 'Pepsi' and Cost ge 200",
                    product => product, (facetKey, facets) =>
                    {
                        Assert.IsTrue(facetFields.Contains(facetKey));
                        Assert.IsFalse(facets.ContainsKey("Coke"));
                        Assert.IsTrue(facets.ContainsKey("Pepsi"));
                        Assert.IsFalse(facets.ContainsKey("NeHi"));
                        Assert.IsTrue(facets["Pepsi"] == 2);
                    }, l => { var count = l; });
                found = false;
                foreach (var doc in foundDocs)
                {
                    if (doc.ProductName.Contains("Diet Pepsi"))
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

        [TestMethod]
        public async Task Paging()
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
                var facetFields = new List<string>() { "Brand" };

                long? totalFoundCount = null;
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "*", facetFields, true, 5, 0, null,
                    product => product, (facetKey, facets) =>
                    {
                    }, 
                    (count) => totalFoundCount = count);
                Assert.AreEqual(8, totalFoundCount);
                Assert.AreEqual(5, foundDocs.Count());

                // get the rest of the set
                foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "*", facetFields, true, 5, 5, null,
                    product => product, (facetKey, facets) =>
                    {
                    },
                    (count) => totalFoundCount = count);
                Assert.AreEqual(8, totalFoundCount);
                Assert.AreEqual(3, foundDocs.Count());

            
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
        public async Task Suggest()
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

                var results = await this.azureSearchEngine.SuggestAsync<ProductSuggest>(distributorId, SuggesterName,
                    "Coke", 8, true,
                    suggest => suggest);
                Assert.AreEqual(2, results.Count());
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
            },
            (callback =>
            {
                callback.Invoke(SuggesterName, new List<string>() { "RowKey","ProductName" });   
            }) 
            ,5000);
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
                new Product() {RowKey = "1", Brand = "Coke", ProductName = "Coke Classic", Sku = "123456", Cost = 100},
                new Product() {RowKey = "2", Brand = "Coke", ProductName = "Sprite", Sku = "123457", Cost = 100},
                new Product() {RowKey = "3", Brand = "Coke", ProductName = "Diet Coke", Sku = "123458", Cost = 201},
                new Product() {RowKey = "4", Brand = "Coke", ProductName = "Mello Yellow", Sku = "123459", Cost = 100},
                new Product() {RowKey = "5", Brand = "Pepsi", ProductName = "Pepsi", Sku = "223450", Cost = 200},
                new Product() {RowKey = "6", Brand = "Pepsi", ProductName = "Diet Pepsi", Sku = "223451", Cost = 210},
                new Product() {RowKey = "7", Brand = "Pepsi", ProductName = "Pepsi Clear", Sku = "223452", Cost = 190},
                new Product() {RowKey = "8", Brand = "NeHi", ProductName = "Grape", Sku = "323450", Cost = 300}
            };
            return products;
        }

        [TestMethod]
        public async Task MergeDataInIndex()
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

                var updatedProducts = products.Select(product => new Product()
                {
                    Brand = "Updated" + product.Brand,
                    Cost = product.Cost,
                    ProductName = product.ProductName,
                    RowKey = product.RowKey,
                    Sku = product.Sku
                }).ToList();

                //Add the same again with updates
                await azureSearchEngine.IndexItemsAsync<Product>(distributorId, updatedProducts, async indexName =>
                {
                    await CreateIndexInternalAsync(indexName);
                });


                await Task.Delay(5000);  // Azure Search says the indexing on their side could take some time.  Particularly on a shared search instance.
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(distributorId, "UpdatedCoke", null, false, 50, 0, null,
                    product => product, (s, longs) => { }, l => { var count = l; });
                Assert.IsTrue(foundDocs.Any());
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

        [Ignore]
        [TestMethod]
        // This test is just here to toy with batching index updates and compare times.  It should always be ignored.
        public async Task AddDataToIndexOneAtATime()
        {
            var exception = default(Exception);
            var distributorId = Guid.NewGuid().ToString("N");
            try
            {
                await CreateIndexInternalAsync(distributorId);
                var products = CreateProductListByCount(300);

                var stopwatch = new Stopwatch();
                stopwatch.Start();
                foreach (var product in products)
                {
                    await azureSearchEngine.IndexItemsAsync<Product>(distributorId, new List<Product>() {product}, async indexName =>
                    {
                        await CreateIndexInternalAsync(indexName);
                    });
                }
                stopwatch.Stop();
                Console.WriteLine("One at a time: " + stopwatch.Elapsed);

                await DeleteIndexInternalAsync(distributorId);

                await CreateIndexInternalAsync(distributorId);
                stopwatch = new Stopwatch();
                stopwatch.Start();
                await azureSearchEngine.IndexItemsAsync<Product>(distributorId, products, async indexName =>
                {
                    await CreateIndexInternalAsync(indexName);
                });
                stopwatch.Stop();
                Console.WriteLine("Entire list at once: " + stopwatch.Elapsed);
                
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

        private static List<Product> CreateProductListByCount(int count)
        {
            var products = new List<Product>();
            for (var i = 0; i < count; i++)
            {
                var key = i.ToString();
                products.Add(new Product() {RowKey = key, Brand = "Coke", ProductName = "Coke Classic", Sku = "123456" + key, Cost = 100});
            }
            return products;
        }
    }
}
