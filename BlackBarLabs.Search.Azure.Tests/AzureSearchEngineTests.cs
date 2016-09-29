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
            var indexName = Guid.NewGuid().ToString();
            await CreateIndexInternalAsync(indexName);
            await DeleteIndexInternalAsync(indexName);
        }

        [TestMethod]
        public async Task AddFieldsToExistingIndex()
        {
            var indexName = Guid.NewGuid().ToString();
            await CreateIndexInternalAsync(indexName);
            await AddFieldsToExsistingIndexInternalAsync(indexName);
            await azureSearchEngine.MergeOrUploadItemsAsync(indexName, CreateProductListWithAddedFields(),
                    s => { Assert.Fail("Index should already be created"); });
            await DeleteIndexInternalAsync(indexName);
        }

        [TestMethod]
        public async Task DeleteIndex()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString();
            try
            {
                await CreateIndexInternalAsync(indexName);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                await DeleteIndexInternalAsync(indexName);
                if (default(Exception) != exception)
                    throw exception;
            }

        }

        [TestMethod]
        public async Task AddDataToIndex()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString("N");
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductList();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
                });
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                await DeleteIndexInternalAsync(indexName);
                if (default(Exception) != exception)
                    throw exception;
            }
        }

        [TestMethod]
        public async Task DeleteDataFromIndex()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString("N");
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductList();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
                });

                await Task.Delay(5000); // Per Microsoft, there could be a delay in modifying the index.  Wait...
                
                await azureSearchEngine.DeleteItemsAsync<Product>(indexName, products);

                await Task.Delay(5000); // Per Microsoft, there could be a delay in modifying the index.  Wait...

                foreach (var product in products)
                {
                    var found = true;
                    try
                    {
                        var theProduct = await azureSearchEngine.GetDocumentById<Product>(indexName, product.RowKey, product1 =>
                        {
                            return product1 ?? default(Product);
                        });
                        if (theProduct == null) found = false;
                    }
                    catch (Exception)
                    {
                        found = false;
                    }
                    Assert.IsFalse(found);
                }
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                await DeleteIndexInternalAsync(indexName);
                if (default(Exception) != exception)
                    throw exception;
            }
        }

        [TestMethod]
        public async Task Search()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString();
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductList();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
                });
                await Task.Delay(5000);  // Azure Search says the indexing on their side could take some time.  Particularly on a shared search instance.

                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(indexName, "Yellow", null, false, 50, 0, null,
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
                await DeleteIndexInternalAsync(indexName);
                if (default(Exception) != exception)
                    throw exception;
            }
        }

        [TestMethod]
        public async Task GetFacets()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString();
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductList();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
                });
                await Task.Delay(5000);  // Azure Search says the indexing on their side could take some time.  Particularly on a shared search instance.
                var facetFields = new string[] { "Brand" };
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(indexName, "*", facetFields, false, 50, 0, null,
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
                await DeleteIndexInternalAsync(indexName);
                if (default(Exception) != exception)
                    throw exception;
            }
        }

        [TestMethod]
        public async Task Filter()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString();
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductList();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
                });
                await Task.Delay(5000);  // Azure Search says the indexing on their side could take some time.  Particularly on a shared search instance.
                var facetFields = new string[] { "Brand" };
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(indexName, "*", facetFields, false, 50, 0, null,
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
                foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(indexName, "*", facetFields, false, 50, 0, "Brand eq 'Pepsi'",
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
                foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(indexName, "*", facetFields, false, 50, 0, "Brand eq 'Pepsi' and Cost ge 200",
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
                foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(indexName, "*", facetFields, false, 50, 0, "Brand eq 'Pepsi' and Cost ge 200",
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
                await DeleteIndexInternalAsync(indexName);
                if (default(Exception) != exception)
                    throw exception;
            }
        }

        [TestMethod]
        public async Task Paging()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString();
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductList();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
                });
                await Task.Delay(5000);  // Azure Search says the indexing on their side could take some time.  Particularly on a shared search instance.
                var facetFields = new string[] { "Brand" };

                long? totalFoundCount = null;
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(indexName, "*", facetFields, true, 5, 0, null,
                    product => product, (facetKey, facets) =>
                    {
                    },
                    (count) => totalFoundCount = count);
                Assert.AreEqual(8, totalFoundCount);
                Assert.AreEqual(5, foundDocs.Count());

                // get the rest of the set
                foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(indexName, "*", facetFields, true, 5, 5, null,
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
                await DeleteIndexInternalAsync(indexName);
                if (default(Exception) != exception)
                    throw exception;
            }
        }



        [TestMethod]
        public async Task Suggest()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString();
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductList();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
                });
                await Task.Delay(5000);  // Azure Search says the indexing on their side could take some time.  Particularly on a shared search instance.

                var results = await this.azureSearchEngine.SuggestAsync<ProductSuggest>(indexName, SuggesterName,
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
                await DeleteIndexInternalAsync(indexName);
                if (default(Exception) != exception)
                    throw exception;
            }

        }

        private async Task CreateIndexInternalAsync(string indexName)
        {
            var result = await azureSearchEngine.CreateIndexAsync(indexName, field =>
            {
                foreach (var fieldInfo in ProductFieldInfo.SearchFields)
                {
                    field.Invoke(fieldInfo.Name, fieldInfo.Type, fieldInfo.IsKey, fieldInfo.IsSearchable, fieldInfo.IsFilterable, fieldInfo.IsSortable, fieldInfo.IsFacetable, fieldInfo.IsRetrievable);
                }
            },
            (callback =>
            {
                callback.Invoke(SuggesterName, new List<string>() { "RowKey", "ProductName" });
            })
            , 5000);
            Assert.IsTrue(result);
        }

        private async Task AddFieldsToExsistingIndexInternalAsync(string indexName)
        {
            var result = await azureSearchEngine.CreateIndexAsync(indexName, field =>
            {
                foreach (var fieldInfo in ProductFieldInfo.SearchFields)
                {
                    field.Invoke(fieldInfo.Name, fieldInfo.Type, fieldInfo.IsKey, fieldInfo.IsSearchable, fieldInfo.IsFilterable, fieldInfo.IsSortable, fieldInfo.IsFacetable, fieldInfo.IsRetrievable);
                }
                field.Invoke("AddedField1", typeof(string), false, false, false, false, false, false);
                field.Invoke("AddedField2", typeof(string), false, false, false, false, false, false);
            },
            (callback =>
            {
                callback.Invoke(SuggesterName, new List<string>() { "RowKey", "ProductName" });
                callback.Invoke(SuggesterName + "Added", new List<string>() { "AddedField1", "AddedField2" });
            })
            , 5000);
            Assert.IsTrue(result);
        }
        

        private async Task DeleteIndexInternalAsync(string indexName)
        {
            var result = await azureSearchEngine.DeleteIndexAsync(indexName);
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

        private static List<ProductWithAddedFields> CreateProductListWithAddedFields()
        {
            var products = new List<ProductWithAddedFields>
            {
                new ProductWithAddedFields() {RowKey = "1", Brand = "Coke", ProductName = "Coke Classic", Sku = "123456", Cost = 100, AddedField1 = "added1", AddedField2 = "added2"},
                new ProductWithAddedFields() {RowKey = "2", Brand = "Coke", ProductName = "Sprite", Sku = "123457", Cost = 100, AddedField1 = "added1", AddedField2 = "added2"},
            };
            return products;
        }

        [TestMethod]
        public async Task MergeDataInIndex()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString("N");
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductList();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
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
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, updatedProducts, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
                });


                await Task.Delay(5000);  // Azure Search says the indexing on their side could take some time.  Particularly on a shared search instance.
                var foundDocs = await azureSearchEngine.SearchDocumentsAsync<Product>(indexName, "UpdatedCoke", null, false, 50, 0, null,
                    product => product, (s, longs) => { }, l => { var count = l; });
                Assert.IsTrue(foundDocs.Any());
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                await DeleteIndexInternalAsync(indexName);
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
            var indexName = Guid.NewGuid().ToString("N");
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductListByCount(300);

                var stopwatch = new Stopwatch();
                stopwatch.Start();
                foreach (var product in products)
                {
                    await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, new List<Product>() { product }, async idxName =>
                    {
                        await CreateIndexInternalAsync(idxName);
                    });
                }
                stopwatch.Stop();
                Console.WriteLine("One at a time: " + stopwatch.Elapsed);

                await DeleteIndexInternalAsync(indexName);

                await CreateIndexInternalAsync(indexName);
                stopwatch = new Stopwatch();
                stopwatch.Start();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
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
                await DeleteIndexInternalAsync(indexName);
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
                products.Add(new Product() { RowKey = key, Brand = "Coke", ProductName = "Coke Classic", Sku = "123456" + key, Cost = 100 });
            }
            return products;
        }

        [TestMethod]
        public async Task SimultaneousRecordUpdate()
        {
            var exception = default(Exception);
            var indexName = Guid.NewGuid().ToString("N");
            try
            {
                await CreateIndexInternalAsync(indexName);
                var products = CreateProductList();
                await azureSearchEngine.MergeOrUploadItemsAsync<Product>(indexName, products, async idxName =>
                {
                    await CreateIndexInternalAsync(idxName);
                });

                const string expectedBrand = "UpdatedBrand";
                const decimal expectedCost = 1234.56m;
                const string expectedProductName = "UpdatedProductName";
                const string expectedSku = "Sku";

                var product1 = products.First();
                var update1 = new ProductBrandAndName()
                {
                    RowKey = product1.RowKey,
                    Brand = expectedBrand,
                    ProductName = expectedProductName
                };
                var update2 = new ProductSKUAndCost()
                {
                    RowKey = product1.RowKey,
                    Sku = expectedSku,
                    Cost = expectedCost
                };

                var u1 = azureSearchEngine.UpdateItemAtomicAsync<ProductBrandAndName>(indexName, update1);
                await azureSearchEngine.UpdateItemAtomicAsync<ProductSKUAndCost>(indexName, update2);
                await u1;

                await Task.Delay(5000);  // Azure Search says the indexing on their side could take some time.  Particularly on a shared search instance.
                var updatedDoc = await azureSearchEngine.GetDocumentById<Product>(indexName, product1.RowKey,
                    product =>
                    {
                        return new Product()
                        {
                            RowKey = product.RowKey,
                            Brand = product.Brand,
                            Cost = product.Cost,
                            ProductName = product.ProductName,
                            Sku = product.Sku
                        };
                    });
                Assert.IsNotNull(updatedDoc);
                Assert.AreEqual(expectedBrand, updatedDoc.Brand);
                Assert.AreEqual(expectedCost, updatedDoc.Cost);
                Assert.AreEqual(expectedProductName, updatedDoc.ProductName);
                Assert.AreEqual(expectedSku, updatedDoc.Sku);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                await DeleteIndexInternalAsync(indexName);
                if (default(Exception) != exception)
                    throw exception;
            }
        }
    }
}
