using Microsoft.Azure.Search.Models;

namespace BlackBarLabs.Search.Azure.Tests
{
    public static class ProductFieldInfo
    {
        public static SearchFieldInfo[] SearchFields => new[]
        {
            new SearchFieldInfo { Name = "RowKey",        Type = typeof(string),     IsKey = true,  IsSearchable = false, IsFilterable = false, IsSortable = false, IsFacetable = false, IsRetrievable = true},
            new SearchFieldInfo { Name = "Brand",         Type = typeof(string),     IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true},
            new SearchFieldInfo { Name = "ProductName",   Type = typeof(string),     IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false, IsRetrievable = true},
            new SearchFieldInfo { Name = "Sku",           Type = typeof(string),     IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false, IsRetrievable = true},
            new SearchFieldInfo { Name = "Cost",          Type = typeof(double),    IsKey = false, IsSearchable = false, IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true}
        };                                                     
    }
}
