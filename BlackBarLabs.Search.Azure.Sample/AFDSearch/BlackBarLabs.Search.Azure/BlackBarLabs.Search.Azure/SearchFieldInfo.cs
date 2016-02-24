
namespace BlackBarLabs.Search.Azure
{
    public class SearchFieldInfo
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public bool IsKey { get; set; }
        public bool IsSearchable { get; set; }
        public bool IsFilterable { get; set; }
        public bool IsSortable { get; set; }
        public bool IsFacetable { get; set; }
        public bool IsRetrievable { get; set; }
    }
}
