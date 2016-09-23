namespace BlackBarLabs.Search.Azure.Tests
{
    public class Product
    {
        public string RowKey { get; set; }

        public string Brand { get; set; }

        public string ProductName { get; set; }

        public string Sku { get; set; }

        public decimal Cost { get; set; }
    }

    public class ProductWithAddedFields
    {
        public string RowKey { get; set; }

        public string Brand { get; set; }

        public string ProductName { get; set; }

        public string Sku { get; set; }

        public decimal Cost { get; set; }

        public string AddedField1 { get; set; }
        
        public string AddedField2 { get; set; }
    }

    public class ProductBrandAndName
    {
        public string RowKey { get; set; }

        public string Brand { get; set; }

        public string ProductName { get; set; }
    }

    public class ProductSKUAndCost
    {
        public string RowKey { get; set; }

        public string Sku { get; set; }

        public decimal Cost { get; set; }
    }
}