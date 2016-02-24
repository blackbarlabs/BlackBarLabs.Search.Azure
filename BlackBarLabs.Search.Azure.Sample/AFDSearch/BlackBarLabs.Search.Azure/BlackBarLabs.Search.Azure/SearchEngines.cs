using System;
using Microsoft.Azure.Search;

namespace BlackBarLabs.Search.Azure
{
    public class SearchEngines
    {
        private readonly string azureSearchServiceName;
        private readonly string azureSearchServiceApiKey;

        private static SearchServiceClient searchClient;
        private AzureSearchEngine azureSearchEngine;

        public SearchEngines(string azureSearchServiceName, string azureSearchServiceApiKey)
        {
            if (string.IsNullOrEmpty(azureSearchServiceName) || string.IsNullOrEmpty(azureSearchServiceApiKey))
                throw new ArgumentException("Cannot create Azure Search context without Azure Search name or key settings.  Check configuration.");
            this.azureSearchServiceName = azureSearchServiceName;
            this.azureSearchServiceApiKey = azureSearchServiceApiKey;
        }

        private static readonly object AzureSearchEngineLock = new object();
        public AzureSearchEngine AzureSearchEngine
        {
            get
            {
                if (azureSearchEngine != null) return azureSearchEngine;

                lock (AzureSearchEngineLock)
                    if (azureSearchEngine == null)
                    {
                        var serviceName = System.Configuration.ConfigurationManager.AppSettings[azureSearchServiceName];
                        var serviceApiKey = System.Configuration.ConfigurationManager.AppSettings[azureSearchServiceApiKey];
                        searchClient = new SearchServiceClient(serviceName, new SearchCredentials(serviceApiKey));
                        azureSearchEngine = new AzureSearchEngine(searchClient);
                    }

                return azureSearchEngine;
            }
            private set { azureSearchEngine = value; }
        }

      
    }
}

