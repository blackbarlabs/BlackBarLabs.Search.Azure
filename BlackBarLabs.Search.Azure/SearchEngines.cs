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
                        azureSearchEngine = EastFive.Web.Configuration.Settings.GetString(azureSearchServiceName,
                            (serviceName) =>
                            {
                                return EastFive.Web.Configuration.Settings.GetString(azureSearchServiceApiKey,
                                    (serviceApiKey) =>
                                    {
                                        searchClient = new SearchServiceClient(serviceName, new SearchCredentials(serviceApiKey));
                                        return new AzureSearchEngine(searchClient);
                                    },
                                    (why) => { throw new Exception(why); });
                            },
                            (why) => { throw new Exception(why); });
                    }

                return azureSearchEngine;
            }
            private set { azureSearchEngine = value; }
        }

      
    }
}

