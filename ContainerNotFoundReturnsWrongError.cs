using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace AzureQueueIssues
{
    public class ContainerNotFoundReturnsWrongError
    {
        const int ErrorCode_NotFound = 404;
        const string ErrorStatus_ContainerNotFound = "The specified container does not exist.";

        const string TargetApiVersion = "2012-02-12";
        //const string TargetApiVersion = "2014-02-14";

        private CloudStorageAccount _account;

        [SetUp]
        public void Setup()
        {
            //var devStorage = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials("ACCOUNTNAME", "ACCOUNTKEY");
            //_account = new CloudStorageAccount(devStorage, true);
            _account = CloudStorageAccount.DevelopmentStorageAccount;
        }

        private HttpWebRequest GenerateRequestForGetProperties(CloudStorageAccount account, string containerName)
        {

            string url = String.Format("{0}/{1}?restype=container",
                account.BlobEndpoint.AbsoluteUri.ToString().TrimEnd('/'),
                containerName);

            var queryStringValues = new Dictionary<string, string>() { 
				{"restype", "container"}
			};

            var request = HttpWebRequest.CreateHttp(url);
            request.Method = "GET";
            request.Headers.Add("x-ms-version", TargetApiVersion);
            request.Headers.Add("x-ms-date", DateTime.UtcNow.ToString("R", CultureInfo.InvariantCulture));
            request.Headers.Add("Authorization", "SharedKey " + account.Credentials.AccountName + ":"
                + SharedKey.Get(request, account.Credentials.AccountName, account.Credentials.ExportKey(), queryStringValues));
            return request;
        }

        /// <summary>
        /// The documentation doesn't explicitly state what error message we should receive when
        /// we attempt an operation on a non-existent container, but the list of error codes
        /// contains seperate errors for Container Not Found and Blob Not Found, so 
        /// it seems reasonable that we would receive a Container Not Found error.
        /// 
        /// https://msdn.microsoft.com/en-us/library/azure/dd179439.aspx
        /// 
        /// Emulator: Returns 404 Container Not Found (tested with 3.3 and other versions)
        /// Azure API: Returns 404 Blob Not Found (tested with multiple versions)
        /// 
        /// In this case, the Emulator seems correct and the API incorrect.
        /// 
        ///     (And let's not even talk about why the SDK team thought it was a good 
        ///      idea to use a function named FetchAttributes to call the Get Properties
        ///      api method.)
        ///      
        /// </summary>
        [Test]
        public void GetProperties_NonExistentContainer_ReturnsContainerNotFoundError()
        {
            var blobClient = _account.CreateCloudBlobClient();
            var containerReference = blobClient.GetContainerReference("nonexistent-container");
            //verify for sure that it doesn't exist
            if (containerReference.Exists())
                throw new Exception("Invalid test - the container name exists ahead of time");

            int statusCode = -1;
            string status = "not defined";
            try
            {
                containerReference.FetchAttributes();
            }
            catch(StorageException exc)
            {
                statusCode = exc.RequestInformation.HttpStatusCode;
                status = exc.RequestInformation.HttpStatusMessage;
            }

            Assert.AreEqual(ErrorCode_NotFound, statusCode);
            Assert.AreEqual(ErrorStatus_ContainerNotFound, status);
        }

        [Test]
        public void GetPropertiesManually_NonExistentContainer_ReturnsContainerNotFoundError()
        {
            var blobClient = _account.CreateCloudBlobClient();
            string containerName = "nonexistent-container";
            var containerReference = blobClient.GetContainerReference(containerName);
            //verify for sure that it doesn't exist
            if (containerReference.Exists())
                throw new Exception("Invalid test - the container name exists ahead of time");

            int statusCode = -1;
            string status = "not defined";
            try
            {
                var request = GenerateRequestForGetProperties(_account, containerName);
                using (var response = (HttpWebResponse)request.GetResponse())
                {
                    statusCode = (int)response.StatusCode;
                    status = response.StatusDescription;
                }
            }
            catch (WebException wexc)
            {
                var response = (HttpWebResponse)wexc.Response;
                statusCode = (int) response.StatusCode;
                status = response.StatusDescription;
            }

            Assert.AreEqual(ErrorCode_NotFound, statusCode);
            Assert.AreEqual(ErrorStatus_ContainerNotFound, status);
        }
    }
}
