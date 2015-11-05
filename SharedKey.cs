using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace AzureQueueIssues
{
    public static class SharedKey
    {
        public static string Get(HttpWebRequest request, string accountName, byte[] accountKey, Dictionary<string, string> queryStringArgs, string contentLengthOverride = "")
        {
            var canonicalizedHeaders = request.Headers.AllKeys
                                                      .Where(k => k.StartsWith("x-ms-"))
                                                      .Select(k => k + ":" + request.Headers[k])	// no breaking whitespace to worry about in this operation
                                                      .OrderBy(kv => kv);

            var queryStrings = queryStringArgs.OrderBy(kvp => kvp.Key)
                                              .Select(kvp => kvp.Key + ":" + kvp.Value);

            string canonicalizedResource =
                "/" + accountName +
                String.Join("", request.RequestUri.Segments) + "\n" +
                String.Join("\n", queryStrings);

            var contentLength = (request.Headers["Content-Length"] != null ? request.Headers["Content-Length"] : contentLengthOverride);

            string stringToSign =
                request.Method + "\n"
                /* Content-Encoding */ + "\n"
                /* Content-Language */ + "\n"
                /* Content-Length */ + contentLength + "\n"
                /* Content-MD5 */ + "\n"
                /* Content-Type */ + "\n"
                /* Date */ + "\n"
                /* If-Modified-Since */ + "\n"
                /* If-Match */ + "\n"
                /* If-None-Match */ + "\n"
                /* If-Unmodified-Since */ + "\n"
                /* Range */ + "\n"
              + String.Join("\n", canonicalizedHeaders) + "\n"
              + canonicalizedResource;

            using (HashAlgorithm hashAlgorithm = new HMACSHA256(accountKey))
            {
                byte[] messageBuffer = Encoding.UTF8.GetBytes(stringToSign);
                return Convert.ToBase64String(hashAlgorithm.ComputeHash(messageBuffer));
            }
        }
    }
}
