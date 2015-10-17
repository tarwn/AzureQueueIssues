using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace AzureQueueIssues
{
	[TestFixture]
	public class PopReceiptMismatchReturnsWrongError
	{
		const int ErrorCode_PopReceiptMismatch = 400;
		const string Error_PopReceiptMismatchMessage = "The specified pop receipt did not match the pop receipt for a dequeued message.";
		const string Code_PopReceiptMismatchMessage = "PopReceiptMismatch";
		const string Code_InvalidQueryParameterValue = "InvalidQueryParameterValue";

        //const string TargetApiVersion = "2012-02-12";
        const string TargetApiVersion = "2014-02-14";

		const HttpStatusCode Status_SuccessfulUpdate = HttpStatusCode.NoContent;

		private CloudStorageAccount _account;

		[SetUp]
		public void Setup()
		{
			//var devStorage = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials("ACCOUNTNAME", "ACCOUNTKEY");
			//_account = new CloudStorageAccount(devStorage, true);
			_account = CloudStorageAccount.DevelopmentStorageAccount;
		}

		/// <summary>
		/// According to the documentation, attempting to update an invisible queueitem with the wrong
		/// pop receipt will return a 400 error with the code 'PopReceiptMismatch' which is described
		/// as 'The specified pop receipt did not match the pop receipt for a dequeued message.'
		/// </summary>
		/// <remarks>
		/// Source: http://msdn.microsoft.com/en-us/library/windowsazure/dd179446.aspx
		/// 
		/// The expectation that this error would be this particular 400 is reflected by discussions
		/// with Azure support and other documents in MSDN 
		/// 
		/// - Feb 2011 (2 API versions ago): http://msdn.microsoft.com/en-us/library/dd179347.aspx
		/// 
		/// - Newer documents don't specify what error will be sent for 'mismatched pop receipt' 
		/// (http://msdn.microsoft.com/en-us/library/windowsazure/hh452234.aspx), but show that 
		/// is the correct terminology
		/// 
		/// - An MSDN forum thread verified that a 404 comes back, but indicated this was a flaw in 
		/// the documentation (http://social.msdn.microsoft.com/Forums/windowsazure/en-US/aab37e27-2f04-47db-9e1d-66fd224ac925/handling-queue-message-deletion-error).
		/// The answer here is that you will get a 404 if either the item doens't exist or the popreceipt 
		/// doesn't match and will only receive a popreceipt mismatch if you use the receipt from a different 
		/// item. 
		/// Which doesn't seem like a defensible design decision to me if it is the case.
		/// </remarks>
		[Test]
		public void UpdateMessage_UsingIncorrectPopReceipt_Returns400PopReceiptMismatch()
		{
			// Create the queue client
			var queueClient = _account.CreateCloudQueueClient();

			// Retrieve a reference to a queue
			var queue = queueClient.GetQueueReference("unit-test" + Guid.NewGuid().ToString());
			queue.CreateIfNotExists();
			queue.Clear();

			// let's queue up a sample message
			var message = new CloudQueueMessage("test content");
			// note the unecessary difference in terminology between API (Put) and reference SDK (Add)
			queue.AddMessage(message);

			// now we get the item and violate the first Get's popreceipt
			var queueMessage1 = queue.GetMessage(TimeSpan.FromSeconds(1));
			Thread.Sleep(TimeSpan.FromSeconds(2));
			var queueMessage2 = queue.GetMessage(TimeSpan.FromSeconds(60));
			// and just to be absolutely clear that we didn't get the same receipt a second time
			Assert.AreNotEqual(queueMessage1.PopReceipt, queueMessage2.PopReceipt);

			// now lets harvest the error from using the first popreceipt
			int statusCode = -1;
			string status = "not defined";
			try
			{
				queue.UpdateMessage(queueMessage1, TimeSpan.FromSeconds(60), Microsoft.WindowsAzure.Storage.Queue.MessageUpdateFields.Visibility);
			}
			catch (StorageException exc)
			{
				statusCode = exc.RequestInformation.HttpStatusCode;
				status = exc.RequestInformation.HttpStatusMessage;
			}

			// prove that the item is still valid and it was definately a popreceipt mismatch
			queue.UpdateMessage(queueMessage2, TimeSpan.FromSeconds(60), Microsoft.WindowsAzure.Storage.Queue.MessageUpdateFields.Visibility);

			//cleanup
			queue.Delete();

			// documented response
			Assert.AreEqual(Error_PopReceiptMismatchMessage, status);
			Assert.AreEqual(ErrorCode_PopReceiptMismatch, statusCode);
		}

		/// <summary>
		/// One succesful test to show my manual REST update works properly
		/// </summary>
		[Test]
		public void UpdateMessage_UsingHandWrittenCode_Returns204NoContentOnSuccesfulUpdate()
		{
			string queueName = "unit-test-" + Guid.NewGuid().ToString();
			var queueClient = _account.CreateCloudQueueClient();

			// Retrieve a reference to a queue
			var queue = queueClient.GetQueueReference(queueName);
			queue.CreateIfNotExists();
			queue.Clear();

			// let's queue up a sample message
			var message = new CloudQueueMessage("test content");
			// note the unecessary difference in terminology between API (Put) and reference SDK (Add)
			queue.AddMessage(message);

			// now we get the item
			var queueMessage = queue.GetMessage(TimeSpan.FromSeconds(60));

			// prove I know what I'm doing
			var status = HttpStatusCode.NotImplemented;
			try
			{
				var request = GenerateRequestForUpdate(_account, queueName, queueMessage.Id, queueMessage.PopReceipt, 60);
				using (var response = (HttpWebResponse)request.GetResponse())
				{
					status = response.StatusCode;
				}
			}
			finally
			{
				//cleanup
				queue.Delete();
			}

			Assert.AreEqual(Status_SuccessfulUpdate, status);
		}

		/// <summary>
		/// The MSDN post referenced above attempted to say that the PopReceipt error code is only for misformatted popreceipts,
		/// but the correct code for that case is InvalidQueryParameter. This test reflects that that is the case and that 
		/// this scenario does not result in the PopReceiptMismatch error
		/// </summary>
		[Test]
		public void UpdateMessage_UsingIncorrectPopReceiptFormat_ReturnsInvalidParameterAndNotPopReceiptMismatch()
		{
			string queueName = "unit-test-" + Guid.NewGuid().ToString();
			var queueClient = _account.CreateCloudQueueClient();

			// Retrieve a reference to a queue
			var queue = queueClient.GetQueueReference(queueName);
			queue.CreateIfNotExists();
			queue.Clear();

			// let's queue up a sample message
			var message = new CloudQueueMessage("test content");
			// note the unecessary difference in terminology between API (Put) and reference SDK (Add)
			queue.AddMessage(message);

			// now we get the item
			var queueMessage = queue.GetMessage(TimeSpan.FromSeconds(60));

			// and then make a request with a totally invalid receipt
			int statusCode = -1;
			string status = "not defined";
			try
			{
				var request = GenerateRequestForUpdate(_account, queueName, queueMessage.Id, "NotEvenCloseToCorrectFormat", 60);
				using (var response = (HttpWebResponse)request.GetResponse())
				{
					Assert.Fail("Successful update, not what we were expecting");
				}
			}
			catch (WebException exc)
			{
				statusCode = (int)exc.Status;
				using (var stream = exc.Response.GetResponseStream())
				{
					var xml = new StreamReader(stream).ReadToEnd();
					var doc = XDocument.Parse(xml);
					status = doc.Root.Elements().Where(e => e.Name.LocalName == "Code")
												.Select(e => e.Value)
												.FirstOrDefault();
				}
			}

			//cleanup
			queue.Delete();

			// The MSDN forum post indicated this was the proper time to get a PopReceiptMismatch (it's not)
			Assert.AreNotEqual(Code_PopReceiptMismatchMessage, status);
			Assert.AreNotEqual(ErrorCode_PopReceiptMismatch, statusCode);
			Assert.AreEqual(Code_InvalidQueryParameterValue, status);
		}

		/// <summary>
		/// The one case the MSDN forum thread stated would result in the PopReceiptMismatch was an example where a popreceipt
		/// was used across two queues, so we'll add a test for that too
		/// </summary>
		[Test]
		public void UpdateMessage_UsingPopReceiptFromDifferentQueuesMessage_Returns400PopReceiptMismatch()
		{
			// Create the queue client
			var queueClient = _account.CreateCloudQueueClient();

			// Retrieve a reference to a queue
			var queueName = "unit-test" + Guid.NewGuid().ToString();
			var queue = queueClient.GetQueueReference(queueName);
			queue.CreateIfNotExists();
			queue.Clear();

			var queue2 = queueClient.GetQueueReference("unit-test2" + Guid.NewGuid().ToString());
			queue2.CreateIfNotExists();
			queue2.Clear();

			// let's queue up sample messages
			queue.AddMessage(new CloudQueueMessage("test content"));
			queue2.AddMessage(new CloudQueueMessage("test content"));

			var queueMessage1 = queue.GetMessage(TimeSpan.FromSeconds(60));
			var queueMessage2 = queue2.GetMessage(TimeSpan.FromSeconds(60));

			// and then make a request with a totally invalid receipt
			int statusCode = -1;
			string status = "not defined";
			try
			{
				var request = GenerateRequestForUpdate(_account, queueName, queueMessage1.Id, queueMessage2.PopReceipt, 60);
				using (var response = (HttpWebResponse)request.GetResponse())
				{
					Assert.Fail("Successful update, not what we were expecting");
				}
			}
			catch (WebException exc)
			{
				statusCode = (int)exc.Status;
				using (var stream = exc.Response.GetResponseStream())
				{
					var xml = new StreamReader(stream).ReadToEnd();
					var doc = XDocument.Parse(xml);
					status = doc.Root.Elements().Where(e => e.Name.LocalName == "Code")
												.Select(e => e.Value)
												.FirstOrDefault();
				}
			}

			//cleanup
			queue.Delete();
			queue2.Delete();

			// Did we get a PopReceiptMismatch finally?
			Assert.AreEqual(Code_PopReceiptMismatchMessage, status);
			Assert.AreEqual(ErrorCode_PopReceiptMismatch, statusCode);
		}


		/// <summary>
		/// And in case the MSDN thread was trying to test a valid formatted but never existed pop receipt, how about one more
		/// </summary>
		[Test]
		public void UpdateMessage_UsingMadeUpButValidFormatPopReceipt_Returns400PopReceiptMismatch()
		{
			// Create the queue client
			var queueClient = _account.CreateCloudQueueClient();

			// Retrieve a reference to a queue
			var queueName = "unit-test" + Guid.NewGuid().ToString();
			var queue = queueClient.GetQueueReference(queueName);
			queue.CreateIfNotExists();
			queue.Clear();

			// let's queue up sample messages
			queue.AddMessage(new CloudQueueMessage("test content"));
			var queueMessage = queue.GetMessage(TimeSpan.FromSeconds(60));

			// and then make a request with a totally invalid receipt
			int statusCode = -1;
			string status = "not defined";
			try
			{
				var fakePopReceipt = queueMessage.PopReceipt;
				fakePopReceipt = fakePopReceipt.Replace(fakePopReceipt.Last(), fakePopReceipt.Last() != 'A' ? 'A' : 'B');
				var request = GenerateRequestForUpdate(_account, queueName, queueMessage.Id, fakePopReceipt, 60);
				using (var response = (HttpWebResponse)request.GetResponse())
				{
					Assert.Fail("Successful update, not what we were expecting");
				}
			}
			catch (WebException exc)
			{
				statusCode = (int)exc.Status;
				using (var stream = exc.Response.GetResponseStream())
				{
					var xml = new StreamReader(stream).ReadToEnd();
					var doc = XDocument.Parse(xml);
					status = doc.Root.Elements().Where(e => e.Name.LocalName == "Code")
												.Select(e => e.Value)
												.FirstOrDefault();
				}
			}

			//cleanup
			queue.Delete();

			// Did we get a PopReceiptMismatch finally?
			Assert.AreEqual(Code_PopReceiptMismatchMessage, status);
			Assert.AreEqual(ErrorCode_PopReceiptMismatch, statusCode);
		}

		private HttpWebRequest GenerateRequestForUpdate(CloudStorageAccount account, string queueName, string messageId, string popReceipt, int visibilityTimeout)
		{
			
			string url = String.Format("{0}/{1}/messages/{2}?popreceipt={3}&visibilitytimeout={4}",
				account.QueueEndpoint.AbsoluteUri.ToString().TrimEnd('/'),
				queueName,
				Uri.EscapeDataString(messageId),
				Uri.EscapeDataString(popReceipt),
				visibilityTimeout);

			var queryStringValues = new Dictionary<string, string>() { 
				{"popreceipt",popReceipt},
				{"visibilitytimeout",visibilityTimeout.ToString()}
			};

			var request = HttpWebRequest.CreateHttp(url);
			request.Method = "PUT";
			request.ContentLength = 0;
			request.Headers.Add("x-ms-version", TargetApiVersion);
			request.Headers.Add("x-ms-date", DateTime.UtcNow.ToString("R", CultureInfo.InvariantCulture));
			request.Headers.Add("Authorization", "SharedKey " + account.Credentials.AccountName + ":"
				+ SharedKeyFor(request, account.Credentials.AccountName, account.Credentials.ExportKey(), queueName, messageId, queryStringValues));
			return request;
		}

		private string SharedKeyFor(HttpWebRequest request, string accountName, byte[] accountKey, string queueName, string messageId, Dictionary<string, string> queryStringArgs)
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

			string stringToSign =
				request.Method + "\n"
				/* Content-Encoding */ + "\n"
				/* Content-Language */ + "\n"
				/* Content-Length */ + "0\n"
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
