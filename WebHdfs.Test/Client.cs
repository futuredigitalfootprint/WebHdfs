using System;
using System.IO;
using System.Linq.Expressions;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Xunit;

namespace WebHdfs.Test
{
	public class Integration
	{
		private const string BASE_URL = "http://test.me/plz/";
		private const string USER = "hdfs";
		private const string TEST_FILE = "test.txt";
		private const string BOOL_RESULT = "{ \"boolean\" : true }";

		[Fact]
		public async Task GetStatus()
		{
			const string path = "/path/to/file";
			await CallClient(c => c.GetFileStatus(path), HttpMethod.Get, path, "GETFILESTATUS");
		}

		[Fact]
		public async Task CreateDirectory()
		{
			const string path = "/path/to/file";
			await CallClient(c => c.CreateDirectory(path), HttpMethod.Put, path, "MKDIRS");
		}
		
		[Fact]
		public async Task CreateFile()
		{
			if (!File.Exists(TEST_FILE))
				using (File.Create(TEST_FILE)){ }
			const string path = "/path/to/file";
			await CallClient(c => c.CreateFile(TEST_FILE, path), HttpMethod.Put, path, "CREATE");
			await CallClient(c => c.CreateFile(TEST_FILE, path, CancellationToken.None), HttpMethod.Put, path, "CREATE");
			await CallClient(c => c.CreateFile(File.OpenRead(TEST_FILE), path), HttpMethod.Put, path, "CREATE");
			await CallClient(c => c.CreateFile(File.OpenRead(TEST_FILE), path, CancellationToken.None), HttpMethod.Put, path, "CREATE");
		}

		[Fact]
		public async Task DeleteDirectory()
		{
			const string path = "/path/to/file";
			await CallClient(c => c.DeleteDirectory(path), HttpMethod.Delete, path, "DELETE");
		}

		[Fact]
		public async Task GetContentSummary()
		{
			const string path = "/path/to/file";
			await CallClient(c => c.GetContentSummary(path), HttpMethod.Get, path, "GETCONTENTSUMMARY");
		}

		[Fact]
		public async Task GetDirectoryStatus()
		{
			const string path = "/path/to/file";
			await CallClient(c => c.GetDirectoryStatus(path), HttpMethod.Get, path, "LISTSTATUS", "{\"FileStatuses\":{\"FileStatus\":[{ \"accessTime\":0,\"blockSize\":0,\"childrenNum\":4,\"fileId\":308665,\"group\":\"hdfs\",\"length\":0,\"modificationTime\":1429776977330,\"owner\":\"hdfs\",\"pathSuffix\":\"hdfs\",\"permission\":\"755\",\"replication\":0,\"type\":\"DIRECTORY\"}]}}");
		}

		[Fact]
		public async Task GetFileChecksum()
		{
			const string path = "/path/to/file";
			await CallClient(c => c.GetFileChecksum(path), HttpMethod.Get, path, "GETFILECHECKSUM");
		}

		[Fact]
		public async Task GetFileStatus()
		{
			const string path = "/path/to/file";
			await CallClient(c => c.GetFileStatus(path), HttpMethod.Get, path, "GETFILESTATUS");
		}

		[Fact]
		public async Task GetHomeDirectory()
		{
			const string path = "/";
			await CallClient(c => c.GetHomeDirectory(), HttpMethod.Get, path, "GETHOMEDIRECTORY");
		}

		[Fact]
		public async Task OpenFile()
		{
			const string path = "/path/to/file";
			await CallClient(c => c.OpenFile(path, CancellationToken.None), HttpMethod.Get, path, "OPEN");
			await CallClient(c => c.OpenFile(path, CancellationToken.None), HttpMethod.Get, path, "OPEN");
		}

		[Fact]
		public async Task RenameDirectory()
		{
			const string path = "/path/to/file";
			const string newPath = path + "-new";
			await CallClient(c => c.RenameDirectory(path, newPath), HttpMethod.Put, path, "RENAME&destination=" + newPath, BOOL_RESULT);
		}

		[Fact]
		public async Task SetAccessTime()
		{
			const string path = "/path/to/file";
			const string time = "123";
			await CallClient(c => c.SetAccessTime(path, time), HttpMethod.Put, path, "SETTIMES&accesstime=" + time, BOOL_RESULT);
		}

		[Fact]
		public async Task SetGroup()
		{
			const string path = "/path/to/file";
			const string param = "123";
			await CallClient(c => c.SetGroup(path, param), HttpMethod.Put, path, "SETOWNER&group=" + param, BOOL_RESULT);
		}

		[Fact]
		public async Task SetModificationTime()
		{
			const string path = "/path/to/file";
			const string param = "123";
			await CallClient(c => c.SetModificationTime(path, param), HttpMethod.Put, path, "SETTIMES&modificationtime=" + param, BOOL_RESULT);
		}

		[Fact]
		public async Task SetOwner()
		{
			const string path = "/path/to/file";
			const string param = "123";
			await CallClient(c => c.SetOwner(path, param), HttpMethod.Put, path, "SETOWNER&owner=" + param, BOOL_RESULT);
		}

		[Fact]
		public async Task SetPermissions()
		{
			const string path = "/path/to/file";
			const string param = "123";
			await CallClient(c => c.SetPermissions(path, param), HttpMethod.Put, path, "SETPERMISSION&permission=" + param, BOOL_RESULT);
		}

		[Fact]
		public async Task SetReplicationFactor()
		{
			const string path = "/path/to/file";
			const int param = 100;
			await CallClient(c => c.SetReplicationFactor(path, param), HttpMethod.Put, path, "SETREPLICATION&replication=" + param, BOOL_RESULT);
		}

		[Fact]
		public async Task GetEmptyResult()
		{
			const string path = "/path/to/file";
			await CallClient(async c =>
				{
					var file = await c.GetFileStatus(path);
					Assert.Null(file);
					return file;
				}, HttpMethod.Get, path, "GETFILESTATUS", status: HttpStatusCode.NotFound);
		}

		private static async Task<TResult> CallClient<TResult>(Func<WebHdfsClient, Task<TResult>> caller, HttpMethod method, string url, string operation, string result = "{}", HttpStatusCode status = HttpStatusCode.OK)
		{
			var handler = new Mock<FakeHttpMessageHandler> { CallBase = true };

			Expression<Func<FakeHttpMessageHandler, HttpResponseMessage>> homeCall = t => t.Send(It.Is<HttpRequestMessage>(
							msg =>
							   msg.Method == HttpMethod.Get &&
							   msg.RequestUri.ToString() == "http://test.me/plz/webhdfs/v1/?user.name=hdfs&op=GETHOMEDIRECTORY"));

			handler.Setup(homeCall)
				   .Returns(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"Path\":\"/user/hdfs\"}", System.Text.Encoding.UTF8, "application/json") })
				   .Verifiable();


			if (!operation.StartsWith("GETHOMEDIRECTORY", StringComparison.OrdinalIgnoreCase))
			{
				Expression<Func<FakeHttpMessageHandler, HttpResponseMessage>> innerCall = t => t.Send(It.Is<HttpRequestMessage>(
					msg =>
						msg.Method == method &&
						msg.RequestUri.ToString().StartsWith(BASE_URL + WebHdfsClient.PREFIX + url + "?user.name=" + USER + "&op=" + operation, StringComparison.OrdinalIgnoreCase)));

				handler.Setup(innerCall)
						.Returns(new HttpResponseMessage(status) { Content = new StringContent(result, System.Text.Encoding.UTF8, "application/json") })
						.Verifiable();
			}

			var client = new WebHdfsClient(handler.Object, BASE_URL, USER);
			var response = await caller(client);
			handler.Verify();
			return response;
		}

		public class FakeHttpMessageHandler : HttpMessageHandler
		{
			public virtual HttpResponseMessage Send(HttpRequestMessage request)
			{
				return new HttpResponseMessage(HttpStatusCode.NoContent);
			}

			protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
			{
				return Task.FromResult(Send(request));
			}
		}
	}
}
