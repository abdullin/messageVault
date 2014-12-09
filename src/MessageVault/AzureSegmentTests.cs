using System;
using System.Text;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;

namespace MessageVault {


    public static class TestEnvironment {

        static TestEnvironment() {
            _account = CloudStorageAccount.DevelopmentStorageAccount;
            _client = _account.CreateCloudBlobClient();

        }
        static readonly CloudStorageAccount _account;
        static CloudBlobClient _client;

        static int _sequence = 0;

        public static CloudBlobContainer GetTestContainer(object caller) {
            var type = caller.GetType();

            var value = Interlocked.Increment(ref _sequence);
            var container = string.Format("{0}-{1:yyyy-MM-dd-hh-mm-ss}-{2}", type.Name.ToLowerInvariant(), DateTime.Now, value);
            
            var reference = _client.GetContainerReference(container);
            reference.CreateIfNotExists();
            return reference;
        }

    }

    public sealed class AzureSegmentTests {



        CloudBlobContainer _folder;
        AzureSegmentFactory _factory;

        [TestFixtureSetUp]
        public void Setup() {
            _folder = TestEnvironment.GetTestContainer(this);
            _factory = new AzureSegmentFactory(_folder);
            Logging.InitTrace();

        }
        [TestFixtureTearDown]
        public void Teardown() {
            _folder.Delete();
        }

        [Test]
        public void PageWrite() {
            var segment = _factory.OpenOrCreate("check1");

            var word = Encoding.UTF8.GetBytes("test-me");
            segment.Append(new[]{word});
            segment.Append(new[]{word});

            Assert.AreEqual(2*(word.Length + 4 ) ,segment.BlobPosition);

        }
    }

}