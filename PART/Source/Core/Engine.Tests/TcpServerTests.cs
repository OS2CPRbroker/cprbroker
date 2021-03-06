﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using CprBroker.Engine;
using System.Threading;
using System.Net;
using System.Net.Sockets;

namespace CprBroker.Tests.Engine
{
    namespace TcpServerTests
    {
        [TestFixture]
        public class Running
        {
            public class TcpServerStub : TcpServer
            {
                public TcpServerStub()
                {
                    this.InputMessageSize = Guid.NewGuid().ToString().Length;
                    this.TextEncoding = System.Text.Encoding.GetEncoding(1252);
                }
                public int Calls = 0;

                public override byte[] ProcessMessage(byte[] message)
                {
                    System.Threading.Interlocked.Increment(ref Calls);
                    return _ProcessMessage(message);
                }

                public Func<byte[], byte[]> _ProcessMessage = (msg) => msg;

                
            }

            public class Client
            {
                private TcpServer _Server;
                public Client(TcpServer server)
                {
                    _Server = server;
                }
                public string GetResponse(string msg)
                {
                    var enc = System.Text.Encoding.GetEncoding(1252);
                    byte[] inBytes = enc.GetBytes(msg);
                    using (var client = new TcpClient(_Server.Address, _Server.Port))
                    {
                        using (var stream = client.GetStream() as NetworkStream)
                        {
                            stream.Write(inBytes, 0, inBytes.Length);

                            var outBytes = new Byte[2880 + 28];

                            int bytes = stream.Read(outBytes, 0, outBytes.Length);
                            var ret = enc.GetString(outBytes, 0, bytes);
                            return ret;
                        }
                    }

                }
            }

            int NewPort()
            {
                return new Random().Next(1000, 10000);
            }

            [Test]
            public void Run_OneConnection_OK()
            {
                BrokerContext.Initialize(CprBroker.Utilities.Constants.BaseApplicationToken.ToString(), "");
                using (var server = new TcpServerStub() { Port = NewPort(), Address = "127.0.0.1" })
                {
                    server.Start();
                    var client = new Client(server);
                    var msg = "123";
                    var ret = client.GetResponse(msg);
                    Assert.AreEqual(msg, ret);
                }
            }

            public void Run_ManyConnections_OK(int count)
            {
                BrokerContext.Initialize(CprBroker.Utilities.Constants.BaseApplicationToken.ToString(), "");
                using (var server = new TcpServerStub() { Port = NewPort(), Address = "127.0.0.1" })
                {
                    server.Start();

                    long passed = 0;
                    var threads = new List<Thread>();
                    for (int i = 0; i < count; i++)
                    {
                        var th = new Thread(new ParameterizedThreadStart((p) =>
                        {
                            var client = new Client(server);
                            var msg = Guid.NewGuid().ToString();
                            var ret = client.GetResponse(msg);
                            Interlocked.Increment(ref passed);
                            Assert.AreEqual(msg, ret);
                        }));
                        threads.Add(th);
                    }
                    threads.ForEach(th => th.Start(threads.IndexOf(th)));
                    while (Interlocked.Read(ref passed) < count)
                    {
                        Thread.Sleep(100);
                    }
                    threads.ForEach(th => th.Abort());
                }
            }

            [Test]
            public void Run_ManyConnections_1_9_OK([Range(1, 9, 1)] int count)
            {
                Run_ManyConnections_OK(count);
            }

            [Test]
            public void Run_ManyConnections_10_90_OK([Range(10, 90, 10)] int count)
            {
                Run_ManyConnections_OK(count);
            }

            [Test]
            public void Run_ManyConnections_100_900_OK([Range(100, 900, 100)] int count)
            {
                Run_ManyConnections_OK(count);
            }

            [Ignore]
            [Test]
            public void Run_ManyConnections_1000_9000_OK([Range(1000, 9000, 1000)] int count)
            {
                Run_ManyConnections_OK(count);
            }

            [Ignore]
            [Test]
            public void Run_ManyConnections_10000_90000_OK([Range(10000, 90000, 10000)] int count)
            {
                Run_ManyConnections_OK(count);
            }

            [Test]
            [Ignore]
            public void CallTwice_SecondReturnsEmpty()
            {
                BrokerContext.Initialize(CprBroker.Utilities.Constants.BaseApplicationToken.ToString(), "");
                using (var server = new TcpServerStub() { Port = NewPort(), Address = "127.0.0.1",  })
                {
                    server.Start();

                    using (var client = new TcpClient(server.Address, server.Port))
                    {
                        using (NetworkStream stream = client.GetStream())
                        {
                            var inputData = server.TextEncoding.GetBytes(Guid.NewGuid().ToString());

                            var responses = new List<byte[]>();

                            for (int i = 0; i < 2; i++)
                            {
                                Console.WriteLine("Sending request: round # {0}", i + 1);

                                stream.Write(inputData, 0, inputData.Length);
                                stream.ReadTimeout = 3000;

                                var retData = new Byte[3500];
                                var retBytes = stream.Read(retData, 0, retData.Length);
                                retData = retData.Take(retBytes).ToArray();

                                responses.Add(retData);

                                var retText = Encoding.GetEncoding(1252).GetString(retData);
                                Console.WriteLine(retText);
                            }

                            Assert.IsEmpty(responses[1]);
                        }
                    }
                }
            }
        }
    }
}
