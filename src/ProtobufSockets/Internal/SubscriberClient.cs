﻿using System;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.IO;

namespace ProtobufSockets.Internal
{
    internal class SubscriberClient
    {
        private const LogTag Tag = LogTag.SubscriberClient;

        private long _messageCount;
        private int _disposed;

        private readonly IPEndPoint _endPoint;
        private readonly TcpClient _tcpClient;
        private readonly Thread _consumerThread;
        private readonly NetworkStream _networkStream;
        private readonly Type _type;
        private readonly ProtoSerialiser _serialiser;
        private readonly Action<object> _action;
        private readonly Action<IPEndPoint> _disconnected;
        private long _beat;

        internal static SubscriberClient Connect(IPEndPoint endPoint, string name, string topic, Type type, Action<object> action, Action<IPEndPoint> disconnected)
        {
            Log.Debug(Tag, "Connecting to " + endPoint);
            try
            {
                var serialiser = new ProtoSerialiser();
                var tcpClient = new TcpClient {NoDelay = true, LingerState = {Enabled = true, LingerTime = 0}};
                tcpClient.Connect(endPoint);
                var networkStream = tcpClient.GetStream();

                // handshake
                serialiser.Serialise(networkStream, new Header {Topic = topic, Type = type.FullName, Name = name});
                var ack = serialiser.Deserialize<string>(networkStream);

                if (ack != "OK")
                {
                    return null;
                }

                Log.Debug(Tag, "Subscribing started.");

                return new SubscriberClient(endPoint, type, action, disconnected, tcpClient, networkStream, serialiser);
            }
            catch (InvalidOperationException)
            {
                Log.Debug(Tag, "Connect: InvalidOperationException");
            }
            catch (SocketException e)
            {
                Log.Debug(Tag, "Connect: SocketException: " + e.Message + " [SocketErrorCode:" + e.SocketErrorCode + "]");
            }
            catch (ProtoSerialiserException)
            {
                Log.Error(Tag, "Connect: ProtoSerialiserException");
            }
            catch (IOException e)
            {
                Log.Debug(Tag, "Connect: IOException: " + e.Message);
            }

            Log.Debug(Tag, "Error connecting to " + endPoint);

            return null;
        }

        private SubscriberClient(IPEndPoint endPoint, Type type, Action<object> action, Action<IPEndPoint> disconnected, TcpClient tcpClient, NetworkStream networkStream, ProtoSerialiser serialiser)
        {
            _endPoint = endPoint;
            _type = type;
            _action = action;
            _disconnected = disconnected;
            _tcpClient = tcpClient;
            _networkStream = networkStream;
            _serialiser = serialiser;

            _consumerThread = new Thread(Consume) {IsBackground = true};
            _consumerThread.Start();
        }

        internal long GetMessageCount()
        {
            return Interlocked.CompareExchange(ref _messageCount, 0, 0);
        }

        internal long GetBeatCount()
        {
            return Interlocked.CompareExchange(ref _beat, 0, 0);
        }

        internal void Dispose()
        {
            if (Interlocked.CompareExchange(ref _disposed, 0, 0) == 1) return;

            Interlocked.Exchange(ref _disposed, 1);
            _tcpClient.Close();
            _disconnected(_endPoint);
        }

        private void Consume()
        {
            Log.Info(Tag, "Consumer started [" + Thread.CurrentThread.ManagedThreadId + "]");

            while (Interlocked.CompareExchange(ref _disposed, 0, 0) == 0)
            {
                try
                {
                    var header = _serialiser.Deserialize<Header>(_networkStream);

                    Log.Debug(Tag,
                        "Received header [name=" + (header.Name ?? "<null>") + " type=" + (header.Type ?? "<null>") +
                        " topic=" + (header.Topic ?? "<null>") + "]");

                    // Beat-shake
                    if (header.Type == typeof (Beat).FullName)
                    {
                        var beat = _serialiser.Deserialize<Beat>(_networkStream);
                        _serialiser.Serialise(_networkStream, beat);
                        Log.Debug(Tag, "Heartbeat from publisher");

                        Interlocked.Increment(ref _beat);
                        continue;
                    }

                    var message = _serialiser.Deserialize(_networkStream, _type);

                    Log.Debug(Tag, "Received message.");

                    Interlocked.Increment(ref _messageCount);

                    _action(message);
                }
                catch (SocketException e)
                {
                    Log.Debug(Tag, "Consume: SocketException: " + e.Message + " [SocketErrorCode:" + e.SocketErrorCode + "]");
                    break;
                }
                catch (IOException e)
                {
                    Log.Debug(Tag, "Consume: IOException: " + e.Message);
                    break;
                }
                catch (ProtoSerialiserException)
                {
                    Log.Error(Tag, "Consume: ProtoSerialiserException");
                    break;
                }
                catch (ObjectDisposedException)
                {
                    Log.Debug(Tag, "Connect: ObjectDisposedException");
                    break;
                }
            }

            Dispose();

            Log.Info(Tag, "Consumer exiting [" + Thread.CurrentThread.ManagedThreadId + "]");
        }
    }
}