using System;
using System.Linq;
using System.Net;
using System.Threading;
using ProtobufSockets.Internal;
using ProtobufSockets.Stats;

namespace ProtobufSockets
{
    /// <summary>
    /// Subscriber for events, will be notified by Poblisher
    /// </summary>
    public class Subscriber : IDisposable
    {
        private const LogTag Tag = LogTag.Subscriber;

        private readonly object _reconnectSync = new object();
        private readonly object _typeSync = new object();
        private readonly object _statSync = new object();
        private readonly object _indexSync = new object();
        private readonly object _clientSync = new object();
        private readonly object _disposeSync = new object();

        private readonly string[] _statEndPoints;
        private readonly IPEndPoint[] _endPoints;
        private readonly string _name;

        private int _connected;
        private int _reconnect;
        private long _lastCount;
        private long _totalCount;
        private string _currentEndPoint;
        private string _statTopic;
        private string _statType;

        private bool _disposed;
        private int _indexEndPoint = -1;
        private long _beatCount = -1;
        private int _beatFailover;
        private Timer _reconnectTimer;

        private SubscriberClient _client;
        private string _topic;
        private Type _type;
        private Action<object> _action;
        private Action<IPEndPoint> _connectedAction;
        private readonly Timer _beatTimer;

        /// <summary>
        /// Initializes a new instance of the ProtobufSockets.Subscriber class with the specified Publisher endpoint address.
        /// </summary>
        /// <param name="endPoint">Endpoint of Publishers</param>
        /// <param name="name">Name of Subscriber</param>
        public Subscriber(IPEndPoint endPoint, string name = null):this(new []{endPoint}, name)
        {
        }

        /// <summary>
        /// Initializes a new instance of the ProtobufSockets.Subscriber class with the specified Publisher endpoint address.
        /// </summary>
        /// <param name="endPoints">Endpoints of Publishers</param>
        /// <param name="name">Name of Subscriber</param>
        public Subscriber(IPEndPoint[] endPoints, string name = null)
        {
            _endPoints = endPoints;
            _name = name;
            _statEndPoints = _endPoints.Select(e => e.ToString()).ToArray();

            _beatTimer = new Timer(_ => CheckBeat(), null, 10*1000, 10*1000);
        }

        /// <summary>
        /// Subscribes to all topics.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="action"></param>
        public void Subscribe<T>(Action<T> action)
        {
            Subscribe(null, action);
        }

        /// <summary>
        /// Subscribes to specific topic.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topic">Topic name</param>
        /// <param name="action"></param>
        /// <param name="connected"></param>
        public void Subscribe<T>(string topic, Action<T> action, Action<IPEndPoint> connected = null)
        {
            lock (_typeSync)
            {
                _type = typeof (T);
                _topic = topic;
                _action = m => action((T) m);
                _connectedAction = ep =>
                {
                    Interlocked.Exchange(ref _connected, 1);
                    if (connected != null) connected(ep);
                };

                Log.Info(Tag, "Subscribing to " + (topic ?? "<null>") + " as " + (_name ?? "<null>") + " with type " + _type.FullName);
            }

            Connect();
        }

        public void FailOver()
        {
            CloseClient();
        }

        public SubscriberStats GetStats(bool withSystemStats = false)
        {
            string currentEndPoint;
            string topic;
            string type;
            lock (_statSync)
            {
                currentEndPoint = _currentEndPoint;
                topic = _statTopic;
                type = _statType;
            }

            long count = 0;
            long beatCount = 0;
            long totalCount;
            lock (_clientSync)
            {
                if (_client != null)
                {
                    count = _client.GetMessageCount();
                    beatCount = _client.GetBeatCount();
                }

                if (count < _lastCount)
                    _lastCount = 0;

                _totalCount += count - _lastCount;

                _lastCount = count;

                totalCount = _totalCount;
            }

            var statsBuilder = new SystemStatsBuilder();
            if (withSystemStats)
                statsBuilder.ReadInFromCurrentProcess();

            return new SubscriberStats(Interlocked.CompareExchange(ref _connected, 0, 0) == 1,
                Interlocked.CompareExchange(ref _reconnect, 0, 0),
                Interlocked.CompareExchange(ref _beatFailover, 0, 0),
                count, beatCount, totalCount, currentEndPoint, _statEndPoints, topic, type, _name, statsBuilder.Build());
        }

        public void Dispose()
        {
            lock (_disposeSync) _disposed = true;
            _beatTimer.Dispose();
            CloseClient();
        }

        private void CheckBeat()
        {
            lock (_clientSync)
            {
                if (_client == null) return;
                var beatCount = _client.GetBeatCount();
                var current = Interlocked.Exchange(ref _beatCount, beatCount);

                if (current != beatCount) return;

                Log.Debug(Tag, "Lost the heart beat, will failover..");
                Interlocked.Increment(ref _beatFailover);
                FailOver();
            }
        }

        private void CloseClient()
        {
            Log.Debug(Tag, "Closing client.");
            Interlocked.Exchange(ref _connected, 0);
            lock (_clientSync)
            {
                if (_client != null)
                {
                    Log.Debug(Tag, "Disposing client.");
                    _client.Dispose();
                }
                _client = null;
            }
        }

        private void Connect()
        {
            lock (_disposeSync) if (_disposed) return;

            Log.Debug(Tag, "Connecting..");

            // copy mutables
            Type type;
            string topic;
            Action<object> action;
            Action<IPEndPoint> connectedAction;
            lock (_typeSync)
            {
                type = _type;
                topic = _topic;
                action = _action;
                connectedAction = _connectedAction;
            }

            // next endpoint
            int indexEndPoint;
            lock (_indexSync)
            {
                _indexEndPoint++;
                if (_indexEndPoint == _endPoints.Length)
                    _indexEndPoint = 0;
                indexEndPoint = _indexEndPoint;
            }
            var endPoint = _endPoints[indexEndPoint];

            Log.Info(Tag, "Connecting to " + endPoint);

            // Update stats
            lock (_statSync)
            {
                _currentEndPoint = _statEndPoints[indexEndPoint];
                _statType = type.FullName;
                _statTopic = topic;
            }

            CloseClient();

            var client = SubscriberClient.Connect(endPoint, _name, topic, type, action, Disconnected);

            if (client != null)
            {
                Log.Info(Tag, "Connection successful to " + endPoint);
                connectedAction(endPoint);
                lock (_clientSync)
                    _client = client;
            }
            else
            {
                Log.Info(Tag, "Connection unsuccessful for " + endPoint + ". Will reconnect..");
                Reconnect();
            }
        }

        private void Disconnected(IPEndPoint ep)
        {
            Log.Debug(Tag, "Disconnected from " + ep);
            Reconnect();
        }

        private void Reconnect()
        {
            if (!Monitor.TryEnter(_reconnectSync)) return; // do not queue up simultaneous calls
            try
            {
                Interlocked.Exchange(ref _connected, 0);
                Interlocked.Increment(ref _reconnect);

                if (_reconnectTimer != null)
                    _reconnectTimer.Dispose();

                Log.Debug(Tag, "Running reconnect timer..");
                _reconnectTimer = new Timer(_ => Connect(), null, 700, Timeout.Infinite);
            }
            finally
            {
                Monitor.Exit(_reconnectSync);
            }
        }
    }
}