using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;

namespace ProtobufSockets.Internal
{
    internal class PublisherSubscriptionStore
    {
        private readonly ConcurrentDictionary<Socket, PublisherClient> _cs = new ConcurrentDictionary<Socket, PublisherClient>();

        internal IEnumerable<PublisherClient> Subscriptions
        {
            get { return _cs.Values; }
        }

        internal void Remove(Socket socket)
        {
            if (socket == null) return;

            PublisherClient _;
            _cs.TryRemove(socket, out _);
        }

        internal void Add(Socket socket, PublisherClient client)
        {
            _cs.TryAdd(socket, client);
        }
    }
}