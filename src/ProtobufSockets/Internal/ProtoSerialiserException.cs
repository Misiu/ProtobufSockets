using System;

namespace ProtobufSockets.Internal
{
    internal class ProtoSerialiserException : Exception
    {
        public ProtoSerialiserException()
        {
        }

        public ProtoSerialiserException(Exception exception)
            : base("Protobuf error", exception)
        {
        }
    }
}