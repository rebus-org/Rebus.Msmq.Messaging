using NUnit.Framework;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.Msmq.Messaging.Tests.Contracts;

[TestFixture]
public class MsmqMessageExpiration : MessageExpiration<MsmqTransportFactory> { }