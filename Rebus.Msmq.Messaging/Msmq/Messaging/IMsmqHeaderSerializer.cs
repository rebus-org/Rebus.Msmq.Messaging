using System.Collections.Generic;
using MSMQ.Messaging;

namespace Rebus.Msmq.Messaging;

/// <summary>
/// Interface for msmq header serialization
/// </summary>
public interface IMsmqHeaderSerializer
{
    /// <summary>
    /// Serialize header to msmq-message
    /// </summary>
    void SerializeToMessage(Dictionary<string, string> headers, Message msmqMessage);

    /// <summary>
    /// Deserialize header to key/value-pair.
    /// </summary>
    /// <param name="msmqMessage">msmq-message</param>
    /// <returns>Deserialized headers</returns>
    Dictionary<string, string> Deserialize(Message msmqMessage);
}