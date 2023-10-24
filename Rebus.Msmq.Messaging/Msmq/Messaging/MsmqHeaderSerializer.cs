using System;
using System.Collections.Generic;
using System.Text;
using MSMQ.Messaging;
using Rebus.Serialization;

namespace Rebus.Msmq.Messaging;

/// <summary>
/// Header serializer based on extension
/// </summary>
public class ExtensionHeaderSerializer : IMsmqHeaderSerializer
{
    readonly HeaderSerializer _utf8HeaderSerializer = new() { Encoding = Encoding.UTF8 };

    /// <summary>
    /// Serializes headers to the extension property of the msmq-message
    /// </summary>
    public void SerializeToMessage(Dictionary<string, string> headers, Message msmqMessage)
    {
        if (headers == null) throw new ArgumentNullException(nameof(headers));
        if (msmqMessage == null) throw new ArgumentNullException(nameof(msmqMessage));
        msmqMessage.Extension = _utf8HeaderSerializer.Serialize(headers);
    }

    /// <summary>
    /// Deserialize msmq-message from the extension property
    /// </summary>
    public Dictionary<string, string> Deserialize(Message msmqMessage)
    {
        if (msmqMessage == null) throw new ArgumentNullException(nameof(msmqMessage));
        return _utf8HeaderSerializer.Deserialize(msmqMessage.Extension);
    }
}