using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MSMQ.Messaging;
using Rebus.Bus;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Transport;
using Message = MSMQ.Messaging.Message;

#pragma warning disable 1998

namespace Rebus.Msmq.Messaging;

/// <summary>
/// Implementation of <see cref="ITransport"/> that uses MSMQ to do its thing
/// </summary>
public class MsmqTransport : AbstractRebusTransport, IInitializable, IDisposable
{
    const string CurrentTransactionKey = "msmqtransport-messagequeuetransaction";
    readonly List<Action<MessageQueue>> _newQueueCallbacks = new();
    readonly IMsmqHeaderSerializer _msmqHeaderSerializer;
    readonly ILog _log;

    volatile MessageQueue _inputQueue;
    bool _disposed;

    /// <summary>
    /// Constructs the transport with the specified input queue address
    /// </summary>
    public MsmqTransport(string inputQueueAddress, IRebusLoggerFactory rebusLoggerFactory, IMsmqHeaderSerializer msmqHeaderSerializer) : base(MakeGloballyAddressable(inputQueueAddress))
    {
        if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
        _msmqHeaderSerializer = msmqHeaderSerializer ?? throw new ArgumentNullException(nameof(msmqHeaderSerializer));
        _log = rebusLoggerFactory.GetLogger<MsmqTransport>();
    }

    static string MakeGloballyAddressable(string inputQueueName) => inputQueueName == null ? null : inputQueueName.Contains("@") ? inputQueueName : $"{inputQueueName}@{Environment.MachineName}";

    /// <summary>
    /// Adds a callback to be invoked when a new queue is created. Can be used e.g. to customize permissions
    /// </summary>
    public void AddQueueCallback(Action<MessageQueue> callback)
    {
        _newQueueCallbacks.Add(callback);
    }

    /// <summary>
    /// Initializes the transport by creating the input queue
    /// </summary>
    public void Initialize()
    {
        if (Address != null)
        {
            _log.Info("Initializing MSMQ transport - input queue: {queueName}", Address);

            GetInputQueue();
        }
        else
        {
            _log.Info("Initializing one-way MSMQ transport");
        }
    }

    /// <summary>
    /// Creates a queue with the given address, unless the address is of a remote queue - in that case,
    /// this call is ignored
    /// </summary>
    public override void CreateQueue(string address)
    {
        if (!MsmqUtil.IsLocal(address)) return;

        EnsureCreatedAndVerifyQueue(address);
    }

    /// <summary>
    /// Deletes all messages in the input queue
    /// </summary>
    public void PurgeInputQueue()
    {
        if (!MsmqUtil.QueueExists(Address))
        {
            _log.Info("Purging {queueName} (but the queue doesn't exist...)", Address);
            return;
        }

        _log.Info("Purging {queueName}", Address);
        MsmqUtil.PurgeQueue(Address);
    }

    protected override async Task SendOutgoingMessages(IEnumerable<OutgoingTransportMessage> outgoingMessages, ITransactionContext context)
    {
        // if the transaction context has a message queue transaction, we're being called in a Rebus handler, so we just send all the outgoing messages using that
        if (context.Items.TryGetValue(CurrentTransactionKey, out var result)
            && result is MessageQueueTransaction messageQueueTransaction)
        {
            SendOutgoingMessagesWithThis(messageQueueTransaction);
        }
        else
        {
            //otherwise, we're sending messages outside of a Rebus handler, so the easiest way to send the stuff is like this
            using var newMessageQueueTransaction = new MessageQueueTransaction();

            newMessageQueueTransaction.Begin();
            
            SendOutgoingMessagesWithThis(newMessageQueueTransaction);
            
            newMessageQueueTransaction.Commit();
        }

        void SendOutgoingMessagesWithThis(MessageQueueTransaction transaction)
        {
            foreach (var messagesByDestination in outgoingMessages.GroupBy(g => g.DestinationAddress))
            {
                var destinationQueueName = messagesByDestination.Key;
                var path = MsmqUtil.GetFullPath(destinationQueueName);

                using var queue = new MessageQueue(path, QueueAccessMode.Send);

                foreach (var message in messagesByDestination)
                {
                    var logicalMessage = CreateMsmqMessage(message.TransportMessage);

                    queue.Send(logicalMessage, transaction);
                }
            }
        }
    }

    /// <summary>
    /// Receives the next available transport message from the input queue via MSMQ. Will create a new <see cref="MessageQueueTransaction"/> and stash
    /// it under the <see cref="CurrentTransactionKey"/> key in the given <paramref name="context"/>. If one already exists, an exception will be thrown
    /// (because we should never have to receive multiple messages in the same transaction)
    /// </summary>
    public override async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        if (context == null) throw new ArgumentNullException(nameof(context));
        if (Address == null) throw new InvalidOperationException("This MSMQ transport does not have an input queue, which indicates that it was configured as the transport for a one-way client. It is not possible to reveive anything with it.");

        var queue = GetInputQueue();

        if (context.Items.ContainsKey(CurrentTransactionKey))
        {
            throw new InvalidOperationException("Tried to receive with an already existing MSMQ queue transaction - although it is possible with MSMQ to do so, with Rebus it is an indication that something is wrong!");
        }

        var messageQueueTransaction = new MessageQueueTransaction();
        messageQueueTransaction.Begin();

        context.OnDisposed(_ => messageQueueTransaction.Dispose());
        context.Items[CurrentTransactionKey] = messageQueueTransaction;

        try
        {
            var message = queue.Receive(TimeSpan.FromSeconds(0.5), messageQueueTransaction);

            if (message == null)
            {
                messageQueueTransaction.Abort();
                return null;
            }

            context.OnAck(async _ => messageQueueTransaction.Commit());
            context.OnNack(async _ => messageQueueTransaction.Abort());
            context.OnDisposed(_ => message.Dispose());

            var headers = _msmqHeaderSerializer.Deserialize(message) ?? new Dictionary<string, string>();

            using var target = new MemoryStream();

            await message.BodyStream.CopyToAsync(target);

            return new TransportMessage(headers, target.ToArray());
        }
        catch (MessageQueueException exception)
        {
            if (exception.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
            {
                return null;
            }

            if (exception.MessageQueueErrorCode == MessageQueueErrorCode.InvalidHandle)
            {
                _log.Warn("Queue handle for {queueName} was invalid - will try to reinitialize the queue", Address);
                ReinitializeInputQueue();
                return null;
            }

            if (exception.MessageQueueErrorCode == MessageQueueErrorCode.QueueDeleted)
            {
                _log.Warn("Queue {queueName} was deleted - will not receive any more messages", Address);
                return null;
            }

            throw new IOException($"Could not receive next message from MSMQ queue '{Address}'", exception);
        }
    }

    Message CreateMsmqMessage(TransportMessage message)
    {
        var headers = message.Headers;

        var expressDelivery = headers.ContainsKey(Headers.Express);
        var hasTimeout = headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceivedStr);

        var msmqMessage = new Message
        {
            BodyStream = new MemoryStream(message.Body),
            UseJournalQueue = false,
            Recoverable = !expressDelivery,
            UseDeadLetterQueue = !(expressDelivery || hasTimeout),
            Label = GetMessageLabel(message),
        };

        _msmqHeaderSerializer.SerializeToMessage(headers, msmqMessage);

        if (hasTimeout)
        {
            msmqMessage.TimeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);
        }

        return msmqMessage;
    }

    static string GetMessageLabel(TransportMessage message)
    {
        try
        {
            return message.GetMessageLabel();
        }
        catch
        {
            // if that failed, it's most likely because we're running in legacy mode - therefore:
            return message.Headers.GetValueOrNull(Headers.MessageId)
                   ?? message.Headers.GetValueOrNull("rebus-msg-id")
                   ?? "<unknown ID>";
        }
    }

    void ReinitializeInputQueue()
    {
        if (_inputQueue != null)
        {
            try
            {
                _inputQueue.Close();
                _inputQueue.Dispose();
            }
            catch (Exception exception)
            {
                _log.Warn(exception, "An error occurred when closing/disposing the queue handle for {queueName}", Address);
            }
            finally
            {
                _inputQueue = null;
            }
        }

        GetInputQueue();

        _log.Info("Input queue handle successfully reinitialized");
    }

    MessageQueue GetInputQueue()
    {
        if (_inputQueue != null) return _inputQueue;

        lock (this)
        {
            if (_inputQueue != null) return _inputQueue;

            EnsureCreatedAndVerifyQueue(Address);

            _inputQueue = new MessageQueue(MsmqUtil.GetPath(Address), QueueAccessMode.SendAndReceive)
            {
                MessageReadPropertyFilter = new MessagePropertyFilter
                {
                    Id = true,
                    Extension = true,
                    Body = true,
                }
            };
        }

        return _inputQueue;
    }

    void EnsureCreatedAndVerifyQueue(string address)
    {
        void InvokeNewQueueCallbacks(MessageQueue messageQueue)
        {
            foreach (var callback in _newQueueCallbacks)
            {
                callback(messageQueue);
            }
        }

        var inputQueuePath = MsmqUtil.GetPath(address);

        MsmqUtil.EnsureQueueExists(inputQueuePath, _log, InvokeNewQueueCallbacks);

        MsmqUtil.EnsureMessageQueueIsTransactional(inputQueuePath);
    }

    /// <summary>
    /// Disposes the input message queue object
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;

        try
        {
            _inputQueue?.Dispose();
            _inputQueue = null;
        }
        finally
        {
            _disposed = true;
        }
    }
}