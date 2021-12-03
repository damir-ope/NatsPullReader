# NatsPullReader
NatsReader allows caller to pull a configurable number of messages from a Nats Server. The method NatsReader::consume return a List<Message> objects to the caller.

These classes provide  NATS pull reader functionality. Caller passes an NatsConfiguration object to the ctor of the NatsReader class.
The configuration defines, stream name, subject name as well as an optional filter name to provide a subset of the messages matching subject name.
A batch size configuration parameter limits the results returned to the client thus avoiding an issues around the reader consuming too much memory. there are some other parameters e.g. timeout values in mill-seconds indicating how long to wait for a message to be pulled from NATS.
  Worth noting that NATS pulls messages from NATS in batches of up to 256 messages at a time for better network utilization.

Caller executes the method NatsReader::consume to extract the messages. If the number of messages returned is less than the batch size then this indicates that NATS sever did not have enough messages to honour the request. The caller can invoke the method NatsReader::consume multiple times to fulfil the batch size or process these messages and then consume some more.
The unit tests cover basic usage of these classes

Note: This is not an executable jar file application
