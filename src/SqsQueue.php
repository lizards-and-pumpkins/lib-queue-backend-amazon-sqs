<?php

declare(strict_types=1);

namespace LizardsAndPumpkins\Messaging\Queue\Sqs;

use Aws\Sqs\SqsClient;
use Guzzle\Service\Resource\Model;
use LizardsAndPumpkins\Messaging\MessageReceiver;
use LizardsAndPumpkins\Messaging\Queue;
use LizardsAndPumpkins\Messaging\Queue\Message;
use LizardsAndPumpkins\Util\Storage\Clearable;

class SqsQueue implements Queue, Clearable
{
    const MAX_AWS_SQS_CONSUMEABLE = 10;

    /**
     * @var SqsClient
     */
    private $client;

    /**
     * @var string
     */
    private $queueUrl;

    /**
     * @var int
     */
    private $waitTimeout;

    public function __construct(SqsClient $client, string $queueUrl, $waitTimeout = 20)
    {
        $this->client = $client;
        $this->queueUrl = $queueUrl;
        $this->waitTimeout = $waitTimeout;
    }

    public function clear(): void
    {
        $this->client->purgeQueue(['QueueUrl' => $this->queueUrl]);
    }

    public function count(): int
    {
        /** @var Model $queueAttributes */
        $queueAttributes = $this->client->getQueueAttributes([
            'QueueUrl' => $this->queueUrl,
            'AttributeNames' => ['ApproximateNumberOfMessages'],
        ]);

        return (int) $queueAttributes->get('Attributes')['ApproximateNumberOfMessages'];
    }

    public function add(Message $message): void
    {
        $this->client->sendMessage([
            'QueueUrl' => $this->queueUrl,
            'MessageBody' => $message->serialize(),
        ]);
    }

    public function consume(MessageReceiver $messageReceiver, int $numberOfMessagesToConsume): void
    {
        every($this->getMessages($numberOfMessagesToConsume), function (array $message) use ($messageReceiver) {
            $messageReceiver->receive(Message::rehydrate($message['Body']));

            $this->client->deleteMessage([
                'QueueUrl' => $this->queueUrl,
                'ReceiptHandle' => $message['ReceiptHandle']
            ]);
        });
    }

    /**
     * @param int $numberOfMessagesToConsume
     * @return array[]
     */
    private function getMessages(int $numberOfMessagesToConsume): array
    {
        if ($numberOfMessagesToConsume < 1) {
            throw new \InvalidArgumentException('You need to consume at least one message.');
        }

        $messages = $this->client->receiveMessage([
            'QueueUrl' => $this->queueUrl,
            'WaitTimeSeconds' => $this->waitTimeout,
            'MaxNumberOfMessages' => min($numberOfMessagesToConsume, self::MAX_AWS_SQS_CONSUMEABLE),
        ])->get('Messages');

        return $messages ?? [];
    }
}
