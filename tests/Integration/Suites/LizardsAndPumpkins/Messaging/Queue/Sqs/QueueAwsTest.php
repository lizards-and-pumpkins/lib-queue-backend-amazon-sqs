<?php

declare(strict_types=1);

namespace LizardsAndPumpkins\Messaging\Queue\Sqs;

use Aws\Sqs\SqsClient;
use Guzzle\Service\Resource\Model;
use LizardsAndPumpkins\Messaging\MessageReceiver;
use LizardsAndPumpkins\Messaging\Queue\Message;
use PHPUnit\Framework\TestCase;

class QueueAwsTest extends TestCase
{
    /**
     * @var SqsQueue
     */
    private $queue;

    /**
     * @var SqsClient
     */
    private $sqsClient;

    /**
     * @var string
     */
    private $queueUrl;

    final protected function setUp(): void
    {
        $this->sqsClient = SqsClient::factory([
            'credentials' => [
                'key' => $_ENV['aws_key'],
                'secret' => $_ENV['aws_secret'],
            ],
            'region' => 'eu-central-1',
        ]);

        $queueName = str_replace('.', '-', uniqid('lap-sqs-test', true));

        /** @var Model $response */
        $response = $this->sqsClient->createQueue([
            'QueueName' => $queueName,
        ]);

        $this->queueUrl = $response->get('QueueUrl');
        $this->queue = new SqsQueue($this->sqsClient, $this->queueUrl);
    }

    final protected function tearDown(): void
    {
        $this->sqsClient->deleteQueue([
            'QueueUrl' => $this->queueUrl,
        ]);
    }

    public function testEmptyAtBeginning(): void
    {
        $this->assertSame(0, $this->queue->count());
    }

    public function testCountRaisesAfterAddingAMessage(): void
    {
        $message = Message::withCurrentTime('my Message', [], []);
        $this->queue->add($message);
        $this->assertSame(1, $this->count());
    }

    public function testAddedMessageComesBack(): void
    {
        $message = Message::withCurrentTime('my Message', ['with complex payload'], ['and' => 'some metadata']);
        $this->queue->add($message);

        $receiver = new class implements MessageReceiver
        {
            /**
             * @var Message
             */
            public $message;

            public function receive(Message $message): void
            {
                $this->message = $message;
            }
        };

        $this->queue->consume($receiver, 1);

        /** @var Message $returnedMessage */
        $returnedMessage = $receiver->message;

        $this->assertSame($message->serialize(), $returnedMessage->serialize());
    }

    public function testMoreThan10MessagesComeBack()
    {
        for ($i = 0; $i < 15; $i++) {
            $message = Message::withCurrentTime(
                uniqid('lap', true), ['with complex payload'], ['and' => 'some metadata']
            );
            $this->queue->add($message);
        }

        $this->assertSame(15, $this->queue->count());

        $receiver = new class implements MessageReceiver
        {
            /**
             * @var Message
             */
            public $messages;

            public function receive(Message $message): void
            {
                $this->messages[] = $message;
            }
        };

        $consumeMoreThanPossible = 11;
        $this->queue->consume($receiver, $consumeMoreThanPossible);
        $this->assertLessThanOrEqual(10, count($receiver->messages));
    }

    public function testMessagesAreDeletedAfterConsume()
    {
        $queue = new SqsQueue($this->sqsClient, $this->queueUrl, 2);

        $id = uniqid('lap', true);
        $message = Message::withCurrentTime($id, ['with complex payload'], ['and' => 'some metadata']);
        $queue->add($message);

        $receiverMock = $this->createMock(MessageReceiver::class);
        $receiverMock->expects($this->once())->method('receive');

        $this->assertSame(1, $queue->count());

        $queue->consume($receiverMock, 1);

        sleep(3);

        $queue->consume($receiverMock, 1);
    }

    public function testCountMessages(): void
    {
        $count = 4;
        for ($i = 0; $i < $count; $i++) {
            $this->queue->add(Message::withCurrentTime('name', [], []));
        }
        $this->assertSame($count, $this->queue->count());
    }

    public function testClear(): void
    {
        $count = 4;

        for ($i = 0; $i < $count; $i++) {
            $this->queue->add(Message::withCurrentTime('name', [], []));
        }

        $this->assertSame($count, $this->queue->count());
        $this->queue->clear();
        $this->assertSame(0, $this->queue->count());
    }
}
