<?php

declare(strict_types=1);

namespace LizardsAndPumpkins\Messaging\Queue\Sqs;

use Aws\Sqs\SqsClient;
use Guzzle\Service\Resource\Model;
use LizardsAndPumpkins\Messaging\MessageReceiver;
use LizardsAndPumpkins\Messaging\Queue;
use LizardsAndPumpkins\Messaging\Queue\Message;
use LizardsAndPumpkins\Util\Storage\Clearable;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * @covers \LizardsAndPumpkins\Messaging\Queue\Sqs\SqsQueue
 */
class SqsQueueTest extends TestCase
{
    /**
     * @var SqsQueue
     */
    private $queue;

    /**
     * @var SqsClient|MockObject
     */
    private $sqsClientMock;

    /**
     * @var string
     */
    private $queueName = 'testQueueName';

    final protected function setUp(): void
    {
        $this->sqsClientMock = $this->getMockBuilder(SqsClient::class)
            ->disableOriginalConstructor()
            ->setMethods(['sendMessage', 'getQueueAttributes', 'PurgeQueue', 'receiveMessage', 'deleteMessage'])
            ->getMock();
        $this->queue = new SqsQueue($this->sqsClientMock, $this->queueName);
    }

    public function testImplementsQueue(): void
    {
        $this->assertInstanceOf(Queue::class, $this->queue);
    }

    public function testImplementsClearable(): void
    {
        $this->assertInstanceOf(Clearable::class, $this->queue);
    }

    public function testQueueIsEmptyOnStart(): void
    {
        $count = 0;

        $this->setCountResponse($count);
        $this->assertSame($count, $this->queue->count());
    }

    public function testAddMessageToTestQueue(): void
    {
        $json = 'fancy_serialized_message';

        $message = $this->getMessage();
        $message->method('serialize')->willReturn($json);

        $arguments = [
            'QueueUrl' => $this->queueName,
            'MessageBody' => $json,
        ];

        $this->sqsClientMock->expects($this->once())->method('sendMessage')->with($arguments);

        $this->queue->add($message);
    }

    public function testCountIncreasesOnAddMessage(): void
    {
        $message = $this->getMessage();

        $count = 1;

        $this->setCountResponse($count);

        $this->queue->add($message);
        $this->assertSame($count, $this->queue->count());
    }

    /**
     * @return Message|MockObject
     */
    private function getMessage(): Message
    {
        /** @var Message|MockObject $message */
        $message = $this->createMock(Message::class);

        return $message;
    }

    private function setCountResponse(int $count): void
    {
        $response = $this->createMock(Model::class);
        $response->method('get')->with('Attributes')->willReturn([
            'ApproximateNumberOfMessages' => $count,
        ]);

        $arguments = [
            'QueueUrl' => $this->queueName,
            'AttributeNames' => ['ApproximateNumberOfMessages'],
        ];

        $this->sqsClientMock
            ->expects($this->once())
            ->method('getQueueAttributes')
            ->with($arguments)
            ->willReturn($response);
    }

    public function testCleanCallsPurge(): void
    {
        $this->sqsClientMock->expects($this->once())->method('PurgeQueue')->with(['QueueUrl' => $this->queueName]);

        $this->queue->clear();
    }

    public function testConsume(): void
    {
        /** @var MessageReceiver|MockObject $messageReceiver */
        $messageReceiver = $this->createMock(MessageReceiver::class);
        $messageReceiver->expects($this->once())->method('receive')->with($this->isInstanceOf(Message::class));

        $numberOfMessages = 1;
        $this->prepareReceiveMessages($numberOfMessages);

        $this->queue->consume($messageReceiver, $numberOfMessages);
    }

    public function testConsumeMaxTenMessages(): void
    {
        /** @var MessageReceiver|MockObject $messageReceiver */
        $messageReceiver = $this->createMock(MessageReceiver::class);
        $messageReceiver->expects($this->once())->method('receive')->with($this->isInstanceOf(Message::class));

        $numberOfMessages = 10;
        $this->prepareReceiveMessages($numberOfMessages);

        $moreThanTenMessages = 15;
        $this->queue->consume($messageReceiver, $moreThanTenMessages);
    }

    public function testThrowsExceptionIfConsumeLessThanOneMessage()
    {
        $this->expectExceptionMessage('You need to consume at least one message.');
        $this->expectException(\InvalidArgumentException::class);

        $lessThanOne = 0;

        /** @var MessageReceiver|MockObject $messageReceiver */
        $messageReceiver = $this->createMock(MessageReceiver::class);
        $this->queue->consume($messageReceiver, $lessThanOne);
    }

    public function testConsumeOnGetMessagesReturnsNull(): void
    {
        /** @var MessageReceiver|MockObject $messageReceiver */
        $messageReceiver = $this->createMock(MessageReceiver::class);

        $messages = $this->createMock(Model::class);
        $messages->method('get')->with('Messages')->willReturn(null);

        $this->sqsClientMock->expects($this->once())->method('receiveMessage')->willReturn($messages);

        $this->queue->consume($messageReceiver, 1);
    }

    public function testMessageIsDeletedAfterConsume()
    {
        /** @var MessageReceiver|MockObject $messageReceiver */
        $messageReceiver = $this->createMock(MessageReceiver::class);

        $numberOfMessages = 1;

        $this->prepareReceiveMessages($numberOfMessages);

        $this->sqsClientMock->expects($this->once())->method('deleteMessage');

        $this->queue->consume($messageReceiver, $numberOfMessages);
    }

    /**
     * @param int $numberOfMessages
     */
    private function prepareReceiveMessages(int $numberOfMessages): void
    {
        $arguments = [
            'QueueUrl' => $this->queueName,
            'WaitTimeSeconds' => 20,
            'MaxNumberOfMessages' => $numberOfMessages,
        ];

        $messageBody = Message::withCurrentTime('messageName', ['thisIsThePayload'], ['metadata' => 'is possible!'])
            ->serialize();

        $messages = $this->createMock(Model::class);
        $messages->method('get')->with('Messages')->willReturn(
            $allMessages = [
                $singleMessage = [
                    'Body' => $messageBody,
                    'ReceiptHandle' => uniqid('lap-test', true),
                ],
            ]
        );

        $this->sqsClientMock->expects($this->once())->method('receiveMessage')->with($arguments)->willReturn($messages);
    }
}
