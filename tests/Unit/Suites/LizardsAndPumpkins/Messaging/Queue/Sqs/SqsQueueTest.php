<?php
declare(strict_types=1);

namespace LizardsAndPumpkins\Messaging\Queue\Sqs;

use Aws\Sqs\SqsClient;
use Guzzle\Service\Resource\Model;
use LizardsAndPumpkins\Messaging\Queue\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

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

    protected function setUp()
    {
        $this->sqsClientMock = $this->getMockBuilder(SqsClient::class)
            ->disableOriginalConstructor()
            ->setMethods(['sendMessage', 'getQueueAttributes', 'PurgeQueue'])->getMock();
        $this->queue = new SqsQueue($this->sqsClientMock, $this->queueName);
    }

    public function testQueueIsEmptyOnStart(): void
    {
        $count = 0;

        $this->setCountResponse($count);
        $this->assertSame($count, $this->queue->count());
    }

    public function testAddMessageToTestQueue()
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

    public function testCountIncreasesOnAddMessage()
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
    private function getMessage()
    {
        /** @var Message|MockObject $message */
        $message = $this->createMock(Message::class);
        return $message;
    }

    /**
     * @param int $count
     */
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

    public function testCleanCallsPurge()
    {
        $this->sqsClientMock->expects($this->once())->method('PurgeQueue')->with(['QueueUrl' => $this->queueName]);

        $this->queue->clear();
    }
}
