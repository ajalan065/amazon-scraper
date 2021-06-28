<?php


//use AmazonServicesConfig;
use Aws\Sqs\Exception\SqsException;
use Aws\Sqs\SqsClient;
use CustomerService\src\ReportsDBConnector;

date_default_timezone_set('UTC');
error_reporting(E_ALL);
ini_set("display_errors",
    TRUE);

require __DIR__ . '/../../vendor/aws/aws-autoloader.php';
require_once(__DIR__ . '/../../SubscriptionApi/AmazonServicesConfig.php');
require_once(__DIR__ . '/../../CustomerService/Report/ReturnsReports/ReportsDBConnector.php');

class ScraperInput
{
    public $url, $id, $ref_key;

    /**
     * @var SqsClient
     */
    public $client_sqs;

    public function __construct($input_url, $ref_key)
    {
        $this->client_sqs = SqsClient::factory(AmazonServicesConfig::$SQS_CONFIG);
        $this->url = $input_url;
        $this->ref_key = $ref_key;
        $this->id = md5($ref_key . $input_url);
    }

    public function pushMessageToSqsQueue($retryFlag = false)
    {

        $insertFlag = $this->saveDetailsInDB($retryFlag);
        $queueUrl = "http://localstack:4576/queue/url-queue";

        if (MPS_SERVER == "LIVE") {
            $queueUrl = AmazonServicesConfig::$SQS_WEB_SCRAPER_INPUT_QUEUE_URL;
        }
        if ($insertFlag) {
            $this->client_sqs->sendMessage(array(
                'QueueUrl' => $queueUrl,
                'MessageBody' => json_encode(array(
                    'url' => $this->url,
                    'id' => $this->id,
                    'ref_key' => $this->ref_key
                ))
            ));
        }
    }

    private function saveDetailsInDB($retryFlag)
    {
        $reportConnect = new ReportsDBConnector();
        if ($retryFlag) {
            $reportConnect->dbStoresWrite->query("UPDATE input_scraper SET retry_count=retry_count+1, retry_timestamp=CURRENT_TIMESTAMP() WHERE s_id='" . $this->id . "'");
            return true;
        }

        $resultExists = $reportConnect->dbStoresWrite->fetchQueryToArray("SELECT url FROM input_scraper WHERE s_id='" . $this->id . "'", true);
        if (empty($resultExists)) {
            $reportConnect->dbStoresWrite->query("INSERT INTO input_scraper (s_id, url, ref_key) VALUES ('" . $this->id . "', '" . $this->url . "', '" . $this->ref_key . "')");
            return true;
        }

        return false;
    }

    public function getId()
    {
        return $this->id;
    }

    public static function getPublicS3Url($id)
    {
        $reportConnect = new ReportsDBConnector();
        $url = $reportConnect->dbStoresRead->fetchQueryToArray("SELECT s3_url FROM input_scraper WHERE s_id='" . $id . "'");

        return $url;
    }

    public static function updateS3Url()
    {
        $reportConnect = new ReportsDBConnector();
        $results = self::readMessageFromSqsQueue();
        $result = $results[0];
        $receipt_handle = $results[1];

        if (!empty($result)) {
            foreach ($result as $item) {
                $s3_url_query = "UPDATE input_scraper SET s3_url='" . $item["body"] . "' WHERE s_id='" . $item["id"] . "'";
                $r = $reportConnect->dbStoresWrite->query($s3_url_query);
                if (empty($r)) {
                    error_log("input scraper not updating se_url : " . $s3_url_query);
                }
            }
        }
        self::deleteSqsMsg($receipt_handle[0], $receipt_handle[1], $receipt_handle[2]);
    }

    public static function readMessageFromSqsQueue()
    {
        $messages = SqsClient::factory(AmazonServicesConfig::$SQS_CONFIG)->receiveMessage(array(
            'QueueUrl' => AmazonServicesConfig::$SQS_WEB_SCRAPER_OUTPUT_QUEUE_URL,
            'MaxNumberOfMessages' => 10,
            'VisibilityTimeout' => 1000
        ));

        $received_messages[] = $messages['Messages'];
        $result = array();
        $receiptHandleArray = array();
        $cnt = 1;

        if (!empty($received_messages)) {
            foreach ($received_messages as $item) {
                if (is_array($item) && count($item) > 0) {
                    foreach ($item as $val) {
                        $body = json_decode($val['Body'], true);
                        $result[] = $body;
                        $receiptHandleArray[] = array(
                            "Id" => $cnt++,
                            "ReceiptHandle" => $val['ReceiptHandle']
                        );
                    }
                }
            }
        }

        $deleteSqsArr = array($receiptHandleArray, AmazonServicesConfig::$SQS_WEB_SCRAPER_OUTPUT_QUEUE_URL, SqsClient::factory(AmazonServicesConfig::$SQS_CONFIG));


        return array($result, $deleteSqsArr);
    }

    public static function deleteSqsMsg($receiptHandleArray, $queueUrl, SqsClient $clientSqs) {
        $arrBatchDelete = array_chunk($receiptHandleArray,
            10);
        $arrResult = array();
        foreach ($arrBatchDelete as $arrBatch) {
            try {
                $arrResult[] = $clientSqs->DeleteMessageBatch(
                    array(
                        'QueueUrl' => $queueUrl,
                        'Entries' => $arrBatch
                    )
                );
            } catch (SqsException $e) {
                sleep(3);
                $clientSqs = SqsClient::factory(AmazonServicesConfig::$SQS_CONFIG);
                $arrResult[] = $clientSqs->DeleteMessageBatch(
                    array(
                        'QueueUrl' => $queueUrl,
                        'Entries' => $arrBatch
                    )
                );
                error_log("Scraper Ruby Module Error - " . basename(__FILE__) . " Exception :: "
                    . 'DeleteMessageBatch tried again for :' .
                    json_encode($arrBatch) . ", ----- Reason : " . $e->getMessage());
            }
        }

        return $arrResult;
    }
}