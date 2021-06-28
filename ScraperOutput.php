<?php

//use AmazonServicesConfig;
use Aws\Sqs\Exception\SqsException;
use CustomerService\src\ReportsDBConnector;
use Aws\Sqs\SqsClient;

date_default_timezone_set( 'UTC' );
error_reporting( E_ALL );
ini_set( "display_errors",
    TRUE );

//require __DIR__ . '/../../vendor/aws/aws-autoloader.php';
require_once(__DIR__. '/../../SubscriptionApi/AmazonServicesConfig.php');
require_once(__DIR__ . '/../../CustomerService/Report/ReturnsReports/ReportsDBConnector.php');
require_once(__DIR__ . '/ScraperInput.php');

class ScraperOutput {

    public static function readMessageFromSqsQueue() {
        $messages = SqsClient::factory( AmazonServicesConfig::$SQS_CONFIG )->receiveMessage( array(
            'QueueUrl'            => AmazonServicesConfig::$SQS_WEB_SCRAPER_RETRY_QUEUE_URL,
            'MaxNumberOfMessages' => 10,
            'VisibilityTimeout'   => 1000
        ) );

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

        $deleteSqsArr = array($receiptHandleArray, AmazonServicesConfig::$SQS_WEB_SCRAPER_RETRY_QUEUE_URL, SqsClient::factory(AmazonServicesConfig::$SQS_CONFIG));

        return array($result, $deleteSqsArr);
    }

    public static function executeMessages() {
        $reportConnect = new ReportsDBConnector();

        $results = self::readMessageFromSqsQueue();
        $result = $results[0];
        $receipt_handle = $results[1];

        if(!empty($result)) {
            foreach ($result as $item) {
                if ($item["action"] == "retry") {
                    $orig_url = $reportConnect->dbStoresRead->fetchQueryToArray("SELECT ref_key, url FROM input_scraper WHERE s_id = '" . $item["id"] . "'");
                    $si = new ScraperInput($orig_url['url'], $orig_url['ref_key']);
                    try {
                        $si->pushMessageToSqsQueue(true);
                    } catch (Exception $e) {
                        error_log("could not insert into input_scraper " . $e->getMessage());
                        print_r($e->getMessage());
                    }
                }
                else if ($item["action"] == "success") {
//                    self::insertDetailsIntoDB($item);
                }

                self::deleteSqsMsg($receipt_handle[0], $receipt_handle[1], $receipt_handle[2]);
            }
        }
    }

    private static function insertDetailsIntoDB($item) {
        $reportConnect = new ReportsDBConnector();

        if ($item["ref_key"] == "link_scraper") {
            $body = json_decode($item["body"], true);
            $count = count($body["asins"]);
            for ($c = 0; $c < $count; $c++) {
                $array = array(
                    'brand_id' =>  $body["brand_id"],
                    'asin_value' => $body["asins"][$c],
                    'ratings' => $body["ratings"][$c]
                );
                $reportConnect->dbStoresWrite->arrayToInsert("brand_asins", $array);
            }
        }
        else if ($item["ref_key"] == "asin_scraper") {
            $body = json_decode($item["body"], true);
            $asin = $reportConnect->dbStoresRead->fetchQueryToArray("SELECT id from brand_asins WHERE asin_value='" . $body['asin_value'] . "'", true);
            $res = $reportConnect->dbStoresRead->fetchQueryToArray("SELECT id FROM asin_details WHERE asin_id=" . $asin['id'], true);
            $array = array(
                'bb_winner' => $body['merchant_info'],
                'bb_price' => $body['price'],
                'category' => $body['category'],
                'rank' => $body['rank']
            );

            if (empty($res)) {
                $array['asin_id'] = $asin['id'];
                $array['asin_value'] = $body['asin_value'];
                $reportConnect->dbStoresWrite->arrayToInsert("asin_details", $array);
            }
            else {
                $reportConnect->dbStoresWrite->arrayToUpdate("asin_details", $array, "asin_id=" . $asin['id']);
            }
        }
        else if ($item["ref_key"] == "offer_scraper") {
            $body = json_decode($item["body"], true);
        }
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