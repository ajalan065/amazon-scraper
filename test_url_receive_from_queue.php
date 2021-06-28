<?php

use CustomerService\src\ReportsDBConnector;

require_once(__DIR__ . '/ScraperOutput.php');
require_once(__DIR__ . '/../../CustomerService/Report/ReturnsReports/ReportsDBConnector.php');

test_pull();

function test_pull() {
    $reportConnect = new ReportsDBConnector();
    $so = new ScraperOutput();
    $res = $so->readMessageFromSqsQueue();
//    $so->retryFailedMessages();
    t($res);
}