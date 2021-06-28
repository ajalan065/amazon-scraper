<?php

require_once(__DIR__ . '/../../../CustomerService/Report/ReturnsReports/ReportsDBConnector.php');
require_once(__DIR__ . '/../../Scraper/ScraperInput.php');

$reportConnect = new \CustomerService\src\ReportsDBConnector();

$res = $reportConnect->dbStoresWrite->fetchQueryToArray("SELECT s_id, url, ref_key FROM input_scraper where converted_flag=3");

if (!empty($res)) {
    foreach ($res as $item) {
        t($item);
        $reportConnect->dbStoresWrite->query("DELETE FROM input_scraper WHERE s_id='" . $item['s_id'] . "'");
        $si = new ScraperInput($item['url'], $item['ref_key']);
        try {
            $si->pushMessageToSqsQueue();
        }catch (Exception $e) {
            $flag = false;
            error_log("could not insert into input_scraper " . $e->getMessage());
            print_r($e->getMessage());
        }
    }
}