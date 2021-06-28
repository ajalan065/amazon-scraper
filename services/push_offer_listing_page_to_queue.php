<?php

use CustomerService\src\ReportsDBConnector;

require_once(__DIR__ . '/../../Scraper/ScraperInput.php');
require_once(__DIR__ . '/../../../CustomerService/Report/ReturnsReports/ReportsDBConnector.php');

push_to_queue();

function push_to_queue() {
    $reportConnect = new ReportsDBConnector();

    $results = $reportConnect->dbStoresRead->fetchQueryToArray("SELECT id, asin_value FROM asin_details WHERE in_progress=0 limit 10");

    foreach ($results as $result) {
        $flag = true;

        $si = new ScraperInput("https://www.amazon.com/gp/offer-listing/" . $result["asin_value"] . "?startIndex=0", 'offer_scraper');

        try {
            $si->pushMessageToSqsQueue();
        }catch (Exception $e) {
            $flag = false;
            error_log("could not insert into input_scraper " . $e->getMessage());
            print_r($e->getMessage());
        }
        if ($flag == false) {
            $reportConnect->dbStoresWrite->query("UPDATE asin_details SET in_progress=2 WHERE id=" . $result["id"]);
        }
        else {
            $reportConnect->dbStoresWrite->query("UPDATE asin_details SET in_progress=1 WHERE id=" . $result["id"]);
        }
    }
}
