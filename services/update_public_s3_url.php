<?php

use CustomerService\src\ReportsDBConnector;

require_once(__DIR__ . '/../../Scraper/ScraperInput.php');
require_once(__DIR__ . '/../../../CustomerService/Report/ReturnsReports/ReportsDBConnector.php');

updatePublicS3UrlInDB();

function updatePublicS3UrlInDB() {
    ScraperInput::updateS3Url();
     // delete acknowledged messages
}