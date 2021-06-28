require 'aws-sdk-sqs'
require 'aws-sdk-s3'
require 'json'
require 'random_user_agent'
require 'httparty'
require 'uri'
require 'cgi'

def parse(event:, context:)
	body = event["Records"][0]["body"]

	body_hash = JSON.parse(body)

	url = body_hash["url"]
	id = body_hash["id"]
	ref_key = body_hash["ref_key"]

	puts event
	puts "-----------"
	puts Time.now.utc
	
	http_response = HTTParty.get(url, headers: {'User-Agent' => "#{RandomUserAgent.randomize}"})

	if http_response.response.code == '200'
		s3 = Aws::S3::Client.new(region: 'us-east-1')
		s3_res = Aws::S3::Resource.new(client: s3)
		
		sqs = Aws::SQS::Client.new(region: 'us-east-1')
		
		resp = s3.put_object({bucket: '123stores-web-scraper', key: "#{id}", body: http_response.response.body})
		
		body = s3_res.bucket('123stores-web-scraper').object("#{id}").public_url

		qurl = "https://sqs.us-east-1.amazonaws.com/118637774962/web-scrapper-output-queue"

		sqs.send_message({queue_url: qurl, message_body: ({id: id, ref_key: ref_key, body: body}).to_json})
	else
		raise Exception.new
	end
end