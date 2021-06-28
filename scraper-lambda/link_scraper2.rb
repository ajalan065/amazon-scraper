require 'httparty'
require 'nokogiri'
require 'mysql2'
require 'random_user_agent'
require 'uri'
require 'cgi'
load './constants.rb'

class LinkScraper2
  
  def fetch_base_url
    begin
      connection = Mysql2::Client.new(Constants::WRITE)
      puts connection
      result_url = connection.query("SELECT id, brand_link from amazon_brand_url_input where in_progress='link_received' limit 1")
    rescue Exception => e
      puts e
    end

    puts result_url

    if (result_url.first && (!(result_url.first.empty?)))
        return [result_url.first["id"], result_url.first["brand_link"]]
    else
        return ['', '']
    end
  end

  def parsed_page(url)
    Nokogiri::HTML(HTTParty.get(url, headers: {'User-Agent' => "#{RandomUserAgent.randomize}"}))
  end

  def get_asins_list
    base_id, base_url = fetch_base_url
    page_no = 1
    exit_flag = false

    puts "------------ #{base_id} ---------------- #{base_url}"

    if (!(base_url.empty?))
        until exit_flag == true

          connection = Mysql2::Client.new(Constants::WRITE)
          result_url = connection.query("UPDATE amazon_brand_url_input set in_progress='link_recreation_started' where id=" + base_id.to_s)

          url = remake_url(base_url + "&page=#{page_no}")
          puts url
          parse_page = Nokogiri::HTML(HTTParty.get(url, headers: {'User-Agent' => "#{RandomUserAgent.randomize}"}))
          next if error(parse_page)

          asin_count_arr = parse_page.css('#search').css('.rush-component')[0].css('h1')[0].css('.sg-col-inner')[0].text.strip.split(' ')
          if ((['over','around', 'less', 'more'].include? (asin_count_arr[2])) && asin_count_arr[2].to_i == 0)
            asin_count = asin_count_arr[3].gsub(',', '').to_i
          else
            asin_count = asin_count_arr[2].gsub(',', '').to_i
          end
          #approx_pages = ((asin_count/16)*1.40).to_i

          #hard coding to 20 as that is amazon limit
          approx_pages = 20
          puts approx_pages
          insert_into_db(remake_url(base_url + "&page="), approx_pages, base_id)
          page_no += 1
          exit_flag = (page_no == 2) ? true : false
          sleep(rand(2..20))
        end
    end
  end

  def remake_url(url)
    temp_url = URI.parse(url)

    # modified query paramter.
    return "https://www.amazon.com/s?" + URI.encode_www_form(CGI.parse(temp_url.query).select! { |key, value| ["rh", "page"].include?(key) }) rescue url
  end

  def error(parsed_page)
    if ((parsed_page.css('.a-last').text.include?("Sorry, we just need to make sure you're not a robot")) || parsed_page.title.include?("Something went wrong") || parsed_page.css('img').count == 0) || parsed_page.title.downcase == 'amazon' || (parsed_page.css('center')[0] && parsed_page.css('center')[0].downcase.include?('hello!'))
      return true
    else
      return false
    end
  end

  def insert_into_db(url, pages, base_id)
    begin
      connection = Mysql2::Client.new(Constants::WRITE)
      puts "here"
      ins_query = 'UPDATE amazon_brand_url_input SET brand_link="' + url + '", approx_pages=' + pages.to_s + ' WHERE id=' + base_id.to_s
      puts ins_query
      result = connection.query(ins_query)
      connection.query("UPDATE amazon_brand_url_input SET in_progress='link_recreated' where id=" + base_id.to_s)
    rescue Exception => e
      puts e
    end

  end


end

ls = LinkScraper2.new

ls.get_asins_list

