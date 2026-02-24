import csv
import json
import os
import smtplib
from email.message import EmailMessage
from math import ceil
import uuid
from datetime import datetime
from collections import defaultdict

from curl_cffi import requests
# import requests
from scrapy import Spider, Request, Selector


class MpbSpider(Spider):
    name = "mpb"
    base_url = 'https://www.mpb.com/nl-nl'

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,

        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,  # 60 sec page timeout
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },

        # Middlewares
        # "DOWNLOADER_MIDDLEWARES": {
        #     "mpb_products.middlewares.DataImpulseProxyMiddleware": 350,
        #     "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 400,
        # },

        # ⭐ DataImpulse proxy inside Playwright
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "proxy": {
                    "server": "http://gw.dataimpulse.com:823",
                    "username": "a81a192a105ce445337b__cr.nl",
                    "password": "df1bb30ecb142960",
                }
            }
        },

        "PLAYWRIGHT_MAX_PAGES_PER_CONTEXT": 4,

    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'content-language': 'nl_NL',
        'priority': 'u=1, i',
        'referer': 'https://www.mpb.com/nl-nl/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cookies = self.load_cookies()
        self.duplicate_skipped_counter = 0
        self.summary_data = {}
        self.gmail_config = self.get_configuration_from_txt('input/email_alert_config.txt')
        self.sender_email = self.gmail_config.get('SENDER_EMAIL')
        self.receiver_email = self.gmail_config.get('RECEIVER_EMAIL')
        self.email_obj = self.build_connection_with_gmail()  # Login to Gmail with app password
        self.notes_filename = 'input/mpb_product_notes.csv'
        self.output_filename = f'output/mpb_products_{datetime.now().strftime("%d%m%Y%H%M")}.json'
        self.failed_pages_status = []
        self.seen_product_notes_items = {row.get('sku'): row.get('notes') for row in self.read_csv_file() if
                                         row.get('sku')}
        self.seen_product_notes_skus = set(self.seen_product_notes_items.keys())
        self.seen_product_urls = []
        self.current_scrapped_items = []
        self.start_time = datetime.utcnow()
        self.failed_pages = 0

    def start_requests(self):
        # working url below
        url = 'https://www.mpb.com/search-service/product/query/?filter_query[object_type]=product&filter_query[product_condition_star_rating]=%5B1%20TO%205%5D%20AND%20NOT%200&filter_query[model_market]=EU&filter_query[model_available]=true&filter_query[model_is_published_out]=true&field_list=model_name&field_list=model_description&field_list=product_price&field_list=model_url_segment&field_list=product_sku&field_list=product_condition&field_list=product_shutter_count&field_list=product_hour_count&field_list=product_battery_charge_count&field_list=product_id&field_list=product_images&field_list=model_id&field_list=product_price_reduction&field_list=product_price_original&field_list=product_price_modifiers&field_list=model_available_new&sort[product_last_online]=DESC&facet_minimum_count=1&facet_field=model_brand&facet_field=model_category&facet_field=model_product_type&facet_field=product_condition_star_rating&facet_field=product_price&facet_field=%2A&start=0&rows=1000&minimum_match=100%25'
        yield Request(url=url, headers=self.headers,
                      # meta={"playwright": True},
                      meta={
                          "playwright": True,
                          "playwright_page_methods": [
                              ("wait_for_load_state", "networkidle"),
                          ],
                      },
                      errback=self.errback_handler,
                      cookies=self.cookies
                      )

    def parse(self, response, **kwargs):
        yield from self.parse_products(response)

        # # TODO: Uncomment these below code lines for production
        try:
            json_data = json.loads(response.css('pre ::text').get(''))
        except:
            json_data = {}

        total_results = json_data.get('total_results') or 0
        total_page = ceil(total_results / 1000)

        # # TODO: Uncomment these below code lines for production
        # for page_number in range(1,total_page+1):
        for page_number in range(1, 2 + 1):
            next_page_url = response.url.replace('&start=0', f'&start={page_number * 1000}')
            yield Request(url=next_page_url, headers=self.headers,
                          # meta={"playwright": True},
                          meta={
                              "playwright": True,
                              "playwright_page_methods": [
                                  ("wait_for_load_state", "networkidle"),
                              ],
                          },
                          callback=self.parse_products, errback=self.errback_handler, cookies=self.cookies)

    def parse_products(self, response):
        try:
            json_data = json.loads(response.css('pre ::text').get(''))
        except:
            json_data = {}

        results = json_data.get('results') or []

        for row in results[:5]:  # TODO: remove the 100 slicing
            product_sku = self.get_first_value(row, 'product_sku')
            if not product_sku:
                continue

            product_slug = self.get_first_value(row, 'model_url_segment')
            product_url = f'https://www.mpb.com/nl-nl/product/{product_slug}/sku-{product_sku}'

            if product_url in self.seen_product_urls:
                self.duplicate_skipped_counter += 1
                continue

            item = dict()
            item['product_title'] = self.get_first_value(row, 'model_name')
            item['sku'] = product_sku
            item['price'] = self.get_product_price(row)
            item['condition'] = self.get_first_value(row, 'product_condition')
            item['availability'] = 'in_stock'
            item['shutter_count'] = self.get_first_value(row, 'product_shutter_count')
            item['notes'] = ''
            item['url'] = product_url

            self.seen_product_urls.append(product_url)

            # check if product notes already scrapped in file then we don't need to do detail page request
            # we are requesting detail page only for notes
            if product_sku in self.seen_product_notes_skus:
                item['notes'] = self.seen_product_notes_items.get(product_sku)
                self.current_scrapped_items.append(item)
                yield item
                continue

            yield from self.parse_details(product_url)
            d = 1

            # yield Request(url=product_url, headers=self.headers,
            #               # meta={"playwright": True},
            #               meta={
            #                   "playwright": True,
            #                   "playwright_page_methods": [
            #                       ("wait_for_load_state", "networkidle"),
            #                   ],
            #               },
            #               callback=self.parse_details,errback=self.errback_handler)

    def parse_details(self, product_url):
        # product_response = requests.get(product_url, headers=self.headers, impersonate="chrome")

        proxy = 'http://a81a192a105ce445337b__cr.nl:df1bb30ecb142960@gw.dataimpulse.com:823'
        proxies = {
            # "http": proxy,
            "https": proxy,
        }
        # product_response = requests.get(product_url, headers=self.headers, proxies=proxies, timeout=100, verify=False)
        product_response = requests.get(product_url, headers=self.headers, impersonate="chrome", proxies=proxies, timeout=60)

        print(f'\nResponse status:{product_response.status_code} for Product:{product_url}\n')

        response = Selector(text=product_response.text)

        try:
            json_data = json.loads(response.css('#__NEXT_DATA__ ::text').get(''))['props']['pageProps']
        except:
            json_data = {}

        model_info = json_data.get('modelInfo', {}) or {}
        product_info = json_data.get('productInfo') or {}

        item = dict()
        item['product_title'] = product_info.get('name') or response.css('.product-name ::text').get(
            '') or model_info.get('brand', {}).get('name')
        item['sku'] = product_info.get('sku')
        item['price'] = product_info.get('listPrice')
        item['condition'] = product_info.get('condition')
        item['availability'] = 'in_stock' if not product_info.get('isSold') else 'out_of_stock'
        item['shutter_count'] = ''.join([attr.get('content') for attr in product_info.get('attributes', []) or [] if
                                         attr.get('name', '').lower() == 'SHUTTER_COUNT'.lower()][
                                        :1]).strip() or response.css(
            '[data-testid="product-details__shutter-count-attribute__title"] strong ::text').get('')
        item['notes'] = ', '.join([r.get('tierDescription') for r in product_info.get('observations', []) or []])
        item['url'] = product_url

        self.current_scrapped_items.append(item)

        # write input into csv file to reduce request in future
        self.write_item_into_csv_file(item={'sku': item['sku'], 'notes': item['notes']})

        yield item

    def errback_handler(self, failure):
        try:
            request_status = failure.value.response.status
        except:
            request_status = None

        self.failed_pages += 1
        self.failed_pages_status.append(request_status)

    def format_scraped_data(self, status="completed", failed_pages=0, duration_seconds=0):
        """
        Convert flat list of product variants into grouped product structure.
        """

        scrape_run_id = str(uuid.uuid4())
        scrape_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        products_map = defaultdict(list)

        for item in self.current_scrapped_items:
            # Normalize product URL (remove /sku-xxxx part)
            base_url = item["url"].split("/sku-")[0]

            try:
                shutter_count = int(item["shutter_count"]) if item["shutter_count"] else None
            except:
                shutter_count = None

            variant = {
                # "url": item["url"], #FOR TESTING
                "sku": item["sku"],
                "price": float(item["price"]) if item["price"] else None,
                "condition": str(item["condition"]).replace("_", " ").title() if item["condition"] else None,
                "availability": item["availability"],
                "shutter_count": shutter_count,
                "notes": item["notes"] if item["notes"] else None
            }

            products_map[(base_url, item["product_title"])].append(variant)

        products = []

        for (product_url, product_title), variants in products_map.items():
            products.append({
                "product_url": product_url,
                "product_title": product_title,
                "variants": variants
            })

        result = {
            "scrape_run_id": scrape_run_id,
            "scrape_timestamp": scrape_timestamp,
            "status": status,
            "stats": {
                "total_products": len(products),
                "total_variants": len(self.current_scrapped_items),
                "failed_pages": failed_pages,
                "duration_seconds": duration_seconds
            },
            "products": products
        }

        # store results for email
        self.summary_data = result

        # to ensure that all  directories are exists
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)

        # Save to JSON file
        with open(self.output_filename, "w", encoding="utf-8") as json_file:
            json.dump(result, json_file, indent=4, ensure_ascii=False)

    def get_first_value(self, row, key, default=None):
        # Safely get the dict, then the list, then the first item
        values = row.get(key, {}).get('values', [])
        return values[0] if values else default

    def get_product_price(self, row):
        try:
            return float(self.get_first_value(row, 'product_price')) / 100
        except:
            return None

    def read_csv_file(self):
        try:
            with open(self.notes_filename, mode='r', encoding='utf-8') as csv_file:
                return list(csv.DictReader(csv_file))
        except:
            return []

    def write_item_into_csv_file(self, item):
        # to ensure that all  directories are exists
        os.makedirs(os.path.dirname(self.notes_filename), exist_ok=True)
        fieldnames = item.keys()

        with open(self.notes_filename, mode='a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if csvfile.tell() == 0:
                writer.writeheader()

            writer.writerow(item)

    def get_configuration_from_txt(self, filename):
        with open(filename, mode='r') as file:
            lines = file.readlines()

        config = {}

        for line in lines:

            try:
                key, value = line.strip().split('==')
                config[key] = value
            except ValueError:
                pass
            except AttributeError:
                pass

        return config

    def build_connection_with_gmail(self):
        sender_app_password = self.gmail_config.get('SENDER_EMAIL_APP_PASSWORD')

        try:
            email_obj = smtplib.SMTP('smtp.gmail.com', 587)
            email_obj.starttls()
            email_obj.login(self.sender_email, sender_app_password)

            self.logger.info('\n\nGmail Authentication successful...!!\n')
            return email_obj

        except Exception as e:
            self.logger.error('\n\nGmail Authentication failed......!!!\nPlease check your login credentials')

    def send_email_to_client(self):
        """
        Send scraping summary email using summary_data dictionary.
        """

        subject, content = self.get_email_body_and_subject()

        # Prepare EmailMessage
        msg = EmailMessage()
        msg['To'] = self.receiver_email
        msg['From'] = self.sender_email
        msg['Subject'] = subject
        msg.add_alternative(content, subtype='html')

        # Send email with retry logic
        for i in range(2):
            try:
                self.email_obj.send_message(msg)
                print(f'\n\nEmail Sent Successfully to {self.receiver_email}\n')
                break
            except Exception as e:
                print(f'Error in sending Email: {e.args}')
                print('Retrying Email Sending...')
                self.email_obj = self.build_connection_with_gmail()

    def get_email_body_and_subject(self):
        total_products = self.summary_data["stats"]["total_products"]
        total_variants = self.summary_data["stats"]["total_variants"]
        failed_pages = self.summary_data["stats"]["failed_pages"]
        duration_seconds = self.summary_data["stats"]["duration_seconds"]
        scrape_timestamp = self.summary_data["scrape_timestamp"]
        scrape_run_id = self.summary_data["scrape_run_id"]
        status = self.summary_data.get("status", "completed").title()

        # Email subject with source
        subject = f"MPB Scrape Summary: {total_products} Products, {total_variants} Variants"

        # HTML content with source mention
        content = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; padding: 20px; border-radius: 8px; background-color: #f9f9f9;">
                    <h2 style="text-align: center; color: #2a7ae2;">📋 Scraping Summary Report</h2>
                    <p><strong>Scrape Run ID:</strong> <code>{scrape_run_id}</code></p>
                    <p><strong>Timestamp:</strong> {scrape_timestamp}</p>
                    <p><strong>Status:</strong> 
                        <span style="
                            color: {'green' if status.lower() == 'completed' else 'red'};
                            font-weight: bold;
                            padding: 3px 8px;
                            border-radius: 5px;
                            background-color: {'#d4edda' if status.lower() == 'completed' else '#f8d7da'};
                        ">
                            {status}
                        </span>
                    </p>

                    <h3 style="color: #2a7ae2;">Statistics</h3>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Products</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{total_products}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Variants</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{total_variants}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Failed Pages</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd; color: {'red' if failed_pages > 0 else 'green'};">
                                {failed_pages}
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Duration (seconds)</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{duration_seconds}</td>
                        </tr>
                    </table>
                </div>
            </body>
        </html>
        """

        return subject, content

    def load_cookies(self):
        # with open("input/mpb_cookies.json", "r", encoding="utf-8") as f:
        #     return json.load(f)

        return {}

    def close(self, reason):
        end_time = datetime.utcnow()
        duration_seconds = int((end_time - self.start_time).total_seconds())

        status = "completed" if reason == "finished" else "failed"

        self.format_scraped_data(
            status=status,
            failed_pages=self.failed_pages,
            duration_seconds=duration_seconds
        )

        self.send_email_to_client()
