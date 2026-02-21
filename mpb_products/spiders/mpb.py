import json
import os
from math import ceil
import uuid
from datetime import datetime
from collections import defaultdict

from scrapy import Spider, Request


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
        self.seen_product_urls = []
        self.output_filename = f'output/mpb products {datetime.now().strftime("%d%m%Y%H%M")}.json'
        self.current_scrapped_items = []
        self.start_time = datetime.utcnow()
        self.failed_pages = 0

    def start_requests(self):
        # url = 'https://www.mpb.com/search-service/product/query/?filter_query[object_type]=product&filter_query[product_condition_star_rating]=%5B1%20TO%205%5D%20AND%20NOT%200&filter_query[product_last_online]=%5B2026-02-14T00%3A00%3A00.000Z%20TO%20%2A%5D&filter_query[model_market]=EU&filter_query[model_available]=true&filter_query[model_is_published_out]=true&field_list=model_name&field_list=model_description&field_list=product_price&field_list=model_url_segment&field_list=product_sku&field_list=product_condition&field_list=product_shutter_count&field_list=product_hour_count&field_list=product_battery_charge_count&field_list=product_id&field_list=product_images&field_list=model_id&field_list=product_price_reduction&field_list=product_price_original&field_list=product_price_modifiers&field_list=model_available_new&sort[product_last_online]=DESC&facet_minimum_count=1&facet_field=model_brand&facet_field=model_category&facet_field=model_product_type&facet_field=product_condition_star_rating&facet_field=product_price&facet_field=%2A&start=0&rows=1000&minimum_match=100%25'
        url = 'https://www.mpb.com/search-service/product/query/?filter_query[object_type]=product&filter_query[product_condition_star_rating]=%5B1%20TO%205%5D%20AND%20NOT%200&filter_query[model_market]=EU&filter_query[model_available]=true&filter_query[model_is_published_out]=true&field_list=model_name&field_list=model_description&field_list=product_price&field_list=model_url_segment&field_list=product_sku&field_list=product_condition&field_list=product_shutter_count&field_list=product_hour_count&field_list=product_battery_charge_count&field_list=product_id&field_list=product_images&field_list=model_id&field_list=product_price_reduction&field_list=product_price_original&field_list=product_price_modifiers&field_list=model_available_new&sort[product_last_online]=DESC&facet_minimum_count=1&facet_field=model_brand&facet_field=model_category&facet_field=model_product_type&facet_field=product_condition_star_rating&facet_field=product_price&facet_field=%2A&start=0&rows=1000&minimum_match=100%25'
        yield Request(url=url, headers=self.headers, meta={"playwright": True}, errback=self.errback_handler)

    def parse(self, response, **kwargs):
        yield from self.parse_products(response)

        try:
            json_data = json.loads(response.css('pre ::text').get(''))
        except:
            json_data = {}

        total_results = json_data.get('total_results') or 0
        total_page =  ceil(total_results/1000)

        for page_number in range(1,total_page+1):
            next_page_url = response.url.replace('&start=0',f'&start={page_number*1000}')
            yield Request(url=next_page_url, headers=self.headers, meta={"playwright": True},
                          callback=self.parse_products,errback=self.errback_handler)

    def parse_products(self, response):
        try:
            json_data = json.loads(response.css('pre ::text').get(''))
        except:
            json_data = {}

        results = json_data.get('results') or []

        for row in results:
            product_sku = self.get_first_value(row, 'product_sku')
            if not product_sku:
                continue

            product_slug = self.get_first_value(row, 'model_url_segment')
            product_url = f'https://www.mpb.com/nl-nl/product/{product_slug}/sku-{product_sku}'

            if product_url in self.seen_product_urls:
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

            # yield item
            # self.current_scrapped_items.append(item)
            self.seen_product_urls.append(product_url)

            yield Request(url=product_url, headers=self.headers, meta={"playwright": True, 'row':row},
                          callback=self.parse_details,errback=self.errback_handler)

    def parse_details(self, response):
        try:
            json_data = json.loads(response.css('#__NEXT_DATA__ ::text').get(''))['props']['pageProps']
        except:
            json_data = {}

        model_info = json_data.get('modelInfo', {}) or {}
        product_info = json_data.get('productInfo') or {}

        item = dict()
        item['product_title'] = product_info.get('name') or response.css('.product-name ::text').get('') or model_info.get('brand', {}).get('name')
        item['sku'] = product_info.get('sku')
        item['price'] = product_info.get('listPrice')
        item['condition'] = product_info.get('condition')
        item['availability'] = 'in_stock' if not product_info.get('isSold') else 'out_of_stock'
        item['shutter_count'] = ''.join([attr.get('content') for attr in product_info.get('attributes', []) or [] if attr.get('name', '').lower() == 'SHUTTER_COUNT'.lower()][:1]).strip() or response.css('[data-testid="product-details__shutter-count-attribute__title"] strong ::text').get('')
        item['notes'] = ', '.join([r.get('tierDescription') for r in product_info.get('observations', []) or []])
        item['url'] = response.url

        self.current_scrapped_items.append(item)

        yield item

    def errback_handler(self, failure):
        self.failed_pages += 1

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
                "sku": item["sku"],
                "price": float(item["price"]) if item["price"] else None,
                "condition": item["condition"].replace("_", " ").title(),
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

    def close(self, reason):
        end_time = datetime.utcnow()
        duration_seconds = int((end_time - self.start_time).total_seconds())

        status = "completed" if reason == "finished" else "failed"

        self.format_scraped_data(
            status=status,
            failed_pages=self.failed_pages,
            duration_seconds=duration_seconds
        )
        d=1
