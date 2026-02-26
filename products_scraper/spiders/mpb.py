import json
from math import ceil

from scrapy import Request, Selector

from .base import BaseSpider


class MpbSpider(BaseSpider):
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
        #     "products_scraper.middlewares.DataImpulseProxyMiddleware": 350,
        #     "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 400,
        # },

        # ⭐ DataImpulse proxy inside Playwright
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "proxy": {
                    "server": f"http://{BaseSpider.proxy_domain}:{BaseSpider.proxy_port}",
                    "username": BaseSpider.proxy_username,
                    "password": BaseSpider.proxy_password,
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

    variants_found_existing = 0
    details_called = 0

    def start_requests(self):
        # working url below
        url = 'https://www.mpb.com/search-service/product/query/?filter_query[object_type]=product&filter_query[product_condition_star_rating]=%5B1%20TO%205%5D%20AND%20NOT%200&filter_query[model_market]=EU&filter_query[model_available]=true&filter_query[model_is_published_out]=true&field_list=model_name&field_list=model_description&field_list=product_price&field_list=model_url_segment&field_list=product_sku&field_list=product_condition&field_list=product_shutter_count&field_list=product_hour_count&field_list=product_battery_charge_count&field_list=product_id&field_list=product_images&field_list=model_id&field_list=product_price_reduction&field_list=product_price_original&field_list=product_price_modifiers&field_list=model_available_new&sort[product_last_online]=DESC&facet_minimum_count=1&facet_field=model_brand&facet_field=model_category&facet_field=model_product_type&facet_field=product_condition_star_rating&facet_field=product_price&facet_field=%2A&start=0&rows=1000&minimum_match=100%25'
        yield Request(url=url, headers=self.headers,
                      meta={
                          "playwright": True,
                          "playwright_page_methods": [
                              ("wait_for_load_state", "networkidle"),
                          ],
                      },
                      errback=self.errback_handler,
                      )

    def parse(self, response, **kwargs):
        yield from self.parse_products(response)

        try:
            json_data = json.loads(response.css('pre ::text').get(''))
        except:
            json_data = {}

        self.total_results = json_data.get('total_results') or 0
        total_page = ceil(self.total_results / 1000)

        for page_number in range(1, total_page + 1):
            next_page_url = response.url.replace('&start=0', f'&start={page_number * 1000}')
            yield Request(url=next_page_url, headers=self.headers,
                          meta={
                              "playwright": True,
                              "playwright_page_methods": [
                                  ("wait_for_load_state", "networkidle"),
                              ],
                          },
                          callback=self.parse_products, errback=self.errback_handler)

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
                self.variants_found_existing += 1
                print(f"\n\nVariant's notes found in CSV: {self.variants_found_existing}\n\n")
                item['notes'] = self.seen_product_notes_items.get(product_sku)
                self.current_scrapped_items.append(item)
                yield item
                continue

            yield from self.parse_details(product_url=product_url, listing_item=item)

    def parse_details(self, product_url, listing_item):
        self.details_called += 1
        print(f'\n\nNew variant found: {self.details_called}\n\n')

        product_response = self.fetch_product_url_response(product_url)

        # if product request failed then we write listing page data
        if not product_response:
            self.current_scrapped_items.append(listing_item)
            yield listing_item
            return

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

        # to make sure we get the product details
        if not item['product_title']:
            self.current_scrapped_items.append(listing_item)
            yield listing_item
            return

        self.current_scrapped_items.append(item)

        # write notes into csv file to reduce request in future
        self.write_item_into_csv_file(item={'sku': item['sku'], 'notes': item['notes']})

        yield item

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
        print(f"\n\nUsed Existing Variant's Notes: {self.variants_found_existing}\n\n")
        print(f'New Variants Found: {self.details_called}\n\n')
