import csv
import json
import os
import smtplib
from email.message import EmailMessage
import uuid
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

from curl_cffi import requests
from scrapy import Spider


class BaseSpider(Spider):
    name = "base"
    base_url = ''

    load_dotenv('.env')

    proxy = os.getenv('PROXY', '').strip() or ''
    proxy_username, proxy_password = proxy.split('@')[0].split(':')
    proxy_domain, proxy_port = proxy.split('@')[1].split(':')

    proxy = f"http://{proxy}" if proxy else None
    proxies = {"https": proxy} if proxy else None

    headers = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.start_time = datetime.utcnow()
        self.total_results = None
        self.duplicate_skipped_counter = 0
        self.summary_data = {}

        #email settings
        self.sender_email = os.getenv('SENDER_EMAIL')
        self.receiver_email = os.getenv('RECEIVER_EMAIL')
        self.send_email_alert = True if os.getenv('EMAIL_ALERTS_ENABLED', 'false').strip().lower() == 'true' else False
        self.email_obj = self.build_connection_with_gmail()  # Login to Gmail with app password

        #output filenames
        self.mpb_notes_filename = f'mpb_products_notes.csv'
        self.output_filename = f'output/{self.name}/{self.name}_products_{datetime.now().strftime("%d%m%Y%H%M")}.json'
        self.failed_pages_status = []

        #seen products and skus
        self.seen_product_notes_items = {row.get('sku'): row.get('notes') for row in self.read_csv_file() if row.get('sku')}
        self.seen_product_notes_skus = set(self.seen_product_notes_items.keys())

        self.seen_product_urls = []
        self.current_scrapped_items = []
        self.failed_pages = 0

    def start_requests(self):
        pass

    def parse(self, response, **kwargs):
        pass

    def fetch_product_url_response(self, url, max_retries=3, timeout=60):
        """
        Fetch a URL using curl_cffi with retry logic.
        Returns response object if status 200, otherwise None.
        """
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    impersonate="chrome",
                    proxies=self.proxies,
                    timeout=timeout,
                )

                print(f"Attempt {attempt} → Status: {response.status_code} | {url}")

                if response.status_code == 200:
                    return response

            except Exception as e:
                print(f"Attempt {attempt} failed for {url}: {e}")

        print(f"Failed to fetch {url} after {max_retries} attempts.")
        return None

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
                "total_variants_exists": self.total_results,
                "total_products_scrapped": len(products),
                "total_variants_scrapped": len(self.current_scrapped_items),
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
            with open(self.mpb_notes_filename, mode='r', encoding='utf-8') as csv_file:
                return list(csv.DictReader(csv_file))
        except:
            return []

    def write_item_into_csv_file(self, item):
        # to ensure that all  directories are exists
        fieldnames = item.keys()

        with open(self.mpb_notes_filename, mode='a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if csvfile.tell() == 0:
                writer.writeheader()

            writer.writerow(item)

    def build_connection_with_gmail(self):
        if not self.send_email_alert:
            return

        sender_app_password = os.getenv('SENDER_EMAIL_APP_PASSWORD')

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

        #if email send notification disabled then do not send email
        if not self.send_email_alert:
            return

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
        total_products = self.summary_data["stats"]["total_products_scrapped"]
        total_variants = self.summary_data["stats"]["total_variants_scrapped"]
        failed_pages = self.summary_data["stats"]["failed_pages"]
        duration_seconds = self.summary_data["stats"]["duration_seconds"]
        scrape_timestamp = self.summary_data["scrape_timestamp"]
        scrape_run_id = self.summary_data["scrape_run_id"]
        status = self.summary_data.get("status", "completed").title()

        # Email subject with source
        subject = f"{self.name.title()} Scrape Summary: {total_products} Products, {total_variants} Variants"

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
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Variants Exists</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{self.total_results}</td>
                        </tr>

                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Products Scrapped</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{total_products}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Variants Scrapped</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{total_variants}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Duplicate Variants Skipped</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{self.duplicate_skipped_counter}</td>
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
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Output Filename</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{self.output_filename}</td>
                        </tr>

                    </table>
                </div>
            </body>
        </html>
        """

        return subject, content

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
