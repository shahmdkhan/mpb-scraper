copy .env_example, paste inside the products_scraper directory and rename it to .env and fill required fields

install the required packages with the commands: 
pip install -r requirements.txt
playwright install

Go to products_scraper
cd products_scraper

run the MPB scraper
scrapy crawl mpb


Output results will be stored inside /output/mpb
