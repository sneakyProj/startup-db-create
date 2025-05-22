import requests
import json
import time
import re
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import List, Dict, Set
import logging

class WebScraper:
    def __init__(self, config_path: str = 'config.json'):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, self.config['output']['logLevel'].upper()),
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Airtable setup
        self.airtable_headers = {
            'Authorization': f'Bearer {self.config["airtable"]["apiKey"]}',
            'Content-Type': 'application/json'
        }
        
        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['scraping']['userAgent']
        })

    def get_airtable_records(self) -> List[Dict]:
        """Fetch records from Airtable"""
        url = f"https://api.airtable.com/v0/{self.config['airtable']['baseId']}/{self.config['airtable']['tableId']}"
        
        try:
            response = self.session.get(url, headers=self.airtable_headers)
            response.raise_for_status()
            
            records = response.json().get('records', [])
            self.logger.info(f"Retrieved {len(records)} records from Airtable")
            return records
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching Airtable records: {e}")
            return []

    def update_airtable_record(self, record_id: str, scraped_links: List[str]):
        """Update Airtable record with scraped links"""
        if not self.config['output']['updateAirtable']:
            return
            
        url = f"https://api.airtable.com/v0/{self.config['airtable']['baseId']}/{self.config['airtable']['tableId']}"
        
        data = {
            "records": [{
                "id": record_id,
                "fields": {
                    self.config['airtable']['outputColumnName']: '\n'.join(scraped_links)
                }
            }]
        }
        
        try:
            response = self.session.patch(url, headers=self.airtable_headers, json=data)
            response.raise_for_status()
            self.logger.info(f"Updated record {record_id} with {len(scraped_links)} links")
            
        except requests.RequestException as e:
            self.logger.error(f"Error updating Airtable record {record_id}: {e}")

    def scrape_page_links(self, url: str) -> Set[str]:
        """Scrape all links from a webpage"""
        try:
            self.logger.info(f"Scraping links from: {url}")
            
            response = self.session.get(
                url, 
                timeout=self.config['scraping']['timeout'],
                allow_redirects=self.config['scraping']['followRedirects']
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove excluded elements
            for selector in self.config['selectors']['excludeSelectors']:
                for element in soup.select(selector):
                    element.decompose()
            
            links = set()
            
            # Find all links
            for selector in self.config['selectors']['linkSelectors']:
                for element in soup.select(selector):
                    href = element.get('href')
                    if href:
                        # Convert relative URLs to absolute
                        absolute_url = urljoin(url, href)
                        if self.is_valid_link(absolute_url, url):
                            links.add(absolute_url)
            
            # Apply max links limit
            max_links = self.config['scraping']['maxLinksPerPage']
            if max_links > 0:
                links = set(list(links)[:max_links])
            
            self.logger.info(f"Found {len(links)} valid links from {url}")
            return links
            
        except requests.RequestException as e:
            self.logger.error(f"Error scraping {url}: {e}")
            return set()

    def is_valid_link(self, link: str, source_url: str) -> bool:
        """Check if a link meets filtering criteria - LinkedIn links only"""
        try:
            # Must start with the required prefix
            if not link.startswith(self.config['linkFilters']['requiredPrefix']):
                return False
            
            parsed = urlparse(link)
            
            # Basic validation
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # Length check
            if len(link) < self.config['linkFilters']['minLinkLength']:
                return False
            
            # Must be LinkedIn domain
            domain = parsed.netloc.lower()
            if 'linkedin.com' not in domain:
                return False
            
            # Extension filtering
            path = parsed.path.lower()
            if any(path.endswith(ext) for ext in self.config['linkFilters']['blockedExtensions']):
                return False
            
            # Exclude certain LinkedIn URLs that aren't useful
            excluded_paths = ['/login', '/signup', '/help', '/legal', '/privacy', '/cookie-policy']
            if any(excluded in path for excluded in excluded_paths):
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating link {link}: {e}")
            return False

    def run(self):
        """Main execution function - Process only first 10 records"""
        self.logger.info("Starting LinkedIn link scraping process (First 10 records only)")
        
        # Get records from Airtable
        records = self.get_airtable_records()
        
        if not records:
            self.logger.warning("No records found in Airtable")
            return
        
        # Limit to first 10 records
        max_records = self.config['scraping']['maxRecordsToProcess']
        records = records[:max_records]
        self.logger.info(f"Processing first {len(records)} records")
        
        all_results = []
        
        for i, record in enumerate(records, 1):
            record_id = record['id']
            fields = record.get('fields', {})
            source_url = fields.get(self.config['airtable']['linkColumnName'])
            
            if not source_url:
                self.logger.warning(f"No URL found in record {record_id} (row {i})")
                continue
            
            self.logger.info(f"Processing row {i}/{len(records)}: {source_url}")
            
            # Clean URL
            source_url = source_url.strip()
            if not source_url.startswith(('http://', 'https://')):
                source_url = 'https://' + source_url
            
            # Scrape LinkedIn links only
            scraped_links = self.scrape_page_links(source_url)
            linkedin_links = [link for link in scraped_links if link.startswith('https://www.linkedin.com')]
            
            # Store results
            result = {
                'row_number': i,
                'record_id': record_id,
                'source_url': source_url,
                'linkedin_links': linkedin_links,
                'linkedin_links_count': len(linkedin_links)
            }
            all_results.append(result)
            
            # Update Airtable with LinkedIn links only
            self.update_airtable_record(record_id, linkedin_links)
            
            # Delay between requests
            time.sleep(self.config['scraping']['requestDelay'] / 1000)
        
        # Save results to file
        if self.config['output']['saveToFile']:
            with open(self.config['output']['outputPath'], 'w') as f:
                json.dump(all_results, f, indent=2)
            self.logger.info(f"Results saved to {self.config['output']['outputPath']}")
        
        # Summary
        total_linkedin_links = sum(result['linkedin_links_count'] for result in all_results)
        self.logger.info(f"Scraping completed. Processed {len(all_results)} URLs (first 10 rows), found {total_linkedin_links} LinkedIn links total")
        
        # Print summary for each row
        print("\n=== SCRAPING SUMMARY ===")
        for result in all_results:
            print(f"Row {result['row_number']}: {result['source_url']} → {result['linkedin_links_count']} LinkedIn links")
            for link in result['linkedin_links'][:5]:  # Show first 5 links
                print(f"  - {link}")
            if len(result['linkedin_links']) > 5:
                print(f"  ... and {len(result['linkedin_links']) - 5} more")

if __name__ == "__main__":
    scraper = WebScraper()
    scraper.run()
