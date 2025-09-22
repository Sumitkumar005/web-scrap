import json
import datetime
import os
import csv
import requests
from bs4 import BeautifulSoup
from slugify import slugify
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm
import concurrent.futures
import logging
import time
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_args():
    parser = argparse.ArgumentParser(description="Scrape university assets")
    parser.add_argument('--csv', required=True, help="Path to CSV file")
    parser.add_argument('--out', default='.', help="Output base directory")
    parser.add_argument('--max-gallery', type=int, default=8, help="Max gallery images")
    parser.add_argument('--concurrency', type=int, default=4, help="Max concurrent tasks")
    parser.add_argument('--timeout', type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument('--ignore-robots', action='store_true', help="Ignore robots.txt")
    return parser.parse_args()

args = parse_args()

if not os.path.exists(args.csv):
    logging.error("CSV file not found")
    exit(1)

API_CONFIG = {
    'API_TOKEN': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzU3NDg5ODg3LCJpYXQiOjE3NTcwNTc4ODYsImp0aSI6ImQ0NWVhNTI4MTZhNjQwZTI4ZGQ3NWZmYzliZWExNThhIiwidXNlcl9pZCI6MjMxMDV9.g4wYgpuwh1kizOIHc_8Br0C9ehXuvtLFYM3_TOmI2Do',  # Replace with valid token
    'CARD_API_URL': 'https://apis.ambitio.in/api/programs/explore',
    'DETAIL_API_URL': 'https://dashboard.ambitio.club/api/programs/college',
    'HEADERS': {
        'Authorization': '',
        'Content-Type': 'application/json'
    }
}

def setup_api_headers():
    API_CONFIG['HEADERS']['Authorization'] = f"Bearer {API_CONFIG['API_TOKEN']}"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_api(url, method='GET', headers=None, params=None, json_data=None):
    if method == 'GET':
        response = requests.get(url, headers=headers, params=params, timeout=args.timeout)
    elif method == 'PUT':
        response = requests.put(url, headers=headers, json=json_data, timeout=args.timeout)
    response.raise_for_status()
    return response.json()

def check_robots(domain, ignore_robots):
    if ignore_robots:
        return True
    try:
        robots_url = f"{domain}/robots.txt"
        response = requests.get(robots_url, timeout=5)
        if response.status_code == 200 and 'Disallow: /' in response.text:
            return False
        return True
    except:
        return True

def validate_image_url(url):
    try:
        head = requests.head(url, timeout=5)
        if head.status_code == 200 and 'image' in head.headers.get('Content-Type', ''):
            return int(head.headers.get('Content-Length', 0)), url
    except:
        pass
    return 0, None

def scrape_assets(university_name):
    setup_api_headers()
    slug = slugify(university_name)
    scraped_at = datetime.datetime.utcnow().isoformat() + 'Z'
    logo_url = ''
    gallery_urls = []

    domain = 'https://ambitio.club'
    if not check_robots(domain, args.ignore_robots):
        logging.warning(f"Robots.txt disallows for {university_name}")
        return {"name": university_name, "slug": slug, "logo_url": logo_url, "gallery_urls": gallery_urls, "scraped_at": scraped_at}

    params = {'offset': 0, 'limit': 1, 'university': university_name, 'courseType': 'Master'}
    try:
        card_data = fetch_api(API_CONFIG['CARD_API_URL'], 'GET', API_CONFIG['HEADERS'], params)
        if not card_data.get('data', {}).get('results'):
            logging.warning(f"No data for {university_name}")
            return {"name": university_name, "slug": slug, "logo_url": logo_url, "gallery_urls": gallery_urls, "scraped_at": scraped_at}

        course_id = str(card_data['data']['results'][0]['id'])
        payload = {"id": course_id}
        details = fetch_api(API_CONFIG['DETAIL_API_URL'], 'PUT', API_CONFIG['HEADERS'], json_data=payload)
        data = details.get('data', {})
        university = data.get('university', {})
        name = university.get('name', university_name)
        logo_path = university.get('logo', '')
        if logo_path:
            logo_url_candidate = logo_path if logo_path.startswith('http') else f"https://ambitio-django-backend-media.s3.amazonaws.com/{logo_path}"
            _, logo_url = validate_image_url(logo_url_candidate)
        if not logo_url:
            fallback_logo = f"https://ambitio-django-backend-media.s3.amazonaws.com/programs/university/logo/{slug}.jpg"
            _, logo_url = validate_image_url(fallback_logo)

        gallery_candidates = university.get('galleryImages', [])
        valid_gallery = []
        for g in gallery_candidates:
            size, valid_url = validate_image_url(g)
            if valid_url and size > 0:
                valid_gallery.append((size, valid_url))
        gallery_urls = [url for _, url in sorted(valid_gallery, reverse=True)][:args.max_gallery]

        if not gallery_urls:
            page_url = f"{domain}/college/{slug}"
            response = requests.get(page_url, headers=API_CONFIG['HEADERS'], timeout=args.timeout)
            soup = BeautifulSoup(response.text, 'lxml')
            scraped_candidates = []
            for img in soup.find_all('img'):
                src = img.get('src', '')
                if 'gallery-images' in src or 'university/gallery' in src:
                    if src.startswith('//'):
                        src = 'https:' + src
                    elif src.startswith('/'):
                        src = domain + src
                    size, valid_url = validate_image_url(src)
                    if valid_url and size > 0:
                        scraped_candidates.append((size, valid_url))
            gallery_urls = [url for _, url in sorted(scraped_candidates, reverse=True)][:args.max_gallery]
    except Exception as e:
        logging.error(f"Error for {university_name}: {e}")

    return {"name": name, "slug": slug, "logo_url": logo_url or '', "gallery_urls": gallery_urls, "scraped_at": scraped_at}

def download_image(url, path):
    try:
        response = requests.get(url, stream=True, timeout=args.timeout)
        response.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        return True
    except Exception as e:
        logging.error(f"Download error {url}: {e}")
        return False

def main():
    with open(args.csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        universities = [row['University'] for row in reader if 'University' in row]

    if not universities:
        logging.error("No valid universities in CSV")
        exit(1)

    unique_slugs = {}
    for uni in universities:
        slug = slugify(uni)
        if slug not in unique_slugs:
            unique_slugs[slug] = uni

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {executor.submit(scrape_assets, uni): uni for slug, uni in unique_slugs.items()}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Scraping"):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                uni = futures[future]
                logging.error(f"Failed {uni}: {e}")
            time.sleep(1)  # Polite delay

    master = {
        "generated_at": datetime.datetime.utcnow().isoformat() + 'Z',
        "total_universities": len(results),
        "universities": sorted(results, key=lambda x: x['slug'])
    }

    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir = os.path.join(args.out, timestamp)
    os.makedirs(out_dir, exist_ok=True)
    master_path = os.path.join(out_dir, 'master.json')
    with open(master_path, 'w', encoding='utf-8') as f:
        json.dump(master, f, indent=4)

    for res in results:
        slug_dir = os.path.join(out_dir, res['slug'])
        os.makedirs(slug_dir, exist_ok=True)
        uni_json_path = os.path.join(slug_dir, f"{res['slug']}.json")
        with open(uni_json_path, 'w', encoding='utf-8') as f:
            json.dump(res, f, indent=4)
        if res['logo_url']:
            ext = res['logo_url'].split('.')[-1].split('?')[0] or 'jpg'
            logo_path = os.path.join(slug_dir, f"logo.{ext}")
            if download_image(res['logo_url'], logo_path):
                logging.info(f"Downloaded logo for {res['name']}")
            else:
                logging.warning(f"Failed logo download for {res['name']}")
        gallery_dir = os.path.join(slug_dir, 'gallery')
        os.makedirs(gallery_dir, exist_ok=True)
        if res['gallery_urls']:
            for i, url in enumerate(res['gallery_urls']):
                ext = url.split('.')[-1].split('?')[0] or 'jpg'
                img_path = os.path.join(gallery_dir, f"{i+1}.{ext}")
                if download_image(url, img_path):
                    logging.info(f"Downloaded gallery image {i+1} for {res['name']}")
                else:
                    logging.warning(f"Failed gallery image {i+1} for {res['name']}")
        else:
            logging.warning(f"No gallery for {res['name']}")

    logging.info(f"Total processed: {len(results)}. Failures: {len(unique_slugs) - len(results)}")

if __name__ == "__main__":
    main()