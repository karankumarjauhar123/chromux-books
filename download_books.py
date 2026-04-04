"""
Chromux Books Downloader
========================
Downloads books from multiple sources and saves them locally.
Supported sources: Project Gutenberg, Archive.org, Hindwi.org, Rekhta.org

Updates books.json with CDN URLs after successful downloads.
"""

import json
import os
import re
import requests
import time
import io
import PyPDF2
from urllib.parse import quote as url_quote

# Try importing BeautifulSoup (required for Hindwi/Rekhta scraping)
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    print("WARNING: beautifulsoup4 not installed. Hindwi.org and Rekhta.org books will be skipped.")
    print("Install with: pip install beautifulsoup4")
    HAS_BS4 = False

# --- Configuration ---
CDN_BASE_URL = "https://books.chromux.in/books_content"
CONTENT_DIR = "books_content"
API_DIR = "api"
BOOKS_FILE = os.path.join(API_DIR, "books.json")
MIN_FILE_SIZE = 500  # Minimum bytes to consider a file valid

# Common headers to avoid being blocked by websites
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36 ChromuxBrowser/2.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
}

# --- Stats Tracking ---
stats = {
    "downloaded": 0,
    "skipped_cached": 0,
    "skipped_duplicate": 0,
    "failed": 0,
    "failed_books": [],
}


def retry_request(url, max_retries=3, timeout=20, **kwargs):
    """Make an HTTP request with exponential backoff retry logic."""
    headers = kwargs.pop("headers", HEADERS.copy())
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout, **kwargs)
            if response.status_code == 200:
                return response
            if response.status_code == 429:  # Rate limited
                wait = (2 ** attempt) * 3
                print(f"    Rate limited. Waiting {wait}s before retry...")
                time.sleep(wait)
                continue
            if response.status_code >= 500:  # Server error, retry
                wait = 2 ** attempt
                print(f"    Server error {response.status_code}. Retrying in {wait}s...")
                time.sleep(wait)
                continue
            # Client error (4xx), don't retry
            return response
        except requests.exceptions.Timeout:
            wait = 2 ** attempt
            print(f"    Timeout on attempt {attempt + 1}. Retrying in {wait}s...")
            time.sleep(wait)
        except requests.exceptions.ConnectionError:
            wait = 2 ** attempt
            print(f"    Connection error on attempt {attempt + 1}. Retrying in {wait}s...")
            time.sleep(wait)
        except Exception as e:
            print(f"    Request error: {e}")
            return None
    return None


def is_already_downloaded(dest_path):
    """Check if a file already exists and is large enough to be valid."""
    return os.path.exists(dest_path) and os.path.getsize(dest_path) > MIN_FILE_SIZE


def save_text_file(dest_path, text_content):
    """Save text content to a UTF-8 file."""
    with open(dest_path, 'w', encoding='utf-8') as f:
        f.write(text_content)


def save_binary_file(dest_path, content):
    """Save binary content to a file."""
    with open(dest_path, 'wb') as f:
        f.write(content)


# ============================================
# Source-specific downloaders
# ============================================

def download_gutenberg(book, book_id, content_dir):
    """Download a book from Project Gutenberg."""
    dest_filename = f"{book_id}.epub"
    dest_path = os.path.join(content_dir, dest_filename)

    if is_already_downloaded(dest_path):
        print(f"  ✓ Already cached: {book['title']}")
        stats["skipped_cached"] += 1
        book['read_url'] = f"{CDN_BASE_URL}/{dest_filename}"
        return True

    # Try multiple Gutenberg EPUB URL patterns
    possible_urls = [
        f"https://www.gutenberg.org/ebooks/{book_id}.epub.images",
        f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}-images.epub",
        f"https://www.gutenberg.org/ebooks/{book_id}.epub.noimages",
        f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.epub",
    ]

    for url in possible_urls:
        response = retry_request(url, timeout=15)
        if response and response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            # Make sure we got an actual epub, not an HTML error page
            if 'text/html' not in content_type:
                save_binary_file(dest_path, response.content)
                book['read_url'] = f"{CDN_BASE_URL}/{dest_filename}"
                stats["downloaded"] += 1
                print(f"  ✓ Downloaded from Gutenberg")
                time.sleep(1)
                return True
        time.sleep(0.5)

    return False


def download_archive_org(book, book_id, original_url, content_dir):
    """Download a book from Archive.org."""
    identifier = original_url.split('/details/')[-1].split('/')[0]
    metadata_url = f"https://archive.org/metadata/{identifier}"

    meta_resp = retry_request(metadata_url, timeout=15)
    if not meta_resp or meta_resp.status_code != 200:
        print(f"  ✗ Archive.org metadata failed")
        return False

    files = meta_resp.json().get('files', [])
    format_priority = ['.epub', '.pdf', '.txt']

    best_file = None
    best_ext = None
    for ext in format_priority:
        matches = [f for f in files if f.get('name', '').endswith(ext)]
        if matches:
            best_file = matches[0]['name']
            best_ext = ext
            break

    if not best_file:
        print(f"  ✗ No EPUB/PDF/TXT found on Archive.org")
        return False

    # If PDF, we'll extract text and save as .txt
    dest_filename = f"{book_id}.txt" if best_ext == '.pdf' else f"{book_id}{best_ext}"
    dest_path = os.path.join(content_dir, dest_filename)

    if is_already_downloaded(dest_path):
        print(f"  ✓ Already cached: {book['title']}")
        stats["skipped_cached"] += 1
        book['read_url'] = f"{CDN_BASE_URL}/{dest_filename}"
        return True

    download_url = f"https://archive.org/download/{identifier}/{url_quote(best_file)}"
    response = retry_request(download_url, timeout=30)

    if not response or response.status_code != 200:
        print(f"  ✗ Download failed from Archive.org")
        return False

    if best_ext == '.pdf':
        try:
            reader = PyPDF2.PdfReader(io.BytesIO(response.content))
            text_parts = [page.extract_text() or '' for page in reader.pages]
            text = '\n'.join(text_parts).strip()
            if len(text) < 100:
                print(f"  ✗ PDF text extraction yielded too little content")
                return False
            save_text_file(dest_path, text)
        except Exception as e:
            print(f"  ✗ PDF extraction error: {e}")
            return False
    else:
        save_binary_file(dest_path, response.content)

    book['read_url'] = f"{CDN_BASE_URL}/{dest_filename}"
    stats["downloaded"] += 1
    print(f"  ✓ Downloaded from Archive.org ({best_ext})")
    time.sleep(2)
    return True


def scrape_hindwi(book, book_id, original_url, content_dir):
    """Scrape text content from hindwi.org."""
    if not HAS_BS4:
        print(f"  ✗ Skipped (beautifulsoup4 not installed)")
        return False

    dest_filename = f"{book_id}.txt"
    dest_path = os.path.join(content_dir, dest_filename)

    if is_already_downloaded(dest_path):
        print(f"  ✓ Already cached: {book['title']}")
        stats["skipped_cached"] += 1
        book['read_url'] = f"{CDN_BASE_URL}/{dest_filename}"
        return True

    response = retry_request(original_url, timeout=20)
    if not response or response.status_code != 200:
        print(f"  ✗ Failed to fetch hindwi.org page")
        return False

    soup = BeautifulSoup(response.content, 'html.parser')

    # Hindwi.org stores content in article/main content areas
    # Try multiple CSS selectors that might contain the content
    content_selectors = [
        'article .entry-content',
        '.poem-content',
        '.story-content',
        '.novel-content',
        '.content-area article',
        'article',
        '.entry-content',
        'main .content',
        '#content',
        '.post-content',
    ]

    text_content = ""
    for selector in content_selectors:
        elements = soup.select(selector)
        if elements:
            # Get all text from matching elements
            parts = []
            for el in elements:
                # Remove script/style/nav elements
                for unwanted in el.select('script, style, nav, header, footer, .sidebar, .comments, .share-buttons, .related-posts'):
                    unwanted.decompose()
                parts.append(el.get_text(separator='\n', strip=True))
            text_content = '\n\n'.join(parts).strip()
            if len(text_content) > 200:
                break

    # Fallback: try getting all <p> tags from the page body
    if len(text_content) < 200:
        paragraphs = soup.find_all('p')
        text_content = '\n\n'.join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20)

    if len(text_content) < 200:
        print(f"  ✗ Could not extract enough content from hindwi.org (got {len(text_content)} chars)")
        return False

    # Add title and author header
    header = f"{'=' * 60}\n{book['title']}\n{book['author']}\nSource: {original_url}\n{'=' * 60}\n\n"
    save_text_file(dest_path, header + text_content)

    book['read_url'] = f"{CDN_BASE_URL}/{dest_filename}"
    stats["downloaded"] += 1
    print(f"  ✓ Scraped from hindwi.org ({len(text_content)} chars)")
    time.sleep(2)
    return True


def scrape_rekhta(book, book_id, original_url, content_dir):
    """Scrape text content from rekhta.org."""
    if not HAS_BS4:
        print(f"  ✗ Skipped (beautifulsoup4 not installed)")
        return False

    dest_filename = f"{book_id}.txt"
    dest_path = os.path.join(content_dir, dest_filename)

    if is_already_downloaded(dest_path):
        print(f"  ✓ Already cached: {book['title']}")
        stats["skipped_cached"] += 1
        book['read_url'] = f"{CDN_BASE_URL}/{dest_filename}"
        return True

    response = retry_request(original_url, timeout=20)
    if not response or response.status_code != 200:
        print(f"  ✗ Failed to fetch rekhta.org page")
        return False

    soup = BeautifulSoup(response.content, 'html.parser')

    # Rekhta.org content selectors
    content_selectors = [
        '.storyContent',
        '.poemContent',
        '.contentBody',
        '.fullStoryContent',
        '.mainContentBody',
        'article .content',
        '.entry-content',
        'article',
        '#mainContent',
    ]

    text_content = ""
    for selector in content_selectors:
        elements = soup.select(selector)
        if elements:
            parts = []
            for el in elements:
                for unwanted in el.select('script, style, nav, header, footer, .sidebar, .comments, .share-buttons, .ad-container'):
                    unwanted.decompose()
                parts.append(el.get_text(separator='\n', strip=True))
            text_content = '\n\n'.join(parts).strip()
            if len(text_content) > 200:
                break

    # Fallback: get all paragraphs and divs with Hindi text
    if len(text_content) < 200:
        hindi_pattern = re.compile(r'[\u0900-\u097F]')
        all_text_blocks = soup.find_all(['p', 'div'])
        hindi_blocks = []
        for block in all_text_blocks:
            text = block.get_text(strip=True)
            if hindi_pattern.search(text) and len(text) > 30:
                hindi_blocks.append(text)
        text_content = '\n\n'.join(hindi_blocks)

    if len(text_content) < 200:
        print(f"  ✗ Could not extract enough content from rekhta.org (got {len(text_content)} chars)")
        return False

    # Add title and author header
    header = f"{'=' * 60}\n{book['title']}\n{book['author']}\nSource: {original_url}\n{'=' * 60}\n\n"
    save_text_file(dest_path, header + text_content)

    book['read_url'] = f"{CDN_BASE_URL}/{dest_filename}"
    stats["downloaded"] += 1
    print(f"  ✓ Scraped from rekhta.org ({len(text_content)} chars)")
    time.sleep(2)
    return True


# ============================================
# Main orchestrator
# ============================================

def process_book(book, content_dir, processed_ids):
    """Process a single book entry — route to the correct downloader."""
    book_id = str(book['id'])
    original_url = book.get('read_url', '')

    # Skip duplicate book IDs (same book can appear in multiple sections)
    if book_id in processed_ids:
        # Still update the URL if a previous download was successful
        cached_url = processed_ids[book_id]
        if cached_url:
            book['read_url'] = cached_url
        stats["skipped_duplicate"] += 1
        return

    success = False

    if 'gutenberg.org' in original_url:
        success = download_gutenberg(book, book_id, content_dir)

    elif 'archive.org/details/' in original_url:
        success = download_archive_org(book, book_id, original_url, content_dir)

    elif 'hindwi.org' in original_url:
        success = scrape_hindwi(book, book_id, original_url, content_dir)

    elif 'rekhta.org' in original_url:
        success = scrape_rekhta(book, book_id, original_url, content_dir)

    else:
        print(f"  ✗ Unsupported source: {original_url}")

    if success:
        # Cache the CDN URL for duplicate handling
        processed_ids[book_id] = book.get('read_url', '')
    else:
        if book_id not in processed_ids:
            stats["failed"] += 1
            stats["failed_books"].append(f"{book['title']} ({original_url})")
            processed_ids[book_id] = None


def main():
    os.makedirs(CONTENT_DIR, exist_ok=True)

    # Use utf-8-sig to automatically handle potential BOM from Windows
    with open(BOOKS_FILE, 'r', encoding='utf-8-sig') as f:
        data = json.load(f)

    sections = data.get('sections', [])
    total_books = sum(len(s.get('books', [])) for s in sections)

    print("=" * 60)
    print("  Chromux Books Downloader v2.0")
    print(f"  Sections: {len(sections)} | Total Books: {total_books}")
    print(f"  Sources: Gutenberg, Archive.org, Hindwi.org, Rekhta.org")
    print("=" * 60)
    print()

    processed_ids = {}  # Track processed book IDs to avoid duplicates
    book_counter = 0

    for section in sections:
        section_title = section.get('title', 'Unknown Section')
        section_books = section.get('books', [])
        print(f"\n📚 {section_title} ({len(section_books)} books)")
        print("-" * 50)

        for book in section_books:
            book_counter += 1
            print(f"\n[{book_counter}/{total_books}] {book['title']} — {book['author']}")
            process_book(book, CONTENT_DIR, processed_ids)

    # Save the updated JSON back
    with open(BOOKS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Print summary
    print("\n" + "=" * 60)
    print("  DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"  ✓ New downloads:    {stats['downloaded']}")
    print(f"  ✓ Already cached:   {stats['skipped_cached']}")
    print(f"  ↔ Duplicates:       {stats['skipped_duplicate']}")
    print(f"  ✗ Failed:           {stats['failed']}")
    print(f"  Total processed:    {book_counter}")

    if stats["failed_books"]:
        print(f"\n  Failed books:")
        for fb in stats["failed_books"]:
            print(f"    • {fb}")

    print("\n" + "=" * 60)
    print("  books.json updated with CDN URLs.")
    print("=" * 60)


if __name__ == '__main__':
    main()
