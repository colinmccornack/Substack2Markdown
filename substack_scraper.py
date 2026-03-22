import argparse
import json
import os
import shutil
import subprocess
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from time import sleep

from bs4 import BeautifulSoup
import html2text
import markdown
import requests
from tqdm import tqdm
from xml.etree import ElementTree as ET

from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse
from config import EMAIL, PASSWORD

USE_PREMIUM: bool = False  # Set to True if you want to login to Substack and convert paid for posts
BASE_SUBSTACK_URL: str = "https://www.thefitzwilliam.com/"  # Substack you want to convert to markdown
BASE_MD_DIR: str = "substack_md_files"  # Name of the directory we'll save the .md essay files
BASE_HTML_DIR: str = "substack_html_pages"  # Name of the directory we'll save the .html essay files
HTML_TEMPLATE: str = "author_template.html"  # HTML template to use for the author page
JSON_DATA_DIR: str = "data"
NUM_POSTS_TO_SCRAPE: int = 3  # Set to 0 if you want all posts
EPUB_DIR: str = "epub_output"  # Directory to save generated EPUB files
EPUB_CSS: str = "assets/css/essay-styles.css"  # Stylesheet to embed in the EPUB (set to "" to skip)


def extract_main_part(url: str) -> str:
    parts = urlparse(url).netloc.split('.')
    return parts[1] if parts[0] == 'www' else parts[0]


def generate_html_file(author_name: str) -> None:
    """
    Generates a HTML file for the given author.
    """
    if not os.path.exists(BASE_HTML_DIR):
        os.makedirs(BASE_HTML_DIR)

    json_path = os.path.join(JSON_DATA_DIR, f'{author_name}.json')
    with open(json_path, 'r', encoding='utf-8') as file:
        essays_data = json.load(file)

    embedded_json_data = json.dumps(essays_data, ensure_ascii=False, indent=4)

    with open(HTML_TEMPLATE, 'r', encoding='utf-8') as file:
        html_template = file.read()

    html_with_data = html_template.replace(
        '<!-- AUTHOR_NAME -->', author_name
    ).replace(
        '<script type="application/json" id="essaysData"></script>',
        f'<script type="application/json" id="essaysData">{embedded_json_data}</script>'
    )
    html_with_author = html_with_data.replace('author_name', author_name)

    html_output_path = os.path.join(BASE_HTML_DIR, f'{author_name}.html')
    with open(html_output_path, 'w', encoding='utf-8') as file:
        file.write(html_with_author)


def generate_epub(author_name: str, epub_dir: str = EPUB_DIR, css_path: str = EPUB_CSS) -> None:
    """
    Reads the author's JSON data file, sorts posts chronologically, and uses
    pandoc to combine the saved HTML files into a single EPUB.

    Requirements:
        - pandoc must be installed and on your PATH (https://pandoc.org/installing.html)
        - HTML files must already exist (i.e. scrape_posts() has been run)

    Args:
        author_name: The writer_name used when scraping (derived from the subdomain).
        epub_dir:    Directory where the .epub file will be saved.
        css_path:    Path to a CSS file to embed in the EPUB. Pass "" to skip.
    """
    if not shutil.which("pandoc"):
        print("Warning: pandoc not found on PATH — skipping EPUB generation.")
        print("Install it from https://pandoc.org/installing.html and re-run.")
        return

    json_path = os.path.join(JSON_DATA_DIR, f"{author_name}.json")
    if not os.path.exists(json_path):
        print(f"Warning: no JSON data found at {json_path} — skipping EPUB generation.")
        return

    with open(json_path, "r", encoding="utf-8") as f:
        essays = json.load(f)

    if not essays:
        print("Warning: JSON data is empty — skipping EPUB generation.")
        return

    # Sort chronologically; essays with unparseable dates fall to the end
    def parse_date(essay):
        from datetime import datetime
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(essay.get("date", ""), fmt)
            except ValueError:
                continue
        return datetime.max

    essays.sort(key=parse_date)

    # Only include essays whose HTML file actually exists on disk
    html_files = []
    for essay in essays:
        html_link = essay.get("html_link", "")
        if os.path.exists(html_link):
            html_files.append(html_link)
        else:
            print(f"  Skipping missing file: {html_link}")

    if not html_files:
        print("Warning: no HTML files found on disk — skipping EPUB generation.")
        return

    if not os.path.exists(epub_dir):
        os.makedirs(epub_dir)

    epub_path = os.path.join(epub_dir, f"{author_name}.epub")

    cmd = [
        "pandoc",
        *html_files,
        "--metadata", f"title={author_name}",
        "--toc",           # navigable table of contents
        "--toc-depth=1",   # one entry per post (each post starts with an <h1>)
        "--split-level=1", # each <h1> becomes its own EPUB chapter/spine item
        "-o", epub_path,
    ]

    if css_path and os.path.exists(css_path):
        cmd += ["--css", css_path]
    elif css_path:
        print(f"  Note: CSS file not found at '{css_path}' — embedding without stylesheet.")

    print(f"\nGenerating EPUB for '{author_name}' from {len(html_files)} posts...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"EPUB saved to: {epub_path}")
        else:
            print(f"pandoc error (exit {result.returncode}):\n{result.stderr}")
    except Exception as e:
        print(f"Failed to run pandoc: {e}")


class BaseSubstackScraper(ABC):
    def __init__(self, base_substack_url: str, md_save_dir: str, html_save_dir: str):
        if not base_substack_url.endswith("/"):
            base_substack_url += "/"
        self.base_substack_url: str = base_substack_url

        self.writer_name: str = extract_main_part(base_substack_url)
        md_save_dir: str = f"{md_save_dir}/{self.writer_name}"

        self.md_save_dir: str = md_save_dir
        self.html_save_dir: str = f"{html_save_dir}/{self.writer_name}"

        if not os.path.exists(md_save_dir):
            os.makedirs(md_save_dir)
            print(f"Created md directory {md_save_dir}")
        if not os.path.exists(self.html_save_dir):
            os.makedirs(self.html_save_dir)
            print(f"Created html directory {self.html_save_dir}")

        self.keywords: List[str] = ["about", "archive", "podcast"]
        self.post_urls: List[str] = self.get_all_post_urls()

    def get_all_post_urls(self) -> List[str]:
        urls = self.fetch_urls_from_sitemap()
        if not urls:
            urls = self.fetch_urls_from_feed()
        return self.filter_urls(urls, self.keywords)

    def fetch_urls_from_sitemap(self) -> List[str]:
        sitemap_url = f"{self.base_substack_url}sitemap.xml"
        response = requests.get(sitemap_url)

        if not response.ok:
            print(f'Error fetching sitemap at {sitemap_url}: {response.status_code}')
            return []

        root = ET.fromstring(response.content)
        urls = [element.text for element in root.iter('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')]
        return urls

    def fetch_urls_from_feed(self) -> List[str]:
        print('Falling back to feed.xml. This will only contain up to the 22 most recent posts.')
        feed_url = f"{self.base_substack_url}feed.xml"
        response = requests.get(feed_url)

        if not response.ok:
            print(f'Error fetching feed at {feed_url}: {response.status_code}')
            return []

        root = ET.fromstring(response.content)
        urls = []
        for item in root.findall('.//item'):
            link = item.find('link')
            if link is not None and link.text:
                urls.append(link.text)

        return urls

    @staticmethod
    def filter_urls(urls: List[str], keywords: List[str]) -> List[str]:
        return [url for url in urls if all(keyword not in url for keyword in keywords)]

    @staticmethod
    def html_to_md(html_content: str) -> str:
        if not isinstance(html_content, str):
            raise ValueError("html_content must be a string")
        h = html2text.HTML2Text()
        h.ignore_links = False
        h.body_width = 0
        return h.handle(html_content)

    @staticmethod
    def save_to_file(filepath: str, content: str) -> None:
        if not isinstance(filepath, str):
            raise ValueError("filepath must be a string")
        if not isinstance(content, str):
            raise ValueError("content must be a string")
        if os.path.exists(filepath):
            print(f"File already exists: {filepath}")
            return
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(content)

    @staticmethod
    def md_to_html(md_content: str) -> str:
        return markdown.markdown(md_content, extensions=['extra'])

    def save_to_html_file(self, filepath: str, content: str) -> None:
        if not isinstance(filepath, str):
            raise ValueError("filepath must be a string")
        if not isinstance(content, str):
            raise ValueError("content must be a string")

        html_dir = os.path.dirname(filepath)
        css_path = os.path.relpath("./assets/css/essay-styles.css", html_dir)
        css_path = css_path.replace("\\", "/")

        html_content = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Markdown Content</title>
                <link rel="stylesheet" href="{css_path}">
            </head>
            <body>
                <main class="markdown-content">
                {content}
                </main>
            </body>
            </html>
        """

        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(html_content)

    @staticmethod
    def get_filename_from_url(url: str, filetype: str = ".md") -> str:
        if not isinstance(url, str):
            raise ValueError("url must be a string")
        if not isinstance(filetype, str):
            raise ValueError("filetype must be a string")
        if not filetype.startswith("."):
            filetype = f".{filetype}"
        return url.split("/")[-1] + filetype

    @staticmethod
    def combine_metadata_and_content(title: str, subtitle: str, date: str, like_count: str, content) -> str:
        if not isinstance(title, str):
            raise ValueError("title must be a string")
        if not isinstance(content, str):
            raise ValueError("content must be a string")

        metadata = f"# {title}\n\n"
        if subtitle:
            metadata += f"## {subtitle}\n\n"
        metadata += f"**{date}**\n\n"
        metadata += f"**Likes:** {like_count}\n\n"

        return metadata + content

    def download_image(self, url: str, post_name: str) -> str:
        """
        Downloads an image to a shared images directory at the project root and
        returns an absolute path so both markdown and HTML files can reference it
        regardless of where they are saved on disk.

        Images are organised as: images/{writer_name}/{post_name}/{filename}
        """
        img_dir = os.path.join("images", self.writer_name, post_name)
        if not os.path.exists(img_dir):
            os.makedirs(img_dir)

        # Strip query params to get a clean filename; fall back if none is found
        filename = url.split("/")[-1].split("?")[0]
        if not filename or "." not in filename:
            filename = "image.jpg"

        filepath = os.path.join(img_dir, filename)

        if not os.path.exists(filepath):
            try:
                r = requests.get(url, stream=True, timeout=10)
                if r.status_code == 200:
                    with open(filepath, 'wb') as f:
                        for chunk in r.iter_content(1024):
                            f.write(chunk)
                else:
                    print(f"  Could not download image {url}: HTTP {r.status_code}")
                    return url
            except Exception as e:
                print(f"  Could not download image {url}: {e}")
                return url

        # Absolute path ensures the reference works from any file location on disk
        return os.path.abspath(filepath)

    def extract_post_data(self, soup: BeautifulSoup, post_url: str) -> Tuple[str, str, str, str, str]:
        title = soup.select_one("h1.post-title, h2").text.strip()

        subtitle_element = soup.select_one("h3.subtitle")
        subtitle = subtitle_element.text.strip() if subtitle_element else ""

        date_selector = ".pencraft.pc-display-flex.pc-gap-4.pc-reset .pencraft"
        date_element = soup.select_one(date_selector)
        date = date_element.text.strip() if date_element else "Date not available"

        post_name = post_url.split("/")[-1]
        content_div = soup.select_one("div.available-content")

        if content_div:
            for img in content_div.find_all("img"):
                original_url = img.get("src")
                if original_url:
                    local_path = self.download_image(original_url, post_name)
                    img["src"] = local_path
            content_html = str(content_div)
        else:
            content_html = "<p>No content found. Ensure you are logged in.</p>"

        md = self.html_to_md(content_html)

        like_count_element = soup.select_one("a.post-ufi-button .label")
        like_count = like_count_element.text.strip() if like_count_element else "0"

        md_content = self.combine_metadata_and_content(title, subtitle, date, like_count, md)
        return title, subtitle, like_count, date, md_content

    @abstractmethod
    def get_url_soup(self, url: str) -> str:
        raise NotImplementedError

    def save_essays_data_to_json(self, essays_data: list) -> None:
        data_dir = os.path.join(JSON_DATA_DIR)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        json_path = os.path.join(data_dir, f'{self.writer_name}.json')
        if os.path.exists(json_path):
            with open(json_path, 'r', encoding='utf-8') as file:
                existing_data = json.load(file)
            essays_data = existing_data + [data for data in essays_data if data not in existing_data]
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(essays_data, f, ensure_ascii=False, indent=4)

    def scrape_posts(self, num_posts_to_scrape: int = 0, make_epub: bool = True) -> None:
        essays_data = []
        count = 0
        total = num_posts_to_scrape if num_posts_to_scrape != 0 else len(self.post_urls)
        for url in tqdm(self.post_urls, total=total):
            try:
                md_filename = self.get_filename_from_url(url, filetype=".md")
                html_filename = self.get_filename_from_url(url, filetype=".html")
                md_filepath = os.path.join(self.md_save_dir, md_filename)
                html_filepath = os.path.join(self.html_save_dir, html_filename)

                if not os.path.exists(md_filepath):
                    soup = self.get_url_soup(url)
                    if soup is None:
                        total += 1
                        continue
                    title, subtitle, like_count, date, md = self.extract_post_data(soup, url)
                    self.save_to_file(md_filepath, md)

                    html_content = self.md_to_html(md)
                    self.save_to_html_file(html_filepath, html_content)

                    essays_data.append({
                        "title": title,
                        "subtitle": subtitle,
                        "like_count": like_count,
                        "date": date,
                        "file_link": md_filepath,
                        "html_link": html_filepath
                    })
                else:
                    print(f"File already exists: {md_filepath}")
            except Exception as e:
                print(f"Error scraping post: {e}")
            count += 1
            if num_posts_to_scrape != 0 and count == num_posts_to_scrape:
                break
        self.save_essays_data_to_json(essays_data=essays_data)
        generate_html_file(author_name=self.writer_name)
        if make_epub:
            generate_epub(author_name=self.writer_name)


class SubstackScraper(BaseSubstackScraper):
    def __init__(self, base_substack_url: str, md_save_dir: str, html_save_dir: str):
        super().__init__(base_substack_url, md_save_dir, html_save_dir)

    def get_url_soup(self, url: str) -> Optional[BeautifulSoup]:
        try:
            page = requests.get(url, headers=None)
            soup = BeautifulSoup(page.content, "html.parser")
            if soup.find("h2", class_="paywall-title"):
                print(f"Skipping premium article: {url}")
                return None
            return soup
        except Exception as e:
            raise ValueError(f"Error fetching page: {e}") from e


class PremiumSubstackScraper(BaseSubstackScraper):
    def __init__(
            self,
            base_substack_url: str,
            md_save_dir: str,
            html_save_dir: str,
            headless: bool = False,
            user_agent: str = '',
            **kwargs
    ) -> None:
        # NOTE: super().__init__() triggers get_all_post_urls(), which calls get_url_soup(),
        # so the browser must be fully initialized and logged in before calling super().__init__().
        # We set up Selenium first, then call super().
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        options = ChromeOptions()

        # Persist the Chrome profile so you stay logged in between runs
        script_dir = os.path.dirname(os.path.abspath(__file__))
        profile_path = os.path.join(script_dir, "selenium_profile")
        options.add_argument(f"user-data-dir={profile_path}")
        options.add_argument("--profile-directory=Default")

        # Strip out signals that reveal Selenium to Cloudflare/Substack
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        if headless:
            options.add_argument("--headless=new")

        # Use a realistic user agent; default headless agents are often blocked
        if user_agent:
            options.add_argument(f'user-agent={user_agent}')
        else:
            options.add_argument(
                "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )

        print("Initializing Chrome...")
        self.driver = webdriver.Chrome(options=options)

        # Further mask the webdriver fingerprint via JS
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        # Store the URL early so login() can use it before super().__init__() sets it
        if not base_substack_url.endswith("/"):
            base_substack_url += "/"
        self.base_substack_url = base_substack_url

        self.login()

        # Now safe to call super().__init__() — browser is ready and authenticated
        super().__init__(base_substack_url, md_save_dir, html_save_dir)

    def login(self) -> None:
        """Log in with automated credential filling, with a manual fallback pause for captchas."""
        # Check the publication URL directly — if the saved profile has a valid session
        # we'll already be authenticated there and can skip the sign-in flow entirely.
        print(f"Checking session status at {self.base_substack_url}...")
        self.driver.get(self.base_substack_url)
        sleep(5)

        if self._is_logged_in():
            print("Already logged in via saved Chrome profile. Ready to scrape.")
            return

        # Not logged in — navigate to the sign-in page and attempt automated login
        print("No active session found. Opening Substack Sign-in...")
        self.driver.get("https://substack.com/sign-in")
        sleep(5)

        try:
            signin_with_password = self.driver.find_element(
                By.XPATH, "//a[contains(@class, 'login-option')]"
            )
            signin_with_password.click()
            sleep(2)

            self.driver.find_element(By.NAME, "email").send_keys(EMAIL)
            self.driver.find_element(By.NAME, "password").send_keys(PASSWORD)
            self.driver.find_element(By.XPATH, "//button[@type='submit']").click()
            print("Automated credentials submitted.")
        except Exception as e:
            print(f"Could not auto-fill credentials ({e}). Please log in manually in the browser window.")

        print("\n" + "=" * 60)
        print("ACTION REQUIRED:")
        print("1. Complete any Captchas in the Chrome window.")
        print("2. Confirm you are fully logged into your Substack account.")
        print("=" * 60)
        input(">>> Press ENTER once you are fully logged in... ")

        # Substack uses subdomain-scoped cookies, so we must visit the target publication
        # to transfer the session before scraping individual posts.
        print(f"\nTransferring session to {self.base_substack_url}...")
        self.driver.get(self.base_substack_url)
        sleep(5)

        if not self._is_logged_in():
            print("Warning: session didn't transfer to the publication domain.")
            input(">>> Log in on this page if needed, then press ENTER: ")

        print("Session established. Ready to scrape.")

    def _is_logged_in(self) -> bool:
        """
        Check whether the current page indicates an active login session.
        Looks for a 'Sign in' link — its absence means we're authenticated.
        """
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        sign_in_link = soup.find("a", string=lambda t: t and "sign in" in t.lower())
        return sign_in_link is None

    def is_login_failed(self) -> bool:
        return len(self.driver.find_elements(By.ID, 'error-container')) > 0

    def get_url_soup(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a page via Selenium and return its soup. Exits hard if a paywall is detected."""
        try:
            self.driver.get(url)
            sleep(4)  # Be gentle — too-fast requests risk bans

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # Paywall detected means the session has expired or wasn't transferred correctly
            if soup.select_one(".paywall-content") or soup.select_one(".premium-lock"):
                print(f"\n[!] Paywall detected on {url}.")
                print("Your session may have expired. Please restart the script and log in again.")
                os._exit(1)

            return soup
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Scrape a Substack site.')
    parser.add_argument('-u', '--url', type=str,
                        help='The base URL of the Substack site to scrape.')
    parser.add_argument('-d', '--directory', type=str,
                        help='The directory to save scraped posts.')
    parser.add_argument('-n', '--number', type=int, default=0,
                        help='The number of posts to scrape. If 0 or not provided, all posts will be scraped.')
    parser.add_argument('-p', '--premium', action='store_true',
                        help='Include -p in command to use the Premium Substack Scraper with selenium.')
    parser.add_argument('--headless', action='store_true',
                        help='Include --headless to run the browser in headless mode.')
    parser.add_argument('--edge-path', type=str, default='',
                        help='Optional: Path to the Edge browser executable.')
    parser.add_argument('--edge-driver-path', type=str, default='',
                        help='Optional: Path to the Edge WebDriver executable.')
    parser.add_argument('--user-agent', type=str, default='',
                        help='Optional: Custom user agent string for Selenium.')
    parser.add_argument('--html-directory', type=str,
                        help='The directory to save scraped posts as HTML files.')
    parser.add_argument('--no-epub', action='store_true',
                        help='Skip EPUB generation after scraping.')

    return parser.parse_args()


def main():
    args = parse_args()

    if args.directory is None:
        args.directory = BASE_MD_DIR

    if args.html_directory is None:
        args.html_directory = BASE_HTML_DIR

    if args.url:
        if args.premium:
            scraper = PremiumSubstackScraper(
                args.url,
                headless=args.headless,
                md_save_dir=args.directory,
                html_save_dir=args.html_directory
            )
        else:
            scraper = SubstackScraper(args.url, md_save_dir=args.directory, html_save_dir=args.html_directory)
        scraper.scrape_posts(args.number, make_epub=not args.no_epub)

    else:  # Use the hardcoded values at the top of the file
        if USE_PREMIUM:
            scraper = PremiumSubstackScraper(
                base_substack_url=BASE_SUBSTACK_URL,
                md_save_dir=args.directory,
                html_save_dir=args.html_directory,
            )
        else:
            scraper = SubstackScraper(
                base_substack_url=BASE_SUBSTACK_URL,
                md_save_dir=args.directory,
                html_save_dir=args.html_directory
            )
        scraper.scrape_posts(num_posts_to_scrape=NUM_POSTS_TO_SCRAPE, make_epub=not args.no_epub)


if __name__ == "__main__":
    main()