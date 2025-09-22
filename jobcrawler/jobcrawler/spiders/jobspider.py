import scrapy
import urllib
import pandas as pd
from bs4 import BeautifulSoup

# -------------------- Constants --------------------

SCRAPER_API_KEY = "d18cada5a68b2bfdf275d54550a3d095"   # ⬅️ put your ScraperAPI key here
SCRAPER_API_URL = "https://api.scraperapi.com/"

ATS_PLATFORMS = [
    ("Keka", "keka.com"),
    ("Darwinbox", "darwinbox.in"),
    ("Teamtailor", "teamtailor.com"),
    ("Lever", "lever.co"),
    ("Greenhouse", "greenhouse.io"),
    ("Wellfound", "wellfound.com"),
    ("Workday", "myworkdayjobs.com"),
    ("Zoho Recruit", "zohorecruit.com"),
    ("BambooHR", "bamboohr.com"),
    ("Personio", "personio.com"),
    ("SmartRecruiters", "smartrecruiters.com"),
    ("Workable", "workable.com"),
    ("Jobvite", "jobs.jobvite.com"),
    ("JazzHR", "applytojob.com"),
    ("Recruitee", "recruitee.com"),
    ("Zip recruiter", "ziprecruiter.com"),
    ("Glassdoor", "glassdoor.com"),
    ("Instahyre", "instahyre.com"),
]

# -------------------- Helper Functions --------------------

def build_search_query(official_domain: str) -> str:
    """Build Google query string."""
    return (
        f"{official_domain} "
        f"zoho OR lever OR indeed OR workable OR workday OR naukri OR darwinbox "
        f"OR keka OR ziprecruiter OR greenhouse jobs -linkedin"
    )

def extract_google_links(html, max_links=10):
    """Extract organic Google search result links."""
    soup = BeautifulSoup(html, "html.parser")
    result_links = []
    for div in soup.find_all("div", class_="yuRUbf")[:max_links]:
        a_tag = div.find("a", href=True)
        if a_tag:
            result_links.append(a_tag["href"])
    return result_links

def score_url(url, official_domain, company_name=""):
    """Assign a score to each candidate URL."""
    score = 0
    parsed = urllib.parse.urlparse(url)
    domain = parsed.netloc.lower()
    url_lower = url.lower()

    company_clean = company_name.lower().replace(" ", "")
    official_netloc = urllib.parse.urlparse(official_domain).netloc.lower().replace("www.", "")

    for _, platform in ATS_PLATFORMS:
        if platform in domain:
            score += 10

    if official_netloc and official_netloc in domain:
        score += 8

    if company_clean and company_clean in url_lower:
        score += 12

    if any(kw in url_lower for kw in ["jobs", "careers", "join", "hiring"]):
        score += 6

    if any(x in domain for x in ["linkedin.com", "indeed.com"]):
        score += 3

    return score

def wrap_scraperapi(url: str) -> str:
    """Wrap a URL with ScraperAPI proxy."""
    return f"{SCRAPER_API_URL}?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url)}"

# -------------------- Spider --------------------

class JobSpider(scrapy.Spider):
    name = "jobspider"

    def start_requests(self):
        df = pd.read_csv("cleaned_file.csv")

        for _, row in df.iterrows():
            company = str(row["Startup"]).strip()
            official_domain = str(row["Website"]).strip()

            if not official_domain or official_domain.lower() == "nan":
                yield {"company": company, "job_page": None}
                continue

            query = build_search_query(official_domain)
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
            proxy_url = wrap_scraperapi(search_url)

            yield scrapy.Request(
                url=proxy_url,
                callback=self.parse_search_results,
                meta={"company": company, "official_domain": official_domain},
                headers={"User-Agent": "Mozilla/5.0"},
            )

    def parse_search_results(self, response):
        company = response.meta["company"]
        official_domain = response.meta["official_domain"]

        links = extract_google_links(response.text)
        scored_links = [(url, score_url(url, official_domain, company)) for url in links]
        scored_links.sort(key=lambda x: x[1], reverse=True)

        if not scored_links:
            yield {"company": company, "job_page": None}
            return

        best_url, _ = scored_links[0]

        if any(platform in best_url for _, platform in ATS_PLATFORMS):
            yield {"company": company, "job_page": best_url}
        else:
            proxy_fallback = wrap_scraperapi(best_url)
            yield scrapy.Request(
                url=proxy_fallback,
                callback=self.parse_career_page,
                meta={"company": company, "fallback_url": best_url},
                headers={"User-Agent": "Mozilla/5.0"},
                dont_filter=True,
            )

    def parse_career_page(self, response):
        company = response.meta["company"]
        fallback_url = response.meta["fallback_url"]

        ats_link = None
        for a in response.xpath("//a/@href").getall():
            for _, platform in ATS_PLATFORMS:
                if platform in a:
                    ats_link = a
                    break
            if ats_link:
                break

        if ats_link:
            yield {"company": company, "job_page": ats_link}
        else:
            yield {"company": company, "job_page": fallback_url}
