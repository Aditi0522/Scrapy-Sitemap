import scrapy
import pandas as pd
from lxml import etree
from collections import defaultdict

JOB_KEYWORDS = ["join-us", "jobs", "work-with-us", "job-opportunities",
                "current openings", "open positions", "apply", "career", "careers"]

EXCLUDE_KEYWORDS = ["blog", ".jpg", ".jpeg", ".png", ".webp", "event", "events"]


class JobSitemapSpider(scrapy.Spider):
    name = "jobsitemap"

    def __init__(self, csv_file="master_sheet_cleaned_data.csv", output_file="master_sheet_sitemap.txt", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_file = output_file

        # Read Startup + Website columns
        df = pd.read_csv(csv_file)
        self.company_info = df[["Startup", "Website"]].dropna().to_dict("records")

        # Clean output file at start
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write("=== Job Links Collected ===\n")

        # Trackers
        self.visited_sitemaps = set()
        self.company_data = defaultdict(lambda: {
            "Startup": "",
            "Website": "",
            "Robots.txt Link": "",
            "Sitemap Links": set(),
            "Job Links": set()
        })

    def start_requests(self):
        """Start by requesting robots.txt for each company"""
        for company in self.company_info:
            domain = company["Website"].rstrip("/")
            robots_url = domain + "/robots.txt"

            # Initialize company row
            self.company_data[domain]["Startup"] = company["Startup"]
            self.company_data[domain]["Website"] = domain
            self.company_data[domain]["Robots.txt Link"] = robots_url

            yield scrapy.Request(
                robots_url,
                callback=self.parse_robots,
                meta={"domain": domain},
                errback=self.handle_failure
            )

    def handle_failure(self, failure):
        """Handle robots.txt or sitemap fetch failures"""
        domain = failure.request.meta.get("domain")
        if domain:
            self.logger.error(f"Failed to fetch {failure.request.url} for {domain}: {failure.value}")
            

    def parse_robots(self, response):
        domain = response.meta["domain"]

        for line in response.text.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemap_url = line.split(":", 1)[1].strip()
                if sitemap_url not in self.visited_sitemaps:
                    self.visited_sitemaps.add(sitemap_url)
                    self.company_data[domain]["Sitemap Links"].add(sitemap_url)

                    yield scrapy.Request(
                        sitemap_url,
                        callback=self.parse_sitemap,
                        meta={"domain": domain, "sitemap_url": sitemap_url},
                        errback=self.handle_failure
                    )

    def parse_sitemap(self, response):
        domain = response.meta["domain"]
        sitemap_url = response.meta["sitemap_url"]

        try:
            root = etree.fromstring(response.body)
            for loc in root.findall(".//{*}loc"):
                link = loc.text.strip()

                # Nested sitemap
                if "sitemap" in link.lower() and link not in self.visited_sitemaps:
                    self.visited_sitemaps.add(link)
                    self.company_data[domain]["Sitemap Links"].add(link)

                    yield scrapy.Request(
                        link,
                        callback=self.parse_sitemap,
                        meta={"domain": domain, "sitemap_url": link},
                        errback=self.handle_failure
                    )

                # Valid job link
                elif (
                    any(k in link.lower() for k in JOB_KEYWORDS) and
                    not any(ex in link.lower() for ex in EXCLUDE_KEYWORDS)
                ):
                    if link not in self.company_data[domain]["Job Links"]:
                        self.company_data[domain]["Job Links"].add(link)

                        # Log live to text file
                        with open(self.output_file, "a", encoding="utf-8") as f:
                            f.write(f"[{domain}] {link}\n")

        except Exception as e:
            self.logger.error(f"Parse error at {response.url}: {e}")

    def closed(self, reason):
        """Export grouped results at the end"""
        import csv
        with open("original_data_sitemap.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["Startup", "Website", "Robots.txt Link", "Sitemap Links", "Job Links"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for company in self.company_data.values():
                writer.writerow({
                    "Startup": company["Startup"],
                    "Website": company["Website"],
                    # keep blank if missing
                    "Robots.txt Link": company["Robots.txt Link"] or "",
                    "Sitemap Links": "; ".join(sorted(company["Sitemap Links"])) if company["Sitemap Links"] else "",
                    "Job Links": "; ".join(sorted(company["Job Links"])) if company["Job Links"] else ""
                })
