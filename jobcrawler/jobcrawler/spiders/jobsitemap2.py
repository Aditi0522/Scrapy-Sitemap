# import scrapy
# import pandas as pd
# from lxml import etree
# from collections import defaultdict

# JOB_KEYWORDS = ["join-us", "jobs", "work-with-us", "job-opportunities",
#                 "current openings", "open positions", "apply", "career", "careers"]

# EXCLUDE_KEYWORDS = ["blog", ".jpg", ".jpeg", ".png", ".webp", "event", "events"]
# MAX_JOBS_PER_COMPANY = 5

# class JobSitemapSpider(scrapy.Spider):
#     name = "jobsitemap2"

#     def __init__(self, csv_file="cleaned_file_1.csv", output_file="original_data_sitemap.txt", *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.output_file = output_file

#         # Read Startup + Website columns
#         df = pd.read_csv(csv_file)
#         self.company_info = df[["company name", "website url"]].dropna().to_dict("records")

#         # Clean output file at start
#         with open(self.output_file, "w", encoding="utf-8") as f:
#             f.write("=== Job Links Collected ===\n")

#         # Trackers
#         self.visited_sitemaps = set()
#         self.company_data = defaultdict(lambda: {
#             "company name": "",
#             "website url": "",
#             "Robots.txt Link": "",
#             "Sitemap Links": set(),
#             "Job Links": set()
#         })

#     def start_requests(self):
#         """Start by requesting robots.txt for each company"""
#         for company in self.company_info:
#             domain = company["website url"].rstrip("/")
#             robots_url = domain + "/robots.txt"

#             # Initialize company row
#             self.company_data[domain]["company name"] = company["company name"]
#             self.company_data[domain]["website url"] = domain
#             self.company_data[domain]["Robots.txt Link"] = robots_url

#             yield scrapy.Request(
#                 robots_url,
#                 callback=self.parse_robots,
#                 meta={"domain": domain},
#                 errback=self.handle_failure
#             )

#     def handle_failure(self, failure):
#         """Handle robots.txt or sitemap fetch failures"""
#         domain = failure.request.meta.get("domain")
#         if domain:
#             self.logger.error(f"Failed to fetch {failure.request.url} for {domain}: {failure.value}")
            

#     def parse_robots(self, response):
#         domain = response.meta["domain"]

#         for line in response.text.splitlines():
#             if line.lower().startswith("sitemap:"):
#                 sitemap_url = line.split(":", 1)[1].strip()
#                 if sitemap_url not in self.visited_sitemaps:
#                     self.visited_sitemaps.add(sitemap_url)
#                     self.company_data[domain]["Sitemap Links"].add(sitemap_url)

#                     yield scrapy.Request(
#                         sitemap_url,
#                         callback=self.parse_sitemap,
#                         meta={"domain": domain, "sitemap_url": sitemap_url},
#                         errback=self.handle_failure
#                     )

   

#     def parse_sitemap(self, response):
#         domain = response.meta["domain"]
#         sitemap_url = response.meta["sitemap_url"]

#         try:
#             root = etree.fromstring(response.body)
#             for loc in root.findall(".//{*}loc"):
#                 link = loc.text.strip().lower()

#                 # Nested sitemap
#                 if "sitemap" in link and link not in self.visited_sitemaps:
#                     self.visited_sitemaps.add(link)
#                     self.company_data[domain]["Sitemap Links"].add(link)

#                     yield scrapy.Request(
#                         link,
#                         callback=self.parse_sitemap,
#                         meta={"domain": domain, "sitemap_url": link},
#                         errback=self.handle_failure
#                     )

#                 # Potential job listing hub (not detail pages)
#                 elif (
#                     any(link.rstrip("/").endswith(k) for k in JOB_KEYWORDS)
#                     and not any(ex in link for ex in EXCLUDE_KEYWORDS)
#                 ):
#                     # Check if already hit the cap
#                     if len(self.company_data[domain]["Job Links"]) >= MAX_JOBS_PER_COMPANY:
#                         break  # stop processing more job links for this company

#                     if link not in self.company_data[domain]["Job Links"]:
#                         self.company_data[domain]["Job Links"].add(link)

#                         # Write live to file
#                         with open(self.output_file, "a", encoding="utf-8") as f:
#                             f.write(f"[{domain}] {link}\n")

#         except Exception as e:
#             self.logger.error(f"Parse error at {response.url}: {e}")



#     def closed(self, reason):
#         """Export grouped results at the end"""
#         import csv
#         with open("original_data_sitemap.csv", "w", newline="", encoding="utf-8") as csvfile:
#             fieldnames = ["company name", "website url", "Robots.txt Link", "Sitemap Links", "Job Links"]
#             writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#             writer.writeheader()

#             for company in self.company_data.values():
#                 writer.writerow({
#                     "company name": company["company name"],
#                     "website url": company["website url"],
#                     # keep blank if missing
#                     "Robots.txt Link": company["Robots.txt Link"] or "",
#                     "Sitemap Links": "; ".join(sorted(company["Sitemap Links"])) if company["Sitemap Links"] else "",
#                     "Job Links": "; ".join(sorted(company["Job Links"])) if company["Job Links"] else ""
#                 })


import scrapy
import pandas as pd
from lxml import etree
from collections import defaultdict

JOB_KEYWORDS = ["join-us", "jobs", "work-with-us", "job-opportunities",
                "current openings", "open positions", "apply", "career", "careers"]

EXCLUDE_KEYWORDS = ["blog", ".jpg", ".jpeg", ".png", ".webp", "event", "events"]
MAX_JOBS_PER_COMPANY = 5


class JobSitemapSpider(scrapy.Spider):
    name = "jobsitemap2"

    def __init__(self, csv_file="cleaned_file_1.csv", output_file="original_data_sitemap.txt", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_file = output_file

        # Read Startup + Website columns
        df = pd.read_csv(csv_file)
        self.company_info = df[["company name", "website url"]].dropna().to_dict("records")

        # Clean output file at start
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write("=== Job Links Collected ===\n")

        # Trackers
        self.visited_sitemaps = set()
        self.company_done = set()   # <-- NEW: Track companies that reached the limit
        self.company_data = defaultdict(lambda: {
            "company name": "",
            "website url": "",
            "Robots.txt Link": "",
            "Sitemap Links": set(),
            "Job Links": set()
        })

    def start_requests(self):
        """Start by requesting robots.txt for each company"""
        for company in self.company_info:
            domain = company["website url"].rstrip("/")
            robots_url = domain + "/robots.txt"

            # Initialize company row
            self.company_data[domain]["company name"] = company["company name"]
            self.company_data[domain]["website url"] = domain
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

        # If company already marked done, skip
        if domain in self.company_done:
            return

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

        # If company already has enough job links, stop here
        if domain in self.company_done:
            return

        try:
            root = etree.fromstring(response.body)
            for loc in root.findall(".//{*}loc"):
                link = loc.text.strip().lower()

                # If company already has enough job links, stop
                if len(self.company_data[domain]["Job Links"]) >= MAX_JOBS_PER_COMPANY:
                    self.company_done.add(domain)
                    self.logger.info(f"Reached job link limit for {domain}, skipping further sitemaps.")
                    return

                # Nested sitemap
                if "sitemap" in link and link not in self.visited_sitemaps:
                    if domain not in self.company_done:  # only follow if still collecting
                        self.visited_sitemaps.add(link)
                        self.company_data[domain]["Sitemap Links"].add(link)

                        yield scrapy.Request(
                            link,
                            callback=self.parse_sitemap,
                            meta={"domain": domain, "sitemap_url": link},
                            errback=self.handle_failure
                        )

                # Potential job listing hub (not detail pages)
                elif (
                    any(link.rstrip("/").endswith(k) for k in JOB_KEYWORDS)
                    and not any(ex in link for ex in EXCLUDE_KEYWORDS)
                ):
                    if link not in self.company_data[domain]["Job Links"]:
                        self.company_data[domain]["Job Links"].add(link)

                        # Write live to file
                        with open(self.output_file, "a", encoding="utf-8") as f:
                            f.write(f"[{domain}] {link}\n")

                        # If limit reached, stop crawling this company
                        if len(self.company_data[domain]["Job Links"]) >= MAX_JOBS_PER_COMPANY:
                            self.company_done.add(domain)
                            self.logger.info(f"Limit reached for {domain}, skipping rest.")
                            return

        except Exception as e:
            self.logger.error(f"Parse error at {response.url}: {e}")

    def closed(self, reason):
        """Export grouped results at the end"""
        import csv
        with open("original_data_sitemap.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["company name", "website url", "Robots.txt Link", "Sitemap Links", "Job Links"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for company in self.company_data.values():
                writer.writerow({
                    "company name": company["company name"],
                    "website url": company["website url"],
                    # keep blank if missing
                    "Robots.txt Link": company["Robots.txt Link"] or "",
                    "Sitemap Links": "; ".join(sorted(company["Sitemap Links"])) if company["Sitemap Links"] else "",
                    "Job Links": "; ".join(sorted(company["Job Links"])) if company["Job Links"] else ""
                })
