import scrapy
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict

# --- ATS platforms ---
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

class ATSUpgradeSpider(scrapy.Spider):
    name = "ats_upgrade"

    def __init__(self, csv_file="scrapling_raw_results_2_copy.csv", output_file="scrapling_raw_data_2_copy_upgraded.csv", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.csv_file = csv_file
        self.output_file = output_file
        self.df = pd.read_csv(self.csv_file)

        # Store upgraded results here
        self.results = self.df.copy()

        # Mapping {link -> (row_index, original_link)}
        self.link_map = {}

    def start_requests(self):
        for idx, row in self.df.iterrows():
            job_links = row.get("job_url")
            if pd.isna(job_links) or not job_links.strip():
                continue

            for link in [l.strip() for l in job_links.split(";") if l.strip()]:
                self.link_map[link] = idx
                yield scrapy.Request(link, callback=self.parse_job_page, meta={"original_link": link})

    def parse_job_page(self, response):
        original_link = response.meta["original_link"]
        ats_link = None

        try:
            soup = BeautifulSoup(response.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                for _, platform in ATS_PLATFORMS:
                    if platform in href:
                        ats_link = href
                        break
                if ats_link:
                    break
        except Exception as e:
            self.logger.error(f"Error parsing {original_link}: {e}")

        # Update the result row
        row_idx = self.link_map[original_link]
        current_links = [l.strip() for l in str(self.results.at[row_idx, "job_url"]).split(";") if l.strip()]
        upgraded_links = []
        for l in current_links:
            if l == original_link:
                upgraded_links.append(ats_link if ats_link else l)
            else:
                upgraded_links.append(l)
        self.results.at[row_idx, "job_url"] = "; ".join(upgraded_links)

    def closed(self, reason):
        self.results.to_csv(self.output_file, index=False, encoding="utf-8")
        self.logger.info(f"✅ ATS upgraded CSV saved as {self.output_file}")
