import scrapy
from scrapy_splash import SplashRequest
import requests
from bs4 import BeautifulSoup
import pandas as pd


class CoursesSpider(scrapy.Spider):
    name = 'courses'
    start_urls = ['https://talentedge.com/browse-courses']

    def __init__(self, *args, **kwargs):
        super(CoursesSpider, self).__init__(*args, **kwargs)
        self.scraped_data = []  # Initialize a list to store scraped data

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(
                url,
                self.parse,
                args={'wait': 2},
                splash_headers={'X-Splash-Render-Timeout': '10s'}
            )

    def parse(self, response):
        # Extract course names and associated links
        courses = response.xpath('//h3[@class="courses-name"]/a')
        
        for course in courses:
            course_name = self.clean_text(course.xpath('text()').get())
            course_link = self.clean_text(course.xpath('@href').get())
            if course_name and course_link:
                yield SplashRequest(
                    url=response.urljoin(course_link),
                    callback=self.parse_course_details,
                    args={'wait': 2},
                    meta={
                        'course_name': course_name,
                        'view_all': course_link
                    }
                )

    def parse_course_details(self, response):
        course_name = response.meta.get('course_name')
        view_all = response.meta.get('view_all')

        # Extract details based on provided class names
        desc = self.clean_text(response.xpath('//*[@class="desc"]/text()').get())
        
        # Extract specific <li> items from course-specification class
        course_spec_items = response.xpath('//*[@class="course-specification"]//ul/li/text()').getall()
        
        course_spec1 = self.clean_text(course_spec_items[1]) if len(course_spec_items) > 1 else ''
        course_spec2 = self.clean_text(course_spec_items[2]) if len(course_spec_items) > 2 else ''
        course_spec3 = self.clean_text(course_spec_items[3]) if len(course_spec_items) > 3 else ''
        
        deeper_undstnd = self.clean_text(response.xpath('//*[@class="pl-deeper-undstnd"]/text()').get())
        key_skills = [self.clean_text(skill) for skill in response.xpath('//*[@class="key-skills-sec"]//ul/li/text()').getall()]
        cs_titlec = self.clean_text(response.xpath('//*[@class="cs-titlec"]/text()').get())
        eligible_top_right_list = [self.clean_text(item) for item in response.xpath('//*[@class="eligible-top-right-list"]//li/text()').getall()]
        sylb_desk_tab = [self.clean_text(text) for text in response.xpath('//*[@class="sylb-desk-tab"]//text()').getall()]
        best_fname = [self.clean_text(name) for name in response.xpath('//*[@class="best-fname"]/text()').getall()]
        about_ititle = self.clean_text(response.xpath('//*[@class="about-ititle"]/text()').get())
        program_total_pay_0 = self.clean_text(response.xpath('//*[@class="program-details-total-pay-amt-right"][1]/text()').get())
        program_total_pay_1 = self.clean_text(response.xpath('//*[@class="program-details-total-pay-amt-right"][2]/text()').get())

        # Store the data in the list
        self.scraped_data.append({
            'Course Link': self.clean_text(view_all),
            'Title': self.clean_text(course_name),            
            'Description': desc,
            'Duration': course_spec2,
            'Timing': course_spec1,            
            'Course Start': course_spec3,
            'What you will Understand': deeper_undstnd,
            'Skills': ", ".join(key_skills),
            'Target Students': cs_titlec,
            'Pre-requisites': ", ".join(eligible_top_right_list),
            'Content': " ".join(sylb_desk_tab),
            'Faculty Names': ", ".join(best_fname),
            'Institute Name': about_ititle,
            'Fee in INR': program_total_pay_0,
            'Fee in USD': program_total_pay_1
        })

    def clean_text(self, text):
        if text:
            return ' '.join(text.strip().replace('\n', ' ').split())
        return ''

    def closed(self, reason):
        # When the spider closes, save the scraped data to a CSV file
        df = pd.DataFrame(self.scraped_data)
        df.to_csv('scraped_courses.csv', index=False, encoding='utf-8')
        self.log('Data saved to scraped_courses4.csv')
