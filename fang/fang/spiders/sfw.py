# -*- coding: utf-8 -*-
import scrapy
import re
from fang.items import NewHouseItem,ESFHouseItem
from scrapy_redis.spiders import RedisSpider


class SfwSpider(RedisSpider):
    name = 'sfw'
    allowed_domains = ['fang.com']
    # start_urls = ['https://www.fang.com/SoufunFamily.htm']
    redis_key = "fang:start_urls"


    def parse(self, response):
        trs = response.xpath('//div[@class="outCont"]//tr')[0:-1]
        province = None
        for tr in trs:
            tds = tr.xpath('.//td[not(@class)]')
            province_td = tds[0]
            province_text = province_td.xpath('.//text()').get()
            province_text = re.sub(r'\s','',province_text)
            if province_text:
                province = province_text

            #不获取海外的城市房源
            # if province == '其它':
            #     continue

            city_td = tds[1]
            city_links = city_td.xpath('.//a')
            for city_link in city_links:
                city = city_link.xpath('.//text()').get()
                city_url = city_link.xpath('.//@href').get()
                url_module = city_url.split('.',1)
                scheme = url_module[0]
                domain = url_module[1]
                if 'http://bj' in scheme:
                    newhouse_url = 'https://newhouse.fang.com/house/s/'
                    esf_url = 'https://esf.fang.com/'
                else:
                    newhouse_url = scheme +'.newhouse.'+domain+'house/s/'
                    esf_url = scheme+'.esf.'+domain
                # print('城市：%s%s'%(province,city))
                # print('新房链接：%s' % newhouse_url)
                # print('二手房链接：%s' % esf_url)
                yield scrapy.Request(
                    url=newhouse_url,
                    callback=self.parse_newhouse,
                    meta={'info':(province,city)}
                )

                yield scrapy.Request(
                    url=esf_url,
                    callback=self.parse_esf,
                    meta={'info':(province,city)}
                )



    def parse_newhouse(self,response):
        province,city = response.meta.get('info')
        list = response.xpath('//div[contains(@class,"nl_con")]/ul/li')
        for li in list:
            name = li.xpath('.//div[@class="nlcd_name"]/a/text()').get()
            if name:
                name = name.strip()
            rooms = li.xpath('.//div[contains(@class,"house_type")]//a/text()').getall()
            area = ''.join(li.xpath('.//div[contains(@class,"house_type")]/text()').getall())
            area = re.sub(r'\s|－|/','',area)
            address = li.xpath('.//div[@class="address"]/a/@title').get()
            district_text = ''.join(li.xpath('.//div[@class="address"]//text()').getall())
            if district_text:
                district = re.search(r'\[(.+)\]',district_text).group(1)
            sale = li.xpath('.//div[contains(@class,"fangyuan")]/span[1]/text()').get()
            price = ''.join(li.xpath('.//div[@class="nhouse_price"]//text()').getall())
            price = re.sub('\s|广告','',price)
            origin_url = li.xpath('.//div[@class="nlcd_name"]/a/@href').get()

            item = NewHouseItem(province=province,city=city,name=name,rooms=rooms,area=area,address=address,district=district,sale=sale,price=price,origin_url=origin_url)
            yield item

        next_url = response.xpath('//div[@class="page"]//a[@class="next"]/@href').get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url),callback=self.parse_newhouse,meta={'info':(province,city)})


    def parse_esf(self,response):
        province,city = response.meta.get('info')
        dls = response.xpath('//div[@class="shop_list shop_list_4"]/dl')
        for dl in dls:
            item = ESFHouseItem(province=province,city=city)
            item['name'] = dl.xpath('.//p[@class="add_shop"]/a/@title').get()
            infos = dl.xpath('.//p[@class="tel_shop"]/text()').getall()
            infos = list(map(lambda x:re.sub('\s','',x),infos))[0:-1]
            for info in infos:
                if '厅' in info:
                    item['rooms'] = info
                elif '㎡' in info:
                    item['area'] = info
                elif '层' in info:
                    item['floor'] = info
                elif '向' in info:
                    item['toward'] = info
                else:
                    item['year'] = info

            item['address'] = dl.xpath('.//p[@class="add_shop"]/span/text()').get()

            origin_url = response.urljoin(dl.xpath('.//a/@href').get())
            item['origin_url'] = origin_url
            yield scrapy.Request(url=origin_url,callback=self.parse_detail,meta={'info':item})

        next_url = response.xpath('//div[@class="page_al"]/p[1]/a/@href').get()
        yield scrapy.Request(url=response.urljoin(next_url),callback=self.parse_esf,meta={'info':(province,city)})


    def parse_detail(self,response):
        item = response.meta['info']
        item['price'] = ''.join(response.xpath('//div[@class="trl-item_top"]/div[contains(@class,"trl-item")]//text()').getall())
        item['unit'] = ''.join(response.xpath('//div[@class="tab-cont-right"]/div[2]/div[3]/div[@class="tt"]/text()').get())
        yield item

