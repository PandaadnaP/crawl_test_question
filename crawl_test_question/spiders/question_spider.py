# -*- coding: utf-8 -*-
import scrapy
import requests
import pymysql

class QuestionSpider(scrapy.Spider):
    name = "question"
    allowed_domains = ['cooco.net.cn']
    start_urls = [
    ]
    lessonids = []
    lesson_page = {}
    current_lessonid = 0
    chapter = {}
    partid_chapter = {
        501: 1,
        502: 1,
        503: 1,
        504: 1,
        505: 1,
        506: 1,
        507: 1,
        508: 1,
        509: 1,
        510: 1,
        511: 1,
        512: 1,
        513: 1,
        514: 1,
        515: 1,
        516: 1,
        671: 2,
        672: 2,
        673: 2,
        674: 2,
        675: 2,
        676: 2,
        677: 2,
        679: 3,
        680: 3,
        681: 3,
        682: 3,
        683: 3,
        684: 3,
        685: 3,
        686: 3,
        687: 3,
        689: 4,
        690: 4,
        691: 4,
        692: 4,
        693: 4,
        694: 4,
        696: 5,
        697: 5,
        698: 5,
        699: 5,
        700: 5,
        701: 5,
        702: 5,
        703: 5,
        704: 5,
        705: 5,
    }




    headers = {
        "Accept":"text/javascript, text/html, application/xml, text/xml, */*",
        "Accept-Encoding":"gzip, deflate",
        "Accept-Language":"zh-CN,zh;q=0.8,en;q=0.6",
        "Connection":"keep-alive",
        "Content-type":"application/x-www-form-urlencoded",
        "Host":"gzsx.cooco.net.cn",
        "Origin":"http://gzsx.cooco.net.cn",
        "Referer":"http://gzsx.cooco.net.cn/test/",
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3049.0 Safari/537.36",
        "X-Prototype-Version":"1.5.0_rc0",
        "X-Requested-With":"XMLHttpRequest",
    }


    def start_requests(self):


        self.lessonids = range(501, 517) + range(671,678) + range(679,688) \
                    + range(696, 698) + range(699,700)
        self.current_lessonid = self.lessonids.pop()
        return_request = []

        # sql = SQL('127.0.0.1', 'root', 'panda11!!11', 'django_system_test_question')
        # sql = SQL('139.199.227.111', 'llhh', 'llhh', 'django_system_test_question')
        sql = SQL('120.77.87.101', 'llhh', 'llhh', 'llhh')
        # sql = SQL('139.199.227.111', 'llhh', 'llhh', 'llhh')
        sql.connect_sql()
        sql.cursor.execute('DELETE FROM parts')
        sql.insert_info('parts', {
            'id': 1,
            'name': '必修部分',
            'module_id': 1,
        })
        sql.insert_info('parts', {
            'id': 2,
            'name': '选修1',
            'module_id': 1,
        })
        sql.insert_info('parts', {
            'id': 3,
            'name': '选修2',
            'module_id': 1,
        })
        sql.insert_info('parts', {
            'id': 4,
            'name': '选修3',
            'module_id': 1,
        })
        sql.insert_info('parts', {
            'id': 5,
            'name': '选修4',
            'module_id': 1,
        })
        sql.close()
        return_request.append(
                scrapy.FormRequest("http://gzsx.cooco.net.cn/testpage/1/",
                                   formdata={'lessonid': str(self.current_lessonid),
                                             'difficult': '0',
                                             'type': '0',
                                             'orderby': '1'},
                                   headers = self.headers,
                                   cookies={'bdshare_firstime': '1489976748691',
                                            'Hm_lvt_c8ad39b9579b4816dc8f6f805c190308': '1489976749,1490323008,1490323063',
                                            'Hm_lpvt_c8ad39b9579b4816dc8f6f805c190308': '1490335777'}))
        return return_request

    def parse(self, response):

        questions = response.xpath('//li')  # 题目
        lesson_id = response.request.body
        lesson_id = str(lesson_id.split('&')[1].split('=')[1])

        # try:
        page_number = response.xpath('//a[contains(@class, "page-numbers")]/text()').extract()[-1]
        # except:
        #     return []
        page_number = int(page_number)

        # from scrapy.shell import inspect_response
        # inspect_response(response, self)

        count = 0
        for question in questions:
            question_all_p = ''
            try:
                question_type = question.xpath('div[1]/div[3]/div[2]/span[1]/text()').extract()  # 题目类型  选择题，填空题，计算题
                answer_number = question.xpath('div[1]/div[4]/@id').extract() # 答案对应的网址数字  daan-xxxxx
                question_subjects = question.xpath('div[1]/div[3]/div[2]/span[2]/text()').extract()  # 题目知识点
                difficulty = 5 - question.xpath('div[1]/div[1]/div[1]').extract()[0].count('nsts')  # 难度 0~5
                source = question.xpath('div[1]/div[2]/span/text()').extract()  # 题目来源
                date_create = question.xpath('div[1]/div[1]/div[1]/text()').extract()  # 题目来源
                date_create = date_create[1].split(u'：')[-1]
                p_s = question.xpath('div[1]')
                for p in p_s.xpath('p[not(@class)]'):
                    question_all_p += p.extract()
                question_all_p = question_all_p.replace('"', "'")
                question_all_p = question_all_p.replace("src='", "src='http://gzsx.cooco.net.cn")

                answer_number = answer_number[0].split('-')[1]

                if question_type[0] == u'\u9009\u62e9\u9898\xa0\xa0':
                    question_type = 1
                elif question_type[0] == u'\u586b\u7a7a\u9898\xa0\xa0':
                    question_type = 2
                else:
                    question_type = 3
            except:
                continue
            # print answer_number
            response_answer = requests.get('http://gzsx.cooco.net.cn/answerdetail/' + str(answer_number) + '/')
            answer_all_p = response_answer.text
            answer_all_p = answer_all_p.replace('"', "'")
            answer_all_p = answer_all_p.replace("src='", "src='http://gzsx.cooco.net.cn")
            # print answer_all_p
            # print source[0]

            # sql = SQL('127.0.0.1', 'root', 'panda11!!11', 'django_system_test_question')
            # sql = SQL('139.199.227.111', 'llhh', 'llhh', 'llhh')
            sql = SQL('120.77.87.101', 'llhh', 'llhh', 'llhh')
            # sql = SQL('139.199.227.111', 'llhh', 'llhh', 'django_system_test_question')
            sql.connect_sql()
            sql.insert_info('questions', {
                'description': question_all_p,
                'difficulty': int(difficulty),
                'chapter_id': lesson_id,
                'source': source[0],
                'answer': answer_all_p,
                'type': question_type,
                'created_at': date_create,
                'updated_at': date_create,
            })

            line = sql.cursor.execute('SELECT * FROM chapters WHERE id=%d'%(int(lesson_id)))
            try:
                if line == 0:
                    sql.insert_info('chapters',{
                        'id': lesson_id,
                        'name': question_subjects[0],
                        'part_id': self.partid_chapter[int(lesson_id)],
                    })
                    sql.close()
            except:
                pass

            # print(u'%s,%s,%s,%s,%s,%s,%s'%(all_p, question_type, answer_number, question_subjects, str(self.current_lessonid), str(difficulty), source))

            count += 1

        # for i in range(2, page_number + 1):
        for i in range(2, 20):
            yield scrapy.FormRequest("http://gzsx.cooco.net.cn/testpage/" + str(i) + "/",
                                     formdata={'lessonid': lesson_id,
                                     'difficult': '0',
                                     'type': '0',
                                     'orderby': '1'},
                                     headers = self.headers,
                                     cookies={'bdshare_firstime': '1489976748691',
                                              'Hm_lvt_c8ad39b9579b4816dc8f6f805c190308': '1489976749,1490323008,1490323063',
                                              'Hm_lpvt_c8ad39b9579b4816dc8f6f805c190308': '1490335777'})

        if len(self.lessonids) != 0:
            self.current_lessonid = self.lessonids.pop()
            yield scrapy.FormRequest("http://gzsx.cooco.net.cn/testpage/1/",
                               formdata={'lessonid': str(self.current_lessonid),
                                         'difficult': '0',
                                         'type': '0',
                                         'orderby': '1'},
                               headers = self.headers,
                               cookies={'bdshare_firstime': '1489976748691',
                                        'Hm_lvt_c8ad39b9579b4816dc8f6f805c190308': '1489976749,1490323008,1490323063',
                                        'Hm_lpvt_c8ad39b9579b4816dc8f6f805c190308': '1490335777'})

    def get_answer(self, response):
        print response.body

class SQL(object):
    server = None
    user = None
    password = None
    database = None
    connect = None
    connect_mssql = None
    connect_mysql = None
    cursor = None

    def __init__(self, para_server, para_user, para_password, para_database):
        self.server = para_server
        self.user = para_user
        self.password = para_password
        self.database = para_database
        self.connect = None
        self.cursor = None

    def connect_sql(self):
        """
        no parameter
        :return: no return
        """
        self.connect_mysql = pymysql.connect(host=self.server, user=self.user, password=self.password, database=self.database, charset="utf8")
        self.cursor = self.connect_mysql.cursor()

    def insert_info(self, table_name, dic_of_insert_info):
        str_col = ""
        str_val = ""
        flag = 0
        for column_name in dic_of_insert_info:
            value = dic_of_insert_info[column_name]
            if flag == 1:
                str_col += ','
                str_val += ','
            str_col += "%s"%(column_name)
            str_val += self._str_attr(value)
            flag = 1

        command ="INSERT INTO %s " % (table_name) + '(' + str_col + ')' + " VALUES " + '(' + str_val + ')'
        print(command)
        self.cursor.execute(command)
        self.connect_mysql.commit()

    def _str_attr(self, added_str):
        if type(added_str) == str:
            return "'%s'"%(added_str)
        elif type(added_str) == int:
            return "%d"%(added_str)
        else:
            return '"%s"'%(added_str)

    def close(self):
        """
        close the connection
        :return: None
        """
        self.connect_mysql.close()
