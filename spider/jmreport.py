# -*- coding:utf-8 -*

import re
import time
import json
import requests


ENCODING_SCHEDULE = {
    "0": "7", "1": "1", "2": "u", "3": "N", "4": "K", "5": "J", "6": "M", "7": "9", "8": "'", "9": "m", "!": "P",
    "%": "/", "'": "n", "(": "A", ")": "E", "*": "s", "+": "+", "-": "f", ".": "q", "A": "O", "B": "V", "C": "t",
    "D": "T", "E": "a", "F": "x", "G": "H", "H": "r", "I": "c", "J": "v", "K": "l", "L": "8", "M": "F", "N": "3",
    "O": "o", "P": "L", "Q": "Y", "R": "j", "S": "W", "T": "*", "U": "z", "V": "Z", "W": "!", "X": "B", "Y": ")",
    "Z": "U", "a": "(", "b": "~", "c": "i", "d": "h", "e": "p", "f": "_", "g": "-", "h": "I", "i": "R", "j": ".",
    "k": "G", "l": "S", "m": "d", "n": "6", "o": "w", "p": "5", "q": "0", "r": "4", "s": "D", "t": "k", "u": "Q",
    "v": "g", "w": "b", "x": "C", "y": "2", "z": "X", "~": "e", "_": "y",
}


DECODING_SCHEDULE = {
    "7": "0", "1": "1", "u": "2", "N": "3", "K": "4", "J": "5", "M": "6", "9": "7", "'": "8", "m": "9", "P": "!",
    "/": "%", "n": "'", "A": "(", "E": ")", "s": "*", "+": "+", "f": "-", "q": ".", "O": "A", "V": "B", "t": "C",
    "T": "D", "a": "E", "x": "F", "H": "G", "r": "H", "c": "I", "v": "J", "l": "K", "8": "L", "F": "M", "3": "N",
    "o": "O", "L": "P", "Y": "Q", "j": "R", "W": "S", "*": "T", "z": "U", "Z": "V", "!": "W", "B": "X", ")": "Y",
    "U": "Z", "(": "a", "~": "b", "i": "c", "h": "d", "p": "e", "_": "f", "-": "g", "I": "h", "R": "i", ".": "j",
    "G": "k", "S": "l", "d": "m", "6": "n", "w": "o", "5": "p", "0": "q", "4": "r", "D": "s", "k": "t", "Q": "u",
    "g": "v", "b": "w", "C": "x", "2": "y", "X": "z", "e": "~", "y": "_",
}


def encode(code):
    out = ""
    for item in code:
        out = out + ENCODING_SCHEDULE.get(item, item)
    return out


def decode(code):
    out = ""
    for item in code:
        out = out + DECODING_SCHEDULE.get(item, item)
    return out


class QueryServlet(object):

    __slots__ = "s", "url"

    URL = "http://110.0.170.88:9083/smartbi/vision/RMIServlet"
    HEADERS = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Host": "110.0.170.88:9083",
        "Origin": "http://110.0.170.88:9083",
        # REFERER: "http://110.0.170.88:9083/smartbi/vision/index.jsp",
    }

    def __init__(self, session, url=None):
        super(QueryServlet, self).__init__()

        self.s = session
        self.url = url or self.URL

    def __call__(self, data, headers=None):
        return self.s.post(
            self.url,
            headers=headers or self.HEADERS,
            data={
                "encode": encode(data)
            }
        )


class ExportServlet(object):

    __slots__ = "s", "url"

    URL = "http://110.0.170.88:9083/smartbi/vision/ExportExtServlet"
    HEADERS = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    def __init__(self, session, url=None):
        super().__init__()

        self.s = session
        self.url = url or self.URL

    def __call__(self, *, headers=None, data=None):
        headers = headers or {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        return self.s.post(
            self.url,
            headers=headers or self.HEADERS,
            data=data
        )


class ReportClient(object):

    def __init__(self, session, tag_id):
        super().__init__()

        self.s = session
        self.query = QueryServlet(self.s)
        self.export = ExportServlet(self.s)

        self.tag_id = tag_id
        self.view_id = None
        self.client_id = None
        self.param_panel_id = None
        self.param_id = None

    def init(self):
        self.open_combined_query()
        self.create_simple_report()
        self.init_view_ex()
        self.set_simple_client()
        return self

    def open_combined_query(self):
        """
        步骤1
        :return: 通过访标签节点，返回 [viewID] 及 [paramID]
        """
        data = """CombinedQueryService+openCombinedQuery+["%s",null]""" % self.tag_id

        res = self.query(data)
        text = encode(res.text)
        result = json.loads(text)["result"]

        self.view_id = result[0]
        self.param_id = result[1]

        return self

    def create_simple_report(self):
        """
        步骤2
        :return: 创建报表返回 [clientID] 及 [paramPanelID]
        """
        data = """CombinedQueryService+createSimpleReport+[%s]""" % self.view_id
        res = self.query(data)

        result = json.loads(encode(res.text))["result"]

        self.client_id = result["clientId"]
        self.param_panel_id = result["parameterPanelId"]

        return self

    def init_view_ex(self):
        """
        步骤3
        :return: 初始化视图
        """
        data = """CombinedQueryService+initFromBizViewEx+["%s","%s","%s",true]""" % (self.view_id, self.client_id, self.param_id)
        self.query(data)

        return self

    def set_simple_client(self):
        """
        步骤4
        :return: 设置客户端ID，使之有效
        """
        data = """CombinedQueryService+setSimpleReportClientId+["%s","%s"]""" % (self.view_id, self.client_id)
        self.query(data)

        return self

    def set_param_values(self, *args):
        """
        向后台传递参数，设置要导出的数据
        :param args: 参数列表
        :return:
        """
        data = """CompositeService+setParamValuesWithRelated+[%s]""" % ",".join(args)
        self.query(data)
        return self


class CustomerReportClient(ReportClient):

    def __init__(self, session, tag_id):
        super(CustomerReportClient, self).__init__(session, tag_id)

    def set_date(self, date):
        date = time.strftime("%Y-%m-%d", time.strptime(date, "%Y%m%d"))
        self.set_param_values(
            r'"%s"' % self.param_panel_id,
            r'"OutputParameter.%s.p_date"' % self.param_id,
            r'"%s"' % date,
            r'"%s"' % date,
        )
        return self

    def download(self, path):
        data = {
            "type": "TXT",
            "clientId": self.client_id,
            "delimiter": ",",
            "maxRow": -1,
            "mode": "standard",
            "valueType": "value",
            "headerHtml": "",
            "tailHtml": "",
            "combClientType": "combinedQuery",
            "combClientId": self.tag_id,
        }

        res = self.export(data=data)
        with open(path, "wb+") as f:
            f.write(res.content)
    
    
class DepositReportClient(ReportClient):
    
    def __init__(self, session, tag_id):
        super(DepositReportClient, self).__init__(session, tag_id)

    def get_param_root_value(self, *args):
        data = """ParameterPanelService+getParamRootValue+[%s]""" % ",".join(args)
        return self.query(data)

    def get_param_child_value(self, *args):
        data = """ParameterPanelService+getParamChildValue+[%s]""" % ",".join(args)
        return self.query(data)

    def get_param_standby_value(self, *args):
        data = """ParameterPanelService+getParamStandbyValue+[%s]""" % ",".join(args)
        return self.query(data)

    def list_root_inst(self):
        data = (
            r'"%s"' % self.param_panel_id,
            r'"OutputParameter.%s.p_sa_dep_acct_cust_belong_instno"' % self.param_id
        )
        return self.get_param_root_value(*data)

    def list_child_inst(self, inst):
        data = (
            r'"%s"' % self.param_panel_id,
            r'"OutputParameter.%s.p_sa_dep_acct_cust_belong_instno"' % self.param_id,
            r'"pid"',
            r'"%s"' % inst,
        )
        return self.get_param_child_value(*data)

    def list_date(self):
        data = (
            r'"%s"' % self.param_panel_id,
            r'"OutputParameter.%s.p_sa_dep_acct_cust_sys_biz_date"' % self.param_id
        )
        return self.get_param_standby_value(*data)

    def set_inst(self, inst):
        self.set_param_values(
            r'"%s"' % self.param_panel_id,
            r'"OutputParameter.%s.p_sa_dep_acct_cust_belong_instno"' % self.param_id,
            r'"%s"' % inst,
            r'"%s"' % inst,
        )
        return self

    def set_date(self, date):
        #date = time.strftime("%Y-%m-%d", time.strptime(date, "%Y%m%d"))
        self.set_param_values(
            r'"%s"' % self.param_panel_id,
            r'"OutputParameter.%s.p_sa_dep_acct_cust_sys_biz_date"' % self.param_id,
            r'"%s"' % date,
            r'"%s"' % date,
        )
        return self

    def download(self, path):
        data = {
            "type": "TXT",
            "clientId": self.client_id,
            "delimiter": ",",
            "maxRow": -1,
            "mode": "standard",
            "valueType": "value",
            "headerHtml": "<p><br></p>",
            "tailHtml": "",
            "result": "all",
            "contentType": "pageByPage",
            "pageId": "",
            "exportHiddenField": "false",
            "needHiddenParamIds": "",
            "checkedRows": "",
            "headerText": "",
            "tailText": "",
            "combClientType": "combinedQuery",
            "combClientId": self.tag_id,
        }

        res = self.export(data=data)
        with open(path, "wb+") as f:
            f.write(res.content)


class JmReport(object):

    RE_USER_ID = re.compile("^\s*(?!var *)userid *= *(.*)", re.MULTILINE)
    RE_USER_NAME = re.compile("^\s*(?!var *)username *= *(.*)", re.MULTILINE)
    RE_INST_NO = re.compile("^\s*(?!var *)instno *= *(.*)", re.MULTILINE)
    RE_INST_NAME = re.compile("^\s*(?!var *)Instno_name *= *(.*)", re.MULTILINE)
    RE_PINST_NAME = re.compile("^\s*(?!var *)pinstno_name *= *(.*)", re.MULTILINE)
    RE_ROLE_ID = re.compile("^\s*(?!var *)roleid *= *(.*)", re.MULTILINE)
    RE_ROLE_NAME = re.compile("^(?!var *)rolename *= *(.*)", re.MULTILINE)
    RE_TIME = re.compile("^\s*(?!var *)time *= *(.*)", re.MULTILINE)
    RE_COM_ID = re.compile("^\s*(?!var *)comid *= *(.*);", re.MULTILINE)
    RE_AUTH_INFO = re.compile("^\s*authinfo *= *(.*)", re.MULTILINE)

    def __init__(self, user="DY00006", password="202cb962ac59075b964b07152d234b70"):
        super(JmReport, self).__init__()

        self.s = requests.Session()
        self.query = QueryServlet(self.s)

        self.s.headers["Accept"] = "*/*"
        self.s.headers["Accept-Encoding"] = "gzip, deflate"
        self.s.headers["Accept-Language"] = "zh-CN"
        self.s.headers["Connection"] = "Keep-Alive"
        self.s.headers["Content-Type"] = "application/x-www-form-urlencoded;charset=UTF-8"
        self.s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko"

        self.user = user
        self.password = password

        self.bus_id = None
        self.bus_cls_id = None
        self.sub_cls_id = None

        self.cus_id = None
        self.cus_tag_id = None

        self.dep_id = None
        self.dep_tag_id = None

    def close(self):
        self.s.close()

    @classmethod
    def parse_login_info(cls, text):
        return {
            "userid": cls.RE_USER_ID.findall(text)[0].replace(" ", "").replace("'", ""),
            "username": cls.RE_USER_NAME.findall(text)[0].replace(" ", "").replace("'", ""),
            "instno": cls.RE_INST_NO.findall(text)[0].replace(" ", "").replace("'", ""),
            "Instno_name": cls.RE_INST_NAME.findall(text)[0].replace(" ", "").replace("'", ""),
            "pinstno_name": cls.RE_PINST_NAME.findall(text)[0].replace(" ", "").replace("'", ""),
            "roleid": cls.RE_ROLE_ID.findall(text)[0].replace(" ", "").replace("'", ""),
            "rolename": cls.RE_ROLE_NAME.findall(text)[0].replace(" ", "").replace("'", ""),
            "time": cls.RE_TIME.findall(text)[0].replace(" ", "").replace("'", ""),
            "comid": cls.RE_COM_ID.findall(text)[0].replace(" ", "").replace("'", ""),
            "authinfo": cls.RE_AUTH_INFO.findall(text)[0].replace(" ", "").replace("'", ""),
        }

    def login(self):
        self.s.post(
            "http://110.0.170.90:9086/obpm/portal/login/loginWithCiphertext.action",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "110.0.170.90:9086",
            },
            data={
                "_skinType": "",
                "spwd=&returnUrl": "",
                "_showCode": "",
                "myHandleUrl": "null",
                "domainName": "xhrcbdomain",
                "username": self.user,
                "password": self.password,
            },
        )

        res = self.s.post(
            "http://110.0.170.90:9086/obpm/portal/H5/homepage.jsp",
            headers={
                "Host": "110.0.170.90:9086",
            },
        )

        self.s.post(
            "http://110.0.170.88:9083/smartbi/vision/singleLogin.jsp",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "110.0.170.88:9083",
                "Origin": "http://110.0.170.90:9086",
            },
            data=self.parse_login_info(res.text)
        )

        return self

    def composite_login(self):
        data = """CompositeService+compositeLogin+[%s,null]""" % self.user
        return self.query(data)

    def get_child_elements(self, id_):
        data = """CatalogService+getChildElementsWithPurviewType+[%s,"REF"]""" % id_
        return self.query(data)

    def init(self):
        res = self.composite_login()
        result = json.loads(encode(res.text))["result"]
        for r in result:
            method_name = r.get("methodName", None)
            if method_name == "getPublishCatalogsOfCurrentUser":
                result = r.get("result", None)
                break
        if result is None:
            raise ValueError
        for r in result:
            if r["catName"] == "业务报表":
                self.bus_id = r.get("catId", None)

        res = self.get_child_elements(self.bus_id)
        result = json.loads(encode(res.text))["result"]
        for r in result:
            if r["name"] == "业务类报表":
                self.bus_cls_id = r.get("id", None)
            elif r["name"] == "报送类报表":
                self.sub_cls_id = r.get("id", None)

        res = self.get_child_elements(self.bus_cls_id)
        result = json.loads(encode(res.text))["result"]
        for r in result:
            if r["name"] == "存款类报表":
                self.dep_id = r.get("id", None)
            elif r["name"] == "客户信息类":
                self.cus_id = r.get("id", None)

        res = self.get_child_elements(self.cus_id)
        result = json.loads(encode(res.text))["result"]
        for r in result:
            if r["name"] == "CRM-D-001-客户综合信息表":
                self.cus_tag_id = r.get("id", None)

        res = self.get_child_elements(self.dep_id)
        result = json.loads(encode(res.text))["result"]
        for r in result:
            if r["name"] == "DEP-D-012-存款经营数据查询":
                self.dep_tag_id = r.get("id", None)

        return self

    def new_customer_report(self):
        return CustomerReportClient(self.s, self.cus_tag_id)

    def new_deposit_report(self):
        return DepositReportClient(self.s, self.dep_tag_id)


if __name__ == "__main__":
    import getopt
    import sys
    import os

    inst = None
    date = None
    dir_ = None

    opts, args = getopt.getopt(sys.argv[1:], "h", ["version", "type=", "inst=", "date=", "dir="])

    for key, value in opts:
        if key in ("--inst",):
            inst = value.split(",")
        elif key in ("--date",):
            date = value
        elif key in ("--dir",):
            dir_ = value

    if dir_ is None:
        dir_ = "."

    jm = JmReport()
    jm.login().init()

    report = jm.new_customer_report()
    report.init()
    report.set_date(date)
    report.download(os.path.join(dir_, "CUS-D-" + date + ".txt"))

    report = jm.new_deposit_report()
    report.init()
    report.list_root_inst()
    report.list_child_inst("70300")
    report.list_date()
    report.set_date(date)

    for i in inst:
        report.set_inst(i)
        report.download(os.path.join(dir_, "DEP-D-" + i + "-" + date + ".txt"))

    jm.close()

