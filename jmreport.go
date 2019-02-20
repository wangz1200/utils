package jmreport

import (
    "encoding/json"
    "errors"
    "fmt"
    "github.com/gocolly/colly"
    "regexp"
    "strings"
)


var schemaEncode = map[string]string {
    "0": "7", "1": "1", "2": "u", "3": "N", "4": "K", "5": "J", "6": "M", "7": "9", "8": "'", "9": "m", "!": "P",
    "%": "/", "'": "n", "(": "A", ")": "E", "*": "s", "+": "+", "-": "f", ".": "q", "A": "O", "B": "V", "C": "t",
    "D": "T", "E": "a", "F": "x", "G": "H", "H": "r", "I": "c", "J": "v", "K": "l", "L": "8", "M": "F", "N": "3",
    "O": "o", "P": "L", "Q": "Y", "R": "j", "S": "W", "T": "*", "U": "z", "V": "Z", "W": "!", "X": "B", "Y": ")",
    "Z": "U", "a": "(", "b": "~", "c": "i", "d": "h", "e": "p", "f": "_", "g": "-", "h": "I", "i": "R", "j": ".",
    "k": "G", "l": "S", "m": "d", "n": "6", "o": "w", "p": "5", "q": "0", "r": "4", "s": "D", "t": "k", "u": "Q",
    "v": "g", "w": "b", "x": "C", "y": "2", "z": "X", "~": "e", "_": "y", }

var schemaDecode = map[string]string {
    "7": "0", "1": "1", "u": "2", "N": "3", "K": "4", "J": "5", "M": "6", "9": "7", "'": "8", "m": "9", "P": "!",
    "/": "%", "n": "'", "A": "(", "E": ")", "s": "*", "+": "+", "f": "-", "q": ".", "O": "A", "V": "B", "t": "C",
    "T": "D", "a": "E", "x": "F", "H": "G", "r": "H", "c": "I", "v": "J", "l": "K", "8": "L", "F": "M", "3": "N",
    "o": "O", "L": "P", "Y": "Q", "j": "R", "W": "S", "*": "T", "z": "U", "Z": "V", "!": "W", "B": "X", ")": "Y",
    "U": "Z", "(": "a", "~": "b", "i": "c", "h": "d", "p": "e", "_": "f", "-": "g", "I": "h", "R": "i", ".": "j",
    "G": "k", "S": "l", "d": "m", "6": "n", "w": "o", "5": "p", "0": "q", "4": "r", "D": "s", "k": "t", "Q": "u",
    "g": "v", "b": "w", "C": "x", "2": "y", "X": "z", "e": "~", "y": "_", }


func EnCode(str string) string {
    out := ""
    for _, chr := range str {
        if _, ok := schemaEncode[string(chr)]; ok {
            out += schemaEncode[string(chr)]
        } else {
            out += string(chr)
        }
    }
    return out
}


func DeCode(str string) string {
    out := ""
    for _, chr := range str {
        if _, ok := schemaDecode[string(chr)]; ok {
            out += schemaDecode[string(chr)]
        } else {
            out += string(chr)
        }
    }
    return out
}


var reUserID *regexp.Regexp
var reUserName *regexp.Regexp
var reInstNo *regexp.Regexp
var reInstName *regexp.Regexp
var rePInstName *regexp.Regexp
var reRoleID *regexp.Regexp
var reRoleName *regexp.Regexp
var reTime *regexp.Regexp
var reComID *regexp.Regexp
var reAuthInfo *regexp.Regexp


const (
    urlLoginAction = "http://110.0.170.90:9086/obpm/portal/login/loginWithCiphertext.action"
    urlHomePage = "http://110.0.170.90:9086/obpm/portal/H5/homepage.jsp"
    urlSingleLogon = "http://110.0.170.88:9083/smartbi/vision/singleLogin.jsp"
    urlQueryServlet = "http://110.0.170.88:9083/smartbi/vision/RMIServlet"
    urlExportServlet = "http://110.0.170.88:9083/smartbi/vision/ExportExtServlet"
)


func defaultCollector() *colly.Collector {
    c := colly.NewCollector()
    c.UserAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko"

    c.OnRequest(func(req *colly.Request) {
        req.Headers.Set("Accept", "*/*")
        req.Headers.Set("Accept-Encoding", "gzip, deflate")
        req.Headers.Set("Accept-Language", "zh-CN")
        req.Headers.Set("Connection", "Keep-Alive")
        req.Headers.Set("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
    })

    return c
}


type JmReport struct {
    c *colly.Collector
    User string
    Password string

    busID string
    busClsID string
    subClsID string

    busCusID string
    cusTagID string

    busDepID string
    depTagID string


}


func(this *JmReport) login() (string, error) {
    ret := ""

    this.c.OnRequest(func (req *colly.Request) {
        req.Headers.Set("Accept", "*/*")
        req.Headers.Set("Accept-Encoding", "gzip, deflate")
        req.Headers.Set("Accept-Language", "zh-CN")
        req.Headers.Set("Connection", "Keep-Alive")
        req.Headers.Set("Content-Type", "application/x-www-form-urlencoded")
    })
    this.c.OnResponse(func(res *colly.Response) {
        ret = string(res.Body)
    })
    err := this.c.Post(urlLoginAction, map[string]string{
        "_skinType": "",
        "spwd=&returnUrl": "",
        "_showCode": "",
        "myHandleUrl": "null",
        "domainName": "xhrcbdomain",
        "username": this.User,
        "password": this.Password,
    })

    return ret, err
}


func(this *JmReport) visitHomePage() (string, error) {
    ret := ""

    c := this.c.Clone()
    c.OnRequest(func(req *colly.Request) {
        req.Headers.Set("Accept", "*/*")
        req.Headers.Set("Accept-Encoding", "gzip, deflate")
        req.Headers.Set("Accept-Language", "zh-CN")
        req.Headers.Set("Connection", "Keep-Alive")
        req.Headers.Set("Content-Type", "application/x-www-form-urlencoded")
    })
    c.OnResponse(func(res *colly.Response) {
        ret = string(res.Body)
    })
    err := c.Post(urlHomePage, nil)

    return ret, err
}


func(this *JmReport) parseLoginInfo(data string) map[string]string {
    info := make(map[string]string)

    info["userid"] = this.User
    info["username"] = "王震"
    info["instno"] = "70300"
    info["Instno_name"] = "东营融和村镇银行股份有限公司"
    info["pinstno"] = "null"
    info["pinstno_name"] = "江门农村商业银行股份有限公司（含村镇银行）"
    info["roleid"] = reRoleID.FindString(data)
    info["rolename"] = "C02村镇总行部门"
    info["time"] = reTime.FindString(data)
    info["comid"] = reComID.FindString(data)
    info["authinfo"] = "xhrcbsmartbi"

    return info
}


func(this *JmReport) singleLogon(data map[string]string) (string, error) {
    ret := ""

    c := this.c.Clone()
    c.OnRequest(func(req *colly.Request) {
        req.Headers.Set("Accept", "*/*")
        req.Headers.Set("Accept-Encoding", "gzip, deflate")
        req.Headers.Set("Accept-Language", "zh-CN")
        req.Headers.Set("Connection", "Keep-Alive")
        req.Headers.Set("Content-Type", "application/x-www-form-urlencoded")

        req.Headers.Set("Host", "110.0.170.88:9083")
        req.Headers.Set("Origin", "http://110.0.170.90:9086",)
    })
    c.OnResponse(func(res *colly.Response) {
        ret = string(res.Body)
    })
    err := c.Post(urlSingleLogon, data)

    return ret, err
}


func(this *JmReport) compositeLogin() (string, error) {
    cmd := `CompositeService+compositeLogin+["%s",null]`
    data := fmt.Sprintf(cmd, this.User)
    ret := ""

    c := this.c.Clone()
    c.OnRequest(func(req *colly.Request) {
        req.Headers.Set("Accept", "*/*")
        req.Headers.Set("Accept-Encoding", "gzip, deflate")
        req.Headers.Set("Accept-Language", "zh-CN")
        req.Headers.Set("Connection", "Keep-Alive")
        req.Headers.Set("Content-Type", "application/x-www-form-urlencoded")
    })
    c.OnResponse(func(res *colly.Response) {
        ret = string(res.Body)
        ret = EnCode(ret)
    })
    err := c.Post(urlQueryServlet, map[string]string {"encode": EnCode(data)})

    return ret, err
}


func(this *JmReport) getChildElements(id string) (string, error) {
    cmd := `CatalogService+getChildElementsWithPurviewType+["%s","REF"]`
    data := fmt.Sprintf(cmd, id)
    ret := ""

    c := this.c.Clone()
    c.OnRequest(func(req *colly.Request) {
        req.Headers.Set("Accept", "*/*")
        req.Headers.Set("Accept-Encoding", "gzip, deflate")
        req.Headers.Set("Accept-Language", "zh-CN")
        req.Headers.Set("Connection", "Keep-Alive")
        req.Headers.Set("Content-Type", "application/x-www-form-urlencoded")
    })
    c.OnResponse(func(res *colly.Response) {
        ret = string(res.Body)
        ret = EnCode(ret)
    })
    err := c.Post(urlQueryServlet, map[string]string {"encode": EnCode(data)})

    return ret, err
}


func(this *JmReport) Init() {
    this.c = colly.NewCollector()
    this.c.UserAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko"

    this.c.OnRequest(func(req *colly.Request) {
        req.Headers.Set("Accept", "*/*")
        req.Headers.Set("Accept-Encoding", "gzip, deflate")
        req.Headers.Set("Accept-Language", "zh-CN")
        req.Headers.Set("Connection", "Keep-Alive")
        req.Headers.Set("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
    })

    this.User = "DY00006"
    this.Password = "202cb962ac59075b964b07152d234b70"
}


func(this *JmReport) parseBusID(data string) error {
    result := make(map[string]interface{})
    err := json.Unmarshal([]byte(data), &result)
    if err != nil {
        return err
    }

    if _, ok := result["result"]; !ok {
        return errors.New("error")
    }

    lst := result["result"].([]interface{})

    for _, result := range lst {
        r := result.(map[string]interface{})
        if _, ok := r["methodName"]; ok {
            if r["methodName"].(string) == "getPublishCatalogsOfCurrentUser" {
                lst = r["result"].([]interface{})
                break
            }
        }
    }

    if len(lst) == 0 {
        return errors.New("error")
    }

    for _, item := range lst {
        if item.(map[string]interface{})["catName"] == "业务报表" {
            this.busID = item.(map[string]interface{})["catId"].(string)
            break
        }
    }

    return nil
}


func(this *JmReport) Login() error {
    res, err := this.login()
    res, err = this.visitHomePage()
    info := this.parseLoginInfo(res)

    res, err = this.singleLogon(info)

    res, err = this.compositeLogin()
    err = this.parseBusID(res)
    if err != nil {
        fmt.Println(err)
    }

    return err
}


type ReportClient struct {
    c *colly.Collector

    idTag string
    idView string
    idParam string
    idClient string
    idParamPanel string
}


func(this *ReportClient) OpenCombinedQuery() error {
    pattern := `CombinedQueryService+openCombinedQuery+["%s",null]`
    data := fmt.Sprintf(pattern, this.idTag)

    c := colly.NewCollector()
    c.OnResponse(func(res *colly.Response) {
        var data map[string]interface{}
        content := EnCode(string(res.Body))
        json.Unmarshal([]byte(content), &data)
        result := data["result"].([]interface{})
        this.idView = result[0].(string)
        this.idParam = result[1].(string)
    })
    err := c.Post(urlQueryServlet, map[string]string {"encode": EnCode(data)})

    return err
}


func(this *ReportClient) CreateSimpleClient() *ReportClient {
    pattern := `CombinedQueryService+createSimpleReport+["%s"]`
    data := fmt.Sprintf(pattern, this.idView)

    c := colly.NewCollector()
    c.OnResponse(func(res *colly.Response) {
        var data map[string]interface{}
        content := EnCode(string(res.Body))
        json.Unmarshal([]byte(content), &data)
        result := data["result"].(map[string]interface{})
        this.idClient = result["clientId"].(string)
        this.idParamPanel = result["parameterPanelId"].(string)
    })
    c.Post(urlQueryServlet, map[string]string {"encode": EnCode(data)})

    return this
}


func(this *ReportClient) InitViewEx() *ReportClient {
    cmd := `CombinedQueryService+initFromBizViewEx+["%s","%s","%s",true]`
    data := fmt.Sprintf(cmd, this.idView, this.idClient, this.idParam)

    c := colly.NewCollector()
    c.Post(urlQueryServlet, map[string]string {"encode": EnCode(data)})

    return this
}


func(this *ReportClient) SetSimpleClientID() *ReportClient {
    cmd := `CombinedQueryService+setSimpleReportClientId+["%s","%s"]`
    data := fmt.Sprintf(cmd, this.idView, this.idClient)

    c := colly.NewCollector()
    c.Post(urlQueryServlet, map[string]string {"encode": EnCode(data)})

    return this
}


func(this *ReportClient) SetParamValues(values ...string) *ReportClient {
    cmd := `CompositeService+setParamValuesWithRelated+[%s]`
    data := fmt.Sprintf(cmd, strings.Join(values, ","))

    c := colly.NewCollector()
    c.Post(urlQueryServlet, map[string]string {"encode": EnCode(data)})

    return this
}


type CustomerReportClient struct {
    ReportClient
}


func(this *CustomerReportClient) SetDate(date string) *CustomerReportClient {
    return this
}


type DepositReportClient struct {
    ReportClient
}


func init() {
    reRoleID = regexp.MustCompile(`.{4}-.{4}-.{8}-.{4}-.{12}`)
    reTime = regexp.MustCompile(`\d{14}`)
    reComID = regexp.MustCompile(`[\w]{32}`)
}