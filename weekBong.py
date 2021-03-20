import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
from operator import *
import time

TR_REQ_TIME_INTERVAL = 0.2

class Kiwoom (QAxWidget):
    def __init__(self):
        super().__init__()
        self._kiwoom_instance()
        self._set_signal_slot()

    def _kiwoom_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")         #setControl()은 메소드를 객체없이 사용 가능한 함수.

    def _set_signal_slot(self):
        self.OnEventConnect.connect(self._event_connect)        # 로그인 상태와 함께 연결 시도
        self.OnReceiveTrData.connect(self._receive_tr_data)      # tran값들을 넘겨주는 함수.

    """ 이벤트 함수  """
    #로그인 함수
    def comm_connect(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    # 로그인 상태 함수.
    def _event_connect (self, err_code):
        if err_code == 0:
            print("연결됨")
        else:
            print("연결되지 않음")

        self.login_event_loop.exit()

    #trans data를 넘겨주는 함수.
    def _receive_tr_data (self, screen_no, rqname, trcode, record_name, next, unused1, unused2, unused3, unused4):
        if next == '2':
            self.remained_data = True
        else:
            self.remained_data = False

        if rqname == "opt10081_req":
            self._opt10081(rqname, trcode)
        elif rqname == "opt10082_req":
            self._opt10081(rqname, trcode)

        try:
            self.tr_event_loop.exit()
        except AttributeError:
            pass

    #시장구분에 따른 종목 코드 반환 (리스트 반환)
    def get_code_list_by_market (self, market):
        code_list = self.dynamicCall("GetCodeListByMarket(QString)", market)
        code_list = code_list.split(";")
        return code_list[:-1]

    #종목코드의 한글 이름 반환 (하나 반환)
    def get_master_code_name (self, market_code):
        code_name = self.dynamicCall("GetMasterCodeName(QString)", market_code)
        return code_name

    def get_master_listed_stock_date(self, market_code):
        dateOfCompany = self.dynamicCall("GetMasterListedStockDate (QString)", market_code)
        return dateOfCompany

    # 주식 전체 차트 정렬 함수.
    def _list_of_code(self):
        code_list = self.get_code_list_by_market(0)
        name_list = []

        cnt = len(code_list)
        for index_num in range(cnt):
            total_list = []
            company_name = self.get_master_code_name(code_list[index_num])
            date_of_company = self.get_master_listed_stock_date(code_list[index_num])
            # print(code_list[index_num], " : ", company_name)
            # print("상장일 : ", date_of_company, "\n")
            total_list.append(code_list[index_num])
            total_list.append(company_name)
            total_list.append(date_of_company)
            name_list.append(total_list)
        return name_list

    # 상장일자로 정렬된 차트.
    def sort_of_stock_listing_date(self):
        name_dict = self._list_of_code()
        sort_list = sorted(name_dict, key=itemgetter(2))
        return sort_list







    """ Requestion + Response"""
    # Request에 보내는함수
    def set_input_value (self, id, value):
        self.dynamicCall("SetInputValue(QString, QString)", id, value)

    # Requestion :앞의 SetInputValue()를 송신하는 함수
    def comm_rq_data (self, rqname, trcode, next, screen_no):
        self.dynamicCall("CommRqData(QString, QString, int, QString)", rqname, trcode, next, screen_no)
        self.tr_event_loop = QEventLoop()
        self.tr_event_loop.exec_()

    # Response :Data를 받아오는 함수
    def get_comm_data (self, Trancode, reqname, index, item_name):
        ret = self.dynamicCall("GetCommData(QString, QString, int, QString)",
                                Trancode, reqname, index, item_name)
        return ret

    #레코드 반복횟수를 반환한다. (Record Repeat Count)
    def _get_repeat_cnt (self, trcode, rqname):
        ret = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
        return ret


    #일봉+주봉 차트 기준
    def _opt10081(self, rqname, trcode):
        data_cnt = self._get_repeat_cnt(trcode, rqname)   #데이터 개수 반환
        print("DataCnt = ", data_cnt)

        print("data = [일자, 시가, 고가, 저가, 현재가, 거래량]")
        for index_num in range(data_cnt):
            date = self.get_comm_data(trcode, rqname, index_num, "일자")
            open = self.get_comm_data(trcode, rqname, index_num, "시가")
            high = self.get_comm_data(trcode, rqname, index_num, "고가")
            low = self.get_comm_data(trcode, rqname, index_num, "저가")
            close = self.get_comm_data(trcode, rqname, index_num, "현재가")
            volume = self.get_comm_data(trcode, rqname, index_num, "거래량")
            print(date, open, high, low, close, volume)

    def login_and_input(self):
            #기본값 설정
        check = input("(일봉=1, 주봉=2)) : ")
        check = check.strip()

        market_code = input("종목 코드 (ex)039490)   :")
        std_date = input("기준 일자 (ex)20210320) : ")
        end_date = input("시작 날짜 (ex)20180320) : ")

        if market_code == "":
            market_code = "039490"
        if std_date == "":
            std_date = "20210320"
        if end_date == "":
            end_date = str(int(std_date)-20000)

        name = self.get_master_code_name(market_code)
        print("해당 종목코드의 회사 이름 : ",name)
            #opt10081요청
        self.set_input_value("종목코드", market_code)
        self.set_input_value("기준일자", std_date)
            #opt10082요청의 경우
        if check == "2":
            self.set_input_value("끝일자", end_date)
        self.set_input_value("수정주가구분", 1)


        if check == "1":
            self.comm_rq_data("opt10081_req", "opt10081", 0, "0101")
        elif check == "2":
            self.comm_rq_data("opt10082_req", "opt10082", 0, "0101")



if __name__ == "__main__":
    app = QApplication(sys.argv)
    kiwoom = Kiwoom()
    kiwoom.comm_connect()
    # kiwoom.login_and_input()

    list = kiwoom.sort_of_stock_listing_date()
    for temp in list:
        print(temp)

