import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from pykiwoom.kiwoom import *
import os
import pandas as pd
import datetime
import time

class KiwoomEvent():
    def __init__(self):
        self.today = self.today_date()
        self.code_company_date = []
        self.code_list = []

        #로그인
        self.kiwoom = Kiwoom()
        self.kiwoom.CommConnect(block=True)     #block는 자동으로 True 처리 (Blocking 처리)

        #오늘 날짜 얻기
    def today_date(self):
        now = datetime.datetime.now()
        today = now.strftime("%Y%m%d")
        return today

        #현 로그인 상태
    def connect_code(self):
        err_code = self.kiwoom.GetConnectState()
        if err_code == 1:
            print("연결됨.")
        elif err_code == 0:
            print("연결되지 않음.")

    """ 실시간 데이터 사용시 
        실시간 데이터는 SetRealReg()함수로 연결 -> 데이터전달 -> DisConnectRealData()로 연결 끊기
        이후 GetCommRealData로 데이터 받아옴.
    """
        # 실시간 데이터 연결 -> 2021_03-21 제작중,
    def real_data(self, code_arr, fid_num):
        self.kiwoom.SetRealReg("0101")

        self.kiwoom.SetRealRemove("0101")


    """ 실시간 데이터 사용시"""

        #전 종목의 코드 받아오기 (block_request함수를 이용해서 한꺼번에 데이터를 받아오는 것이 가능)
    def all_chart_code_extract(self):
        kospi = self.kiwoom.GetCodeListByMarket("0")
        kosdaq = self.kiwoom.GetCodeListByMarket("10")
        all_chart = kospi+kosdaq

        #파이썬 enumerate 사용법 : https://wikidocs.net/16045
        for i, code in enumerate(all_chart):
            print(f"{i}/{len(all_chart)} {all_chart}")
            dataframe_ = self.kiwoom.block_request("opt10081",
                                                   종목코드=code,
                                                   기준일자=self.today,
                                                   수정주가구분=1,
                                                   output="주식일봉차트조회",
                                                   next=0)
            out_name = f"{code}.xlsx"
            dataframe_.to_excel("D:/PycharmProject/Test/PyKiwoomProject/dayData", sheet_name=out_name)
            time.sleep(3.6)

        # 엑셀파일에서 데이터 추출.
    def all_chart_code_intract(self):

        file_list = os.listdir("D:/PycharmProject/Test/PyKiwoomProject/dayData")
        xlst_list = [num for num in file_list if num.endswith(".xlsx")]
        close_data = []

            # 종가로 데이터 추출.
        for xls in xlst_list:
            code = xls.split(".")[0]
            data_frame_ = pd.read_excel(xls)
            data_frame2_ = data_frame_[["일자", "현재가"]].copy()
            data_frame2_ = data_frame2_.set_index("일자")
            data_frame2_ = data_frame2_[::-1]
            close_data.append(data_frame2_)
        data_frame_ = pd.concat(close_data, axis=1)
        data_frame2_.to_excel("merge.xlsx")

    """
        <Err_code>
             0   : 정상
            -200 : 시세조회 과부화
            -201 : 입력 구조체 생성 실패
            -202 : 전문작성 입력값 오류
    """
        #일봉차트 조회시 Send하는 역할 --> 후에 self.kiwoom.GetCommData()를 이용해서 데이터를 받아와야함.
    def _opt10001_send(self, market=""):
        if market == "":
            market = "039490"
        self.kiwoom.SetInputValue("종목코드", market)
        err_code = self.kiwoom.CommRqData("opt10001_req", "opt10001", 0, "0101")
        return err_code

    def _opt10081_send(self, market="", date="today"):
        if market == "":
            market = "039490"
        if date == "today":
            date = self.today
        self.kiwoom.SetInputValue("종목코드", market)
        self.kiwoom.SetInputValue("기준일자", date)
        self.kiwoom.SetInputValue("수정주가구분", 1)

        err_code = self.kiwoom.CommRqData("opt10081_req", "opt10081", 0, "0101")
        return err_code

    def _opt10082_send (self, market="", date="today", end_date="today"):
        if market == "":
            market = "039490"
        if date == "today":
            date = self.today
        if end_date == "today":
            end_date = self.today - 20000

        self.kiwoom.SetInputValue("종목코드", market)
        self.kiwoom.SetInputValue("기준일자", date)
        self.kiwoom.SetInputValue("끝일자", end_date)
        self.kiwoom.SetInputValue("수정주가구분", 1)

        err_code = self.kiwoom.CommRqData("opt10082_req", "opt10082", 0, "0101")
        return err_code

    # 종목코드(0) + 회사명(1) + 상장일(2) -> 멤버변수 self.code_company_date에 저장.
    def _basic_data_(self):
        kospi = self.kiwoom.GetCodeListByMarket("0")
        kosdaq = self.kiwoom.GetCodeListByMarket("10")
        all_chart = kospi + kosdaq
        self.code_list = all_chart      #모든 코드 차트 저장
        code_len = len(all_chart)

        for code, index_num in enumerate(all_chart):
            temp_list = []
            company_name = self.kiwoom.GetMasterCodeName(code)
            company_date = self.kiwoom.GetMasterListedStockDate(code)
            temp_list.append(code)
            temp_list.append(company_name)
            temp_list.append(company_date)
            self.code_company_date.append(temp_list)













kiwooms = KiwoomEvent()
    #주의 all_chart_code는 3시간가량 소요됨
#kiwooms.all_chart_code()