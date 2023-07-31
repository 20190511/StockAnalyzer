import os
from datetime import datetime
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd
import krxdata as krx
import talib as ta

def dt(year=0, mon=0, day=0, strs=""):
    ''' (2023,08,11) or "20230811" 을 datatime 객체로 Translate 함수.
    :param year:
    :param mon:
    :param day:

    :param str:
    :return:
    '''
    if strs != "":
        return datetime(year=int(strs[:4]), month=int(strs[4:6]), day=int(strs[6:]))
    else:
        return datetime(year=year, month=mon, day=day)
def df_t(df : pd.DataFrame, index_num : int):
    ''' DataFrame 인덱스값 범위를 계산해서 구해주는 함수.
    '''
    if index_num < 0 or index_num >= len(df):
        return -1
    return df.loc[index_num]
def writeExcelFromDf(df: pd.DataFrame, path: str, sname: str):
    if len(df) == 0:
        return -1
    if not os.path.isfile(path):
        wb = openpyxl.Workbook()
        wb.create_sheet(sname)
        ws = wb[sname]
        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)
        wb.save(path)
    else:
        wb = openpyxl.load_workbook(filename=path)
        wb_list = wb.sheetnames
        if "Sheet" in wb_list:
            del wb["Sheet"]

        header=False
        if not sname in wb_list:
            wb.create_sheets(sname)
            header = True
        ws = wb[sname]
        for r in dataframe_to_rows(df, index=False, header=header):
            ws.append(r)
        wb.save(path)
def readExcelToDf(path: str, sname: str):
    ''' 액샐 파일에서 데이터 추출
    :param path : 파일 이름
    :param sname : 해당 엑셀 파일 시트 이름
    :return: 엑셀에서 추출한 DataFrame (*없으면 len(df) == 0)
    '''
    if not os.path.isfile(path):
        return pd.DataFrame()  # NULL 데이터프레임 리턴
    wb = openpyxl.load_workbook(filename=path)
    com_list = wb.sheetnames

    if not sname in com_list:
        return pd.DataFrame()

    df_sheet_idx = pd.read_excel(path, sheet_name=sname, engine="openpyxl")
    return df_sheet_idx
def add_excl_column(path: str,  df: pd.DataFrame, sheets :str, append=False):
    ''' 엑셀 열에 데이터 추가 데이터 추가 '''
    if not os.path.isfile(path):
        df.to_excel(path, sheet_name=sheets, index=False, header=True)
        return
    extract_df = df
    if append == True:
        origin_df = readExcelToDf(path=path, sname=sheets)
        extract_df = df_unify(origin_df, df)
    wb = openpyxl.load_workbook(filename=path)

    if not sheets in wb.sheetnames:
        wb.create_sheet(sheets)
    ws = wb[sheets]
    ws.delete_rows(1, ws.max_row)  # 기존 데이터 삭제
    for row in dataframe_to_rows(extract_df, index=False, header=True):
        ws.append(row)
    wb.save(path)
def df_check_row(df: pd.DataFrame, row_name: str):
    ''' DataFrame에 해당 row_name이 존재하는지 여부
    '''
    df_row_list = df.columns
    return row_name in df_row_list
def df_unify (*dfs):
    ''' DataFrame을 합쳐주는 함수.
        ex) df1, df2, df3 데이터를 df로 합쳐줌.'''
    df = pd.concat(list(dfs), axis=1)
    return df.loc[:, ~df.T.duplicated()]
#필요없으나 사용할 수도 있음
def df_slice(df:pd.DataFrame, data_col_list=["종가"], window=6, count=0):
    ''' DataFrame 을 window크기로 청크로 분할한 값을 리턴
    :param df:
    :param data_col_list: 청크에 포함 시킬 데이터 리스트
    :param window: 청크를 자를 단위 ex) 6 이면 6개 크기로..
    :param count: 몇 번째 인덱스 슬라이싱?
    :return:
    '''

    s, e = count, count + window
    #범위를 벗어나면 빈 데이터프레임 리턴.
    if s < 0 or e > len(df):
        return pd.DataFrame()

    ret_df = df_section(df=df[s:e], data_col_list=data_col_list)
    return ret_df
#필요없으나 사용할 수도 있음.
def df_section(df:pd.DataFrame, data_col_list=["종가"]):
    ''' DataFrame 중 data_col_list 리스트 내부의 섹션만 추출함.
    :param df:
    :param data_col_list:
    :return:
    '''
    ret_df = pd.DataFrame()
    ret_df["날짜"] = df["날짜"]
    for item in data_col_list:
        if df_check_row(df=df, row_name=item):
            ret_df[item] = df[item]
    return ret_df
class StockAnaly:
    def __init__(self):
        #pd.set_option('display.max_columns', None)
        self.mykrx = krx.StockKr()

        ''' Path Info
            "주식코드코스피" : "StockCode_KOSPI.txt",
            "주식코드코스닥" : "StockCode_KOSDAQ.txt",
            "관심주" : "WantCode.txt",

            "일봉": "dayinfo.xlsx",
            "일봉거래공매" : "dayinfosub.xlsx"
        '''
        self.data_path = self.mykrx.data_path
        self.analy_path = {
            "지표": "StockCriteria.xlsx",
            "분석": "StockAnaly.xlsx"
        }
        self.cwd = self.mykrx.cwd
        self.pathTok = krx.pathTok
        self.saved_df = pd.DataFrame()          #분석 or 지표계산할 때 총괄 데이터프레임

        #이동평균선 리스트
        self.sma_window = [5,10,20,60,120]
        self.ema_window = [9,12,26]

        # 분석용 DataFrame
        self.read_df_dayinfo = pd.DataFrame()   # 일봉 데이터 가져오는 멤버변수
        self.read_df_criteria = pd.DataFrame()  # 지표 데이터 가져오는 멤버변수
    def df_clear(self):
        ''' 멤버변수 데이터프레임 초기화함수'''
        self.saved_df.drop(index=self.saved_df.index, inplace=True)
        self.read_df_dayinfo.drop(index=self.read_df_dayinfo.index, inplace=True)
        self.read_df_criteria.drop(index=self.read_df_criteria.index, inplace=True)

    def module(self,code_update=False, day_info=False, daysub_info=False, compute_criteria=True, analysis=True):
        self.mykrx.module(code_update, day_info, daysub_info)
        if compute_criteria == True:
            self.module_criteria_init()
        if analysis:
            self.module_analysis()

    ''' 지표 분석 메소드'''
    def module_analysis(self):
        self.df_clear()
        path_criteria = self.cwd + krx.pathTok + self.analy_path["지표"]
        path_analy = self.cwd + krx.pathTok + self.analy_path["분석"]
        path_dayinfo = self.cwd + krx.pathTok + self.data_path["일봉"]
        if not os.path.isfile(path_criteria): #Compute 시작.
            self.module_criteria_init()

        print("Start Analysis")
        for compony, _ in self.mykrx.thema_total_dict.items():  #나중에 이 부분을 건들면 다른 딕셔너리에 대해서도 수행가능.
            print("[" + compony + " 지표 분석 중 ...]")
            self.read_df_dayinfo = readExcelToDf(path=path_dayinfo, sname=compony)
            self.read_df_criteria = readExcelToDf(path=path_criteria, sname=compony)
            self.saved_df["날짜"] = self.read_df_criteria["날짜"]

            #1. XX일 이동선을 통과했는가?
            self.cross_moving_line(df_day=self.read_df_dayinfo, df_crit=self.read_df_criteria)
            #2. MACD 시그널 체킹 (하한~상한 제한걸어둠)
            self.check_macd(self.read_df_criteria)
            #3. *일 최고가 갱신여부?
            self.cross_highest_price(df_day=self.read_df_dayinfo, df_crit=self.read_df_criteria)
            #4. 스팬친구들
            self.cross_backspan(df_day=self.read_df_dayinfo, df_crit=self.read_df_criteria)
            self.check_spantail(df_crit=self.read_df_criteria)
            self.check_span_position(df_day=self.read_df_dayinfo, df_crit=self.read_df_criteria)

            print(self.saved_df.tail(5))
            #데이터 갱신
            add_excl_column(path=path_analy, sheets=compony, df=self.saved_df)
            self.df_clear()

    #1. 평균이동선을 통과했는가?
    def cross_moving_line(self, df_day: pd.DataFrame, df_crit: pd.DataFrame):
        SMA_list = [i for i in df_crit.columns if "SMA" in i]
        for sma in SMA_list:
            naming = sma+"_Cross"
            self.saved_df[naming] = "X"
            mask = (df_day["종가"] >= df_crit[sma])
            self.saved_df.loc[mask, naming] = "O"
    #2. MACD 추이 (Up,Down_Cross)
    def check_macd(self, df_crit: pd.DataFrame(), low=-1000, high=5000):
        up, down = "MACD_Up_Cross", "MACD_Down_Cross"
        #MACD 교차점을 상승하는지점 (매수지점 추천)
        self.saved_df[up] = "X"
        mask = ((low <= df_crit["MACD_Histogram"]) & (df_crit["MACD_Histogram"]<= high))    #MACD 교차점이 -1000 ~ 1000 사이인가?
        mask &= (df_crit["MACD_Histogram"] > df_crit["MACD_Histogram"].shift(1))            #MACD 교차점의 상승중인가?
        mask &= (df_crit["MACD"] > 0)                                                       #MACD 값이 0보다 큰가?
        self.saved_df.loc[mask, up] = "O"
        #and (df_crit["MACD"] >= low or df_crit["MACD"] <= high)) else "X"

        #MACD 교차점을 하강하는지점 (매도지점 추천)
    #3. &일 최고가를 갱신했는가?
    def cross_highest_price(self, df_day: pd.DataFrame, df_crit: pd.DataFrame):
        high_list = [i for i in df_crit.columns if "최고가" in i]
        for hi in high_list:
            naming = hi + "_Cross"
            self.saved_df[naming] = "X"
            mask = (df_day["종가"] >= df_crit[hi])
            self.saved_df.loc[mask, naming] = "O"

    #4-1. 26일전 주식 종가가 후행스팬 꼬리를 뚫었나?
    def cross_backspan(self, df_day: pd.DataFrame, df_crit: pd.DataFrame):
        check_naming, cross_naming = "후행스팬_Position", "후행스팬_Cross"
        self.saved_df[check_naming] = "X"
        mask = (df_day["종가"] >= df_crit["후행스팬"])
        self.saved_df.loc[mask, check_naming] = "O"
        self.saved_df[check_naming] = self.saved_df[check_naming].shift(25)

        self.saved_df.loc[(self.saved_df[check_naming] == "X") & (self.saved_df[check_naming].shift(1) == "O"), cross_naming] = "up"
        self.saved_df.loc[(self.saved_df[check_naming] == "O") & (self.saved_df[check_naming].shift(1) == "X"), cross_naming] = "down"


    #4-2. 현 주가의 스팬 꼬리(3일전부터 모두 상승?) 가 양의 방향인가? + 스팬이 양수인가?
    def check_spantail(self, df_crit: pd.DataFrame):
        naming = "선행스팬_UpTail"
        self.saved_df[naming] = "X"
        mask = (df_crit["선행스팬1_미래"] > df_crit["선행스팬2_미래"])
        mask &= (df_crit["선행스팬1_미래"] > df_crit["선행스팬1_미래"].shift(1))
        mask &= (df_crit["선행스팬1_미래"].shift(1) > df_crit["선행스팬1_미래"].shift(2))
        self.saved_df.loc[mask, naming] = "O"

    #4-3. 현 주가의 스팬 위치 (up, mid, bot)
    def check_span_position(self, df_day: pd.DataFrame, df_crit: pd.DataFrame):
        naming = "선헹스팬_Position"
        self.saved_df[naming] = "X"
        mask = (df_crit["선행스팬1"] >= df_crit["선행스팬2"])
        self.saved_df.loc[(mask) & (df_day["종가"] <= df_crit["선행스팬2"]), naming] = "down"
        self.saved_df.loc[(mask) & (df_day["종가"] >= df_crit["선행스팬1"]), naming] = "up"
        self.saved_df.loc[(mask) & (df_day["종가"] < df_crit["선행스팬1"]) & (df_day["종가"] > df_crit["선행스팬2"]), naming] = "mid"

    ''' 지표 계산 메소드 ...'''
    def module_criteria_init(self, high_crit=240):
        ''' 초기 day_info.xlsx 로부터 MACD, 60평균선 등을 계산하는데 사용하는 함수 (전데이터 수정)
        :param hgih_crit: 최고가 날 기준 (기본값 240일)
        :return:
        '''
        path_data = self.cwd + krx.pathTok + self.data_path["일봉"]
        path_analy = self.cwd + krx.pathTok + self.analy_path["지표"]
        for company, _ in self.mykrx.thema_total_dict.items():
            print("["+company+" 지표 계산중...]")
            df = readExcelToDf(path=path_data, sname=company)
            print(df.tail(5))
            self.saved_df["날짜"] = df["날짜"]

            #1. 주가이동평균 구함.
            print("{주가이동평균(Moving Average) 계산 중 ...}")
            self.movingAverage(cal_df=df)

            #2. MACD 구함.
            print("{MACD(Moving Average Convergence Divergence) 계산 중 ...}")
            self.macd(cal_df=df)

            #3. 일목기준표 구함.
            print("{일목균형표(Ichimoku Kinkoyo) 계산 중 ...}")
            self.ichimoku(cal_df=df)

            #4. X일 중 최고가를 구함.
            print("{"+ str(high_crit) +"일 최고가(highest price) 계산 중 ...}")
            self.highest_price(cal_df=df, high_crit=high_crit)
            print(self.saved_df.tail(5))
            add_excl_column(path=path_analy, df=self.saved_df, sheets=company)
            self.df_clear()
    #이동평균선 구하는 메소드
    def movingAverage(self, cal_df: pd.DataFrame):
        ''' self.saved_df 에 저장된 데이터로 기반으로 이동평균(moving Average)를 계산 '''
        for w in self.sma_window:
            col_name = "SMA"+str(w)
            self.saved_df[col_name] = ta.SMA(cal_df["종가"], timeperiod=w)
        return self.saved_df
    #MACD 구하는 메소드
    def macd(self, cal_df: pd.DataFrame):
        ema12 = ta.EMA(cal_df["종가"], timeperiod=self.ema_window[1])
        ema26 = ta.EMA(cal_df["종가"], timeperiod=self.ema_window[2])
        self.saved_df["MACD"] = ema12 - ema26
        self.saved_df["MACD_Signal"] = ta.EMA(self.saved_df["MACD"], timeperiod=self.ema_window[0])
        self.saved_df["MACD_Histogram"] = self.saved_df["MACD"] - self.saved_df["MACD_Signal"]
        return self.saved_df
    #일목스님 기준표
    def ichimoku(self, cal_df: pd.DataFrame):
        '''
        고가	저가
        전환선: 9일간의 최고가 + 최소가 의 평균
        기준선: 26일간의 최고 + 최소 의 평균
        선행스팬1: 기준선(Kijun-sen)을 26일 전으로 이동시킵니다.
        선행스팬2: 최근 52일의 고가(High)와 저가(Low)를 더한 후, 52로 나눈 값을 26일 전으로 이동시킵니다.
        후행스팬: 현재 주가를 26일 전으로 이동시킵니다.
        '''
        self.saved_df["전환선"] = (cal_df["고가"].rolling(window=9).max() + cal_df["저가"].rolling(window=9).min()) / 2
        self.saved_df["기준선"] = (cal_df["고가"].rolling(window=26).max() + cal_df["저가"].rolling(window=26).min()) / 2
        self.saved_df["선행스팬1"] = ((self.saved_df["기준선"] + self.saved_df["전환선"])/2).shift(26)
        self.saved_df["선행스팬2"] = ((cal_df["고가"].rolling(window=52).max() + cal_df["저가"].rolling(window=52).min()) / 2).shift(26)
        self.saved_df["후행스팬"] = cal_df["종가"].shift(-25)
        self.saved_df["선행스팬1_미래"] = cal_df["종가"] = ((self.saved_df["기준선"] + self.saved_df["전환선"])/2)
        self.saved_df["선행스팬2_미래"] = ((cal_df["고가"].rolling(window=52).max() + cal_df["저가"].rolling(window=52).min()) / 2)
    #X일 가장 고가? --> 9 26
    def highest_price(self, cal_df: pd.DataFrame, high_crit=240):
        ''' high_crit 일 종가 중 최고가?
        :param cal_df:
        :param high_crit:
        :return:
        '''
        naming = str(high_crit) + "일_최고가"
        self.saved_df[naming] = cal_df["종가"].rolling(window=high_crit).max()
if __name__ == "__main__":
    analy = StockAnaly()
    analy.module(compute_criteria=False)