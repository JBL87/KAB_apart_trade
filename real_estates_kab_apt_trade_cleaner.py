from glob import glob
import pandas as pd
import helper
import conn_db

to_folder = conn_db.get_path('한국감정원_아파트거래현황_raw')

# 한국감정원 아파트거래현황 정리
def clean_kab_apt_trade_volume(): 
    codes = {'LHT_65040':'매입자거주지별_아파트거래',
            'LHT_65050':'거래주체별_아파트거래',
            'LHT_65060':'아파트거래현황_거래규모별',
            'LHT_65070':'거래원인별_아파트거래',
            'LHT_67040':'매입자거주지별_아파트매매거래',
            'LHT_67050':'거래주체별_아파트매매거래',
            'LHT_67060':'거래규모별_아파트매매거래',
            'LHT_67070':'매입자연령대별_아파트매매거래'} 

    # (파일명, col_df 테이블명, 주제명, 종류) 순서로
    kab_volume_list = [('월별_거래규모별_아파트거래_동호수.xlsx','거래규모별', '거래규모', '거래'),
                        ('월별_거래주체별_아파트거래_동호수.xlsx','거래주체별', '거래주체', '거래'),
                        ('월별_매입자거주지별_아파트거래_동호수.xlsx','매입자거주지별', '매입자거주지', '거래'),
                        ('월별_거래원인별_아파트거래_동호수.xlsx','거래원인별', '거래원인', '거래'),
                        ('월별_거래규모별_아파트매매거래_동호수.xlsx','거래규모별_매매', '거래규모', '매매'),
                        ('월별_거래주체별_아파트매매거래_동호수.xlsx', '거래주체별_매매', '거래주체', '매매'),
                        ('월별_매입자거주지별_아파트매매거래_동호수.xlsx','매입자거주지별_매매','매입자거주지', '매매'),
                        ('월별_매입자연령대별_아파트매매거래_동호수.xlsx','매입자연령대별','매입자연령대', '매매')]
    # 시트 주소 : https://docs.google.com/spreadsheets/d/1OqMAhVHcXAoJ8lmfuWxz-FzR83Z1IthRITOy4dpS1wE/edit#gid=270159992
    for item in kab_volume_list:
        df = pd.read_excel(helper.download_folder + item[0], skiprows=10) # 전체 df 가져오기

        # 지역 컬럼 부분만 업로드
        conn_db.to_(df.iloc[:, :3],'한국감정원_아파트거래현황',item[1])

        # 전처리된 지역컬럼의 시도,시군구 컬럼만 받기
        col_df = conn_db.from_('한국감정원_아파트거래현황',item[1]+'_col')[['시도', '시군구']]

        df = df.iloc[:, 3:].reset_index()  # 지역 컬럼과 합치기 위해 전처리
        df = df.merge(col_df.reset_index(), on='index').drop(columns='index')

        cols = ['시도', '시군구', item[2]]
        df = df.melt(id_vars=cols, var_name='날짜', value_name='거래량').dropna()
        filt = df['거래량'] == '-' # 거래량에 0이나 null 대신 '-'이 들어간게 있어서 삭제해 줘야함
        df = df.loc[~filt].copy().reset_index(drop=True)
        df['거래량'] = df['거래량'].astype('int')

        #-----------------------------------
        if '거래' in item[3]:
            file = to_folder + f'아파트거래현황_{item[2]}.pkl'
        else:
            file = to_folder + f'아파트매매거래현황_{item[2]}.pkl'
            df.rename(columns={'거래량':'매매거래량'}, inplace=True)

        df.to_pickle(file)
    print('한국감정원 거래현황 저장 완료. 2차 전처리 시작')
    helper.del_all_files_in_download()

    # 저장한 것 전처리 시작
    files = glob(to_folder+'*.pkl')
    cols = ['거래규모', '거래원인', '거래주체', '매입자거주지','매입자연령대']

    df = pd.DataFrame()
    for file in files:
        temp = pd.read_pickle(file)
        temp['시군구'] = temp['시군구'].str.strip()
        col = temp.columns.tolist()
        for x in col:
            if x in cols:
                temp.rename(columns={x:'기준'},inplace=True)
                temp['기준'] = temp['기준'].apply(lambda x : x.replace('->','→') if '->' in x else x)
                temp['Dataset'] = x+'별'
                if x == '거래원인':
                    temp['기준'] = temp['기준'].apply(lambda x : '분양권전매' if x=='분양권' else '기타소유권이전' if x=='기타' else x)
            else:
                pass
        df = df.append(temp)

    # 지역 명칭 mapping
    col = ['시도','시군구']
    temp = df[col].drop_duplicates().sort_values(by=col)
    conn_db.to_(temp, '한국감정원_아파트거래현황','전체지역_import')
    df['시도+시군구_원본'] = df['시도']+" "+df['시군구']

    temp = ['시도+시군구_원본','시도+시군구']
    region_df = conn_db.from_('한국감정원_아파트거래현황','전체지역_mapping')[temp]
    filt = region_df['시도+시군구'].apply(len)>2
    region_df = region_df.loc[filt].copy()
    temp = '시도+시군구_원본'
    df = df.merge(region_df, on=temp, how='inner').drop(columns=temp)
    df[['시도','시군구']] = df['시도+시군구'].str.split(' ',1,expand=True)

    #--------------------------------
    df['출처'] = '한국감정원'
    df['건물유형'] = '아파트'
    df['주기'] = '월간'
    df['시도+시군구'] = df['시도'] +" "+ df['시군구']
    cols = ['Dataset','시도','시군구','시도+시군구',
            '건물유형','주기','기준','출처','날짜']
    df = df.groupby(by=cols).agg('sum').reset_index()
    for col in ['거래량','매매거래량']:
        df[col] = df[col].astype('int')

    df['날짜'] = df['날짜'].str.replace('월','').str.replace('년 ','-')
    
    # 시군구별 좌표 추가후 저장
    df = helper.add_coordinates(df, '시군구')
    conn_db.export_(df, "한국감정원_아파트거래현황")
