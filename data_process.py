import pandas as pd
import time

def borrow_merge():
    borrow_repay=pd.read_csv('mihua_data/borrow_repay_2213603.csv',usecols=['borrow_id','repay_time','state','penalty_day','amount'])
    borrow_repay_log=pd.read_csv('mihua_data/borrow_repay_log_2213607.csv',usecols=['borrow_id','amount','penalty_day','repay_time'])
    borrow=pd.merge(borrow_repay,borrow_repay_log, how='left', on='borrow_id',suffixes=('_borrow_repay', '_borrow_repay_log'))
    urge_order = pd.read_csv('mihua_data/urge_order_2213594.csv', usecols=['borrow_id','borrow_time', 'count', 'state','user_id']).rename(
        columns={'count':'count_urge_order','state':'state_urge_order','borrow_time':'borrow_time_urge_order','user_id':'user_id_urge_order'})
    # print('urge_order',urge_order.head())
    borrow_urge_data=pd.merge(borrow,urge_order,how='left', on='borrow_id')

    borrow_urge_data.to_csv('mihua_data/borrow_urge.csv',index=False,encoding='gbk')



    # borrow.to_csv('mihua_data/borrow.csv',index=False,encoding='gbk')
def urge_merge():
    urge_order=pd.read_csv('mihua_data/urge_order_2213594.csv',usecols=['borrow_id','count','state'])
    urge_order_log=pd.read_csv('mihua_data/urge_log_2213599.csv',usecols=['borrow_id','state','remark','create_time','way','tag'])
    borrow = pd.merge(urge_order, urge_order_log, how='left', on='borrow_id',
                      suffixes=('_urge_order', '_urge_order_log'))
    borrow.to_csv('mihua_data/urge.csv', index=False, encoding='gbk')

def per_of_total():
    df=pd.read_csv('mihua_data/borrow_urge.csv')
    print('df.shape',df.shape)
    sucess_number=df[df['state']==10].groupby('penalty_day_borrow_repay').size().reset_index().rename(columns={0:'sucess_number'})
    print('sucess_number',sucess_number)
    sucess_number['Proportion']=sucess_number['sucess_number'].apply(lambda row:row/df.shape[0])
    print('sucess_number', sucess_number)
    # sucess_number.to_csv('mihua_data/success_per_of_total.csv',index=False,encoding='gbk')


    user_success_num=df[df['state'] == 10].groupby(['user_id_urge_order','penalty_day_borrow_repay']).size().reset_index().rename(columns={0: 'sucess_number'})
    user_all_number=df.groupby(['user_id_urge_order']).size().reset_index().rename(columns={0: 'all_number'})

    print('user_success',user_success_num.head())
    print('user_all_number',user_all_number.head())

    user_all_number=pd.merge(user_success_num,user_all_number,how='left',on='user_id_urge_order')

    pd.set_option('display.max_columns', None)
    print('user_all_number2',user_all_number.head(30))

    user_all_number['Proportion'] = user_all_number['sucess_number'] / user_all_number['all_number']

    print('user_all_number', user_all_number.head(50))
    # user_all_number.to_excel('mihua_data/success_user_of_total.xlsx', index=False)



    # #这个也可以算结果是一样的
    # def process_user_df(df):
    #     df_temp=df[df['state'] == 10].groupby('penalty_day_borrow_repay').size().reset_index().rename(columns={0: 'sucess_number'}) #有问题，不应该初一df.shape[0],应该除以该催收员的所有订单
    #     df_temp['Proportion']=df_temp['sucess_number']/df.shape[0]
    #     return df_temp
    # user_all_number2=df.groupby('user_id_urge_order').apply(lambda each_df:process_user_df(each_df)).reset_index().drop(columns='level_1')
    # print('user_all_number2',user_all_number2.head(20))
    # user_all_number2.to_excel('mihua_data/success_user_of_total2.xlsx', index=False)

def process_dup():
    df=pd.read_csv('mihua_data/urge_log_2213599.csv')#usecols=["borrow_id","create_time"]
    df['date']=df.apply(lambda  row:row['create_time'].split()[0],axis=1)
    df=df.sort_values(by='date',ascending=True)
    print(df.head(30))
    unique_date=df['date'].unique()
    print('unique_date',len(unique_date),unique_date)#26

    print('nunique_borrow_id',df['borrow_id'].nunique())
    all=set()
    for each in range(len(unique_date)):
         if each==len(unique_date)-1:
            break
         section=set(df.loc[df['date']==unique_date[each],'borrow_id'])&set(df.loc[df['date']==unique_date[each+1],'borrow_id'])
         if len(section)>0:
             all=all|section
             print('section',unique_date[each],unique_date[each+1],len(section),section)
    print('len(all)',len(all),all)

    #根据borrow_id找出同一个案件至少在两天中都被催收过的案件
    multi_urge_df=df[df['borrow_id'].isin(all)].sort_values(by=['borrow_id', 'create_time'])
    # multi_urge_df.to_excel('mihua_data/multi_urge.xlsx', index=False)
    print(multi_urge_df.head())

    borrow_id_30_nunique=multi_urge_df.loc[multi_urge_df['state']==30,'borrow_id'].nunique()
    print('borrow_id_30_nunique',borrow_id_30_nunique)


    # print(df.loc[df['date']=='2018-12-21','borrow_id'])


    # def dup_process(df):
    #     amount=df['borrow_id'].shape[0] - df['borrow_id'].nunique()
    #     print('amount',amount)
    #     return amount
    #
    #
    # df_dup=df.groupby('date').apply(lambda df:dup_process(df)).reset_index().rename(columns={0:'dup_counts'})
    # print('df_dup',df_dup.head(50))


def time_hour_process(total_amounts=4016):
    df=pd.read_csv('mihua_data/borrow_repay_log_2213607.csv')
    print('df.head()',df.head())
    def hour_process(line):
        hour=int(line.split()[1].split(':')[0])
        # print('hour', hour)
        for i in range(24):
            if i<=hour and hour<i+1:
                return i


    df['hour']=df.apply(lambda row:hour_process(row['repay_time']),axis=1)
    print('df.head()2', df.head())
    df_hour_counts=df.groupby('hour').size().reset_index().rename(columns={0:'success_number'})
    df_hour_counts['Proportion']=df_hour_counts['success_number']/total_amounts
    print('df_hour_counts',df_hour_counts.head(24))
    df_hour_counts.to_excel('mihua_data/hour_success_number.xlsx', index=False)

def each_urge_success_number():
    df = pd.read_csv('mihua_data/urge_log_2213599.csv')
    print('df.head()', df.head())
    def process_df(df):
        dict={}
        all_num=df.shape[0]
        success_num=df[df['state']==40].shape[0]
        dict['urge_num']=all_num
        dict['success_state']=success_num
        # print('all_num={},success_num={}'.format(all_num,success_num))

        return pd.DataFrame([dict])

    df_new=df.groupby('borrow_id').apply(lambda df:process_df(df)).reset_index().drop(columns='level_1').sort_values(by='urge_num',ascending=False)
    print('df_new',df_new)
    df_new=df_new[df_new['success_state']==1]
    # df_new.to_excel('mihua_data/every_urge_success_number.xlsx', index=False)


    df_urge_num_success_num=df_new.groupby('urge_num').size().reset_index().rename(columns={0:'success_number'})
    print('df_urge_num_success_num',df_urge_num_success_num.head())
    df_urge_num_success_num.to_excel('mihua_data/urge_num_success_num.xlsx', index=False)



if __name__=='__main__':
    # urge_merge()
    # borrow_merge()
    # per_of_total()
    # process_dup()
    # time_hour_process(total_amounts=4016)
    each_urge_success_number()