import pandas as pd

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
    # sucess_number=df[df['state']==10].groupby('penalty_day_borrow_repay').size().reset_index().rename(columns={0:'sucess_number'})
    # print('sucess_number',sucess_number)
    # sucess_number['Proportion']=sucess_number['sucess_number'].apply(lambda row:row/df.shape[0])
    # print('sucess_number', sucess_number)
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


    # def process_user_df(df):
    #     df['Proportion']=df['sucess_number'] / df.shape[0]#有问题，不应该初一df.shape[0],应该除以该催收员的所有订单
    #     return df
    # user_success_per=user_success_num.groupby('user_id_urge_order').apply(lambda df:process_user_df(df)).reset_index()
    # print('user_success_per', user_success_per.head())


    def process_user_df(df):

        df_temp=df[df['state'] == 10].groupby('penalty_day_borrow_repay').size().reset_index().rename(columns={0: 'sucess_number'}) #有问题，不应该初一df.shape[0],应该除以该催收员的所有订单
        df_temp['Proportion']=df_temp['sucess_number']/df.shape[0]
        return df_temp
    user_all_number2=df.groupby('user_id_urge_order').apply(lambda each_df:process_user_df(each_df)).reset_index().drop(columns='level_1')
    print('user_all_number2',user_all_number2.head(20))
    user_all_number2.to_excel('mihua_data/success_user_of_total2.xlsx', index=False)









if __name__=='__main__':
    # urge_merge()
    # borrow_merge()
    per_of_total()