import os
import pandas as pd

if __name__=='__main__':
    
    df1 = pd.read_csv('./ml-1m/ratings.csv', sep='::', index_col=0)
    df2 = pd.read_csv('./ml-1m/movies.csv', sep='::', index_col=0)
    df3 = pd.read_csv('./ml-1m/users.csv', sep='::', index_col=0)
    
    df1 = df1.join(df3, on="UserID")
    df1 = df1.join(df2, on="MovieID")
    
    filepath = df1.to_csv('C:\spark\HW4\hw4.csv')