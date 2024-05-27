import pandas as pd
import panel as pn
import altair as alt
from datetime import datetime
from dateutil.relativedelta import relativedelta
import bokeh

alt.data_transformers.disable_max_rows()
pn.extension('vega')
pn.extension('tabulator', inline=True)
pn.extension()
alt.themes.enable('powerbi')

today=datetime.today().replace(day=1)

class A():
    def __init__(self): 
        self.df = pd.DataFrame()    # base data
        self.cdf = pd.DataFrame()   #df for filtered specified region
        self.pdf = pd.DataFrame()   #df for pivoted data for display
        self.gdf = pd.DataFrame()   #df for filtered for catalognumber data
        self.fdf = pd.read_excel('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\over.xlsx')
        self.df1 = pd.DataFrame()   #df of ranked catalognumber
        self.count = 0
        self.lvl= 'Region'
        self.cat = []
a=A()

ic = pn.widgets.IntInput(name="Rank",value=a.count,step=1,width=65)
lab = pn.pane.Str("                            ",styles={'font-weight':'bold','font-size': '13pt','padding-top':'18px'})
next = pn.widgets.Button(icon='caret-down-filled',name='Next', button_type='primary',styles={'padding-top':'18px'})
prev = pn.widgets.Button(icon='caret-up-filled',name='Prev', button_type='primary',styles={'padding-top':'18px'})

def data():
    df=pd.read_parquet('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\APAC.parquet')
    return df

df=data()
lw= pn.widgets.Select(name='Location',value='Region',options=['Area','Region','Country'],width=130)
cow= pn.widgets.Select(name=lw.rx(),options=list(df[lw.value].unique()),width=130)

@pn.depends(lw,watch=True)
def lv(e):
    cow.options=list(df[e].unique())
    cow.value = df[e].unique()[0]

@pn.depends(lw,watch=True)
def gbd1(e,df=df):
    print(e)
    df=df.groupby([e.value,'IBP Level 5','CatalogNumber','SALES_DATE']).sum(numeric_only=True).reset_index()
    df['month']=df['SALES_DATE'].dt.month
    df['year']=df['SALES_DATE'].dt.year
    df['year']=df['year'].astype('str')
    df.loc[df['SALES_DATE']<datetime(today.year,today.month,1),'actwfc']=df['`Act Orders Rev']
    df.loc[df['SALES_DATE']>=datetime(today.year,today.month,1),'actwfc']=df['`Fcst DF Final Rev']
    df.loc[df['SALES_DATE']<datetime(today.year,today.month,1),'actwfcst']=df['`Act Orders Rev']
    df.loc[df['SALES_DATE']>=datetime(today.year,today.month,1),'actwfcst']=df['`Fcst Stat Final Rev']
    df=df[df['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12)]
    a.df=df

gbd1(e=lw)
r1 = pn.Row().servable()
r2= pn.Row().servable()
r3= pn.Row().servable()

@pn.depends(cow,lw,watch=True)
def cf(sel,lv1):
    #gbd1(e=lv1)
    a.cdf=a.df[a.df[lw.value]==sel]
    a.df=a.df.groupby([lv1,'IBP Level 5','CatalogNumber','SALES_DATE','year','month']).sum(numeric_only=True).reset_index()
    df1=a.cdf.loc[(a.cdf['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=12)) & (a.cdf['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12)),:]
    df1=df1.groupby('CatalogNumber').sum(numeric_only=True).sort_values(by='actwfc',ascending=False).reset_index()
    a.df1=df1


r1.append(ic)
r1.append(lab)
r1.append(prev)
r1.append(next)
r1.append(lw)
r1.append(cow)

def scf(e):
    print(a.df1)
    t={'Date':[datetime.today()],'Rank':[a.count],'CatalogNumber':[a.df1['CatalogNumber'][a.count]],'Country':[cow.value],'Stat fcst':[e.value],'month':[e.column],'year':[a.pdf.index[e.row]],'old fcst':[e.old]}
    a.fdf=pd.concat([a.fdf,pd.DataFrame(t)],axis=0)

@pn.depends(ic,watch=True)
def cf1(e):
    print(e)
    a.count = ic.value 

@pn.depends(ic,watch=True)
def rdf(e):
    lab.object = str(a.count) + " " + a.df1['CatalogNumber'][a.count] + "  " + cow.value
    a.gdf=a.cdf[a.cdf['CatalogNumber']==a.df1['CatalogNumber'][a.count]]
    a.pdf=a.gdf.pivot_table(index='year',columns='month',values='actwfcst',aggfunc="sum")
    a.pdf=a.pdf.rename(columns={i:str(i) for i in a.pdf.columns})
    frmt={i:bokeh.models.NumberFormatter(format="0,0",text_align='left') for i in a.pdf.columns}
    dfw=pn.widgets.Tabulator(a.pdf,formatters=frmt,widths={i:75 for i in a.pdf.columns})
    dfw.on_edit(scf)
    r2.clear()
    r3.clear()
    r2.append(dfw)
    c1=alt.Chart(a.gdf).mark_bar().encode(x='month:N',y='`Act Orders Rev',color='year:N', xOffset="year:N",tooltip=['year','`Act Orders Rev']).properties(width=790)
    c2=alt.Chart(a.gdf[a.gdf['SALES_DATE']>=datetime(today.year,today.month,1)]).mark_line().encode(x='month:N',y='`Fcst DF Final Rev',color='year:N',tooltip=['year','`Fcst DF Final Rev'])
    c3=alt.Chart(a.gdf[a.gdf['SALES_DATE']>=datetime(today.year,today.month,1)]).mark_line(stroke='blue').encode(x='month:N',y='`Fcst Stat Final Rev',color='year:N',tooltip=['year','`Fcst Stat Final Rev'])
    r3.append(pn.Row(pn.pane.Vega(c1+c2+c3)))
    r3.append(pn.pane.Vega(alt.Chart(a.gdf).mark_line().encode(x='SALES_DATE:T',y='`Act Orders Rev:Q',tooltip=['SALES_DATE:T','`Act Orders Rev:Q']).properties(width=290)))

def nf(e):
    a.count += 1
    ic.value = a.count
def pf(e):
    a.count -= 1
    ic.value = a.count

next.on_click(nf)
prev.on_click(pf)
def edf(e):
    a.fdf['Date']=pd.to_datetime(a.fdf['Date'])
    a.fdf['Date']=a.fdf['Date'].dt.date
    a.fdf.to_excel('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\over.xlsx',index=False)
    a.fdf['Date']=pd.to_datetime(a.fdf['Date'])
eb=pn.widgets.Button(name="Export",button_type='primary',styles={'padding-top':'18px'})
r1.append(eb)
eb.on_click(edf)
