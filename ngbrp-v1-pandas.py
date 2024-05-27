import pandas as pd
from nicegui import ui
import altair as alt
from datetime import datetime
from dateutil.relativedelta import relativedelta
import polars as pl

alt.data_transformers.disable_max_rows()
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

def data():
    df=pd.read_parquet('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\APAC.parquet')
    return df
df=data()

def lv(e):
    cow.options=list(df[e].unique())
    cow.value = df[e].unique()[0]

def gbd1(e,df=df):
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

def cf(sel):
    a.cdf=a.df[a.df[lw.value]==sel.value]
    #a.df=a.df.groupby([lw.value,'IBP Level 5','CatalogNumber','SALES_DATE','year','month']).sum(numeric_only=True).reset_index()
    df1=a.cdf.loc[(a.cdf['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=12)) & (a.cdf['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12)),:]
    df1=df1.groupby('CatalogNumber').sum(numeric_only=True).sort_values(by='actwfc',ascending=False).reset_index()
    a.df1=df1

r1=ui.row()
r2=ui.row()
r3=ui.row()
#with r2:
#    col1=ui.column()
#@pn.depends(ic,watch=True)
def rdf(e):
    lab.text = str(a.count) + " " + a.df1['CatalogNumber'][a.count] + "  " + cow.value
    a.gdf=a.cdf[a.cdf['CatalogNumber']==a.df1['CatalogNumber'][a.count]]
    a.pdf=a.gdf.pivot_table(index='year',columns='month',values='actwfcst',aggfunc="sum")
    a.pdf=a.pdf.rename(columns={i:str(i) for i in a.pdf.columns})
    a.pdf=a.pdf.reset_index()
    print(a.pdf)
    #dfw=ui.aggrid.from_pandas(a.pdf,options={'editable':'true'})
    r3.clear()
    with r3:
        try:
            dfw.delete()
        except:
            pass
        #dfw=ui.aggrid.from_pandas(a.pdf,auto_size_columns=True,options={'editable':True}).on('cellValueChanged', lambda e: scf(e)).style('min-width:1250px;max-height:200px;margin-top:-3px')
        dfw=ui.aggrid({'columnDefs': [{"field":cn,"editable": True} for cn in a.pdf.columns], 'rowData': a.pdf.to_dict('records')}).on('cellValueChanged', lambda e: scf(e)
                                                                                                                ).style('min-width:1250px;max-height:200px;margin-top:-5px')
    #dfw.options['rowData']=[a.pdf.to_records()]
    #dfw.update()
    a.ch1.options['series'][0]['data']= a.gdf[a.gdf['SALES_DATE']>=datetime(today.year,today.month,1)]['`Fcst DF Final Rev'].to_list()
    k=1
    for y in a.gdf['year'].unique():
        a.ch1.options['series'][k]['data']= a.gdf[a.gdf['year']==y].groupby('month').sum(numeric_only=True)['`Act Orders Rev'].to_list()
        a.ch1.options['series'][k]['name']=y
        k+=1
    a.ch1.update()

def nf(e):
    a.count += 1
    ic.value = a.count
def pf(e):
    a.count -= 1
    ic.value = a.count


with r1:
    ic = ui.number(label="Rank",step=1,on_change=rdf).style('width: 50px; margin-top:-14px')
    lab = ui.label("        ").style('font-size:1.5em;width: 350px; margin-top:3px')
    prev = ui.button(icon='arrow_drop_up',text='Prev',on_click=pf)
    next = ui.button(icon='arrow_drop_down',text='Next',on_click=nf)
    lw= ui.select(label='Location',value='Region',options=['Area','Region','Country'],on_change=gbd1).style('min-width: 140px; margin-top:-14px')
    cow= ui.select(label=lw.value,options=list(df[lw.value].unique()),on_change=cf).style('min-width: 160px; margin-top:-14px')
    eb=ui.button(text="Export",icon='file_download')
gbd1(e=lw)

#with r2:
#    dfw=ui.aggrid.from_pandas(a.pdf)

ll=[{'type': 'line', 'data': []}]
ll.extend([{'type':'bar','name':i,'data': []} for i in a.df['year'].unique()])
with r2:
    a.ch1=ui.echart({'xAxis':{'data':a.df['month'].unique()},'yAxis': {},'series': ll}).style('height:390px;width:950px; margin-top:-9px')
    a.ch1.options['tooltip']={}
    a.ch1.options['legend']={}


def scf(e):
    print(e)
    t={'Date':[datetime.today()],'Rank':[a.count],'CatalogNumber':[a.df1['CatalogNumber'][a.count]],'Country':[cow.value],'Stat fcst':[e.args["newValue"]],
       'month':[e.args["colId"]],'year':[a.pdf['year'][e.args["rowIndex"]]],'old fcst':[e.args["oldValue"]]}
    a.fdf=pd.concat([a.fdf,pd.DataFrame(t)],axis=0)

#@pn.depends(ic,watch=True)
def cf1(e):
    print(e)
    a.count = ic.value 

def edf(e):
    a.fdf['Date']=pd.to_datetime(a.fdf['Date'])
    a.fdf['Date']=a.fdf['Date'].dt.date
    a.fdf.to_excel('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\over.xlsx',index=False)
    a.fdf['Date']=pd.to_datetime(a.fdf['Date'])

eb.on_click(edf)

ui.run(title="Forecast Review",binding_refresh_interval=1,reconnect_timeout=70,uvicorn_reload_includes='ngbrp.py')
