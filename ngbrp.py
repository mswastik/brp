import pandas as pd
from nicegui import ui
import altair as alt
from datetime import datetime,date
from dateutil.relativedelta import relativedelta
import polars as pl

alt.data_transformers.disable_max_rows()
alt.themes.enable('powerbi')

today=datetime.today().replace(day=1)

class A():
    def __init__(self): 
        self.df = pl.DataFrame()    # base data
        self.cdf = pl.DataFrame()   #df for filtered specified region
        self.pdf = pl.DataFrame()   #df for pivoted data for display
        self.gdf = pl.DataFrame()   #df for filtered for catalognumber data
        self.fdf = pl.read_excel('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\over.xlsx')
        self.df1 = pl.DataFrame()   #df of ranked catalognumber
        self.count = 0
        self.lvl= 'Region'
        #self.cat = []
a=A()
a.fdf=a.fdf.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))

def data():
    df=pl.read_parquet('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\APAC.parquet')
    return df
df=data()

def lv(e):
    cow.options=list(df[e].unique())
    cow.value = df[e].unique()[0]

def gbd1(e,df=df):
    df=df.with_columns((pl.col('SALES_DATE').dt.month()).alias('month'))
    df=df.with_columns((pl.col('SALES_DATE').dt.year()).alias('year'))
    df1=df.filter(pl.col(lw.value)==e.value)
    df1=df1.group_by([lw.value,'IBP Level 5','CatalogNumber','SALES_DATE','month','year']).sum()
    df1=df1.with_columns(pl.col('`Fcst Stat Final Rev').cast(pl.Int32()))
    df1=df1.with_columns(pl.col('`Act Orders Rev').cast(pl.Int32()))
    df1=df1.with_columns((pl.when(pl.col('SALES_DATE')<=datetime(today.year,today.month,1)-relativedelta(months=1)).then(pl.col('`Act Orders Rev')).otherwise(
                pl.col('`Fcst DF Final Rev'))).alias('actwfc'))
    df1=df1.with_columns((pl.when(pl.col('SALES_DATE')<=datetime(today.year,today.month,1)-relativedelta(months=1)).then(pl.col('`Act Orders Rev')).otherwise(
                pl.col('`Fcst Stat Final Rev'))).alias('actwfcst'))
    df1=df1.filter(pl.col('SALES_DATE')<=datetime(today.year,today.month,1)+relativedelta(months=12))
    a.cdf=df1.filter((pl.col('SALES_DATE')>=datetime(today.year,today.month,1)-relativedelta(months=12)) & (pl.col('SALES_DATE')<=datetime(today.year,today.month,1)+relativedelta(months=12)))
    a.cdf=a.cdf.group_by('CatalogNumber').sum().sort(by='actwfc',descending=True)
    a.df1=df1
    a.df=df

def tmf(e):
    cow.options=df[e.value].unique().to_list()
    cow._props={'label':e.value}
    cow.update()
    r2.clear()
    r3.clear()

r1=ui.row()
r2=ui.row()
r3=ui.row()

def rdf(e):
    cat=a.cdf['CatalogNumber'][a.count]
    lab.text = str(a.count) + " - " + cat + " - " + cow.value + " - " + a.df.filter(pl.col('CatalogNumber')==cat)['IBP Level 5'].unique()[0] + " - " + a.df.filter(
                                                                                                    pl.col('CatalogNumber')==cat)['Franchise'].unique()[0]
    a.gdf=a.df1.filter(pl.col('CatalogNumber')==cat)
    a.pdf=a.gdf.pivot(index='year',columns='month',values='actwfcst',aggregate_function="sum")
    a.pdf=a.pdf[['year']+[str(i) for i in range(1,13)]]
    a.pdf=a.pdf.sort('year')
    r3.clear()
    with r3:
        try:
            dfw.delete()
        except:
            pass
        #dfw=ui.aggrid.from_pandas(a.pdf,auto_size_columns=True,options={'editable':True}).on('cellValueChanged', lambda e: scf(e)).style('min-width:1250px;max-height:200px;margin-top:-3px')
        dfw=ui.aggrid({'columnDefs': [{"field":cn,"editable": True} for cn in a.pdf.columns], 'rowData': a.pdf.to_dicts()}).on('cellValueChanged', lambda e: scf(e)
                                                                                                                ).style('min-width:1250px;max-height:200px;margin-top:-15px')
    
    ll=[{'type': 'line', 'data': []},{'type': 'line', 'data': []}]
    ll.extend([{'type':'bar','name':i,'data': []} for i in a.df['year'].unique().to_list()])
    r2.clear()
    k=2
    with r2:
        a.ch1=ui.echart({'xAxis':{'data':a.df['month'].unique().to_list()},'yAxis': {},'series': ll}).style('height:390px;width:950px; margin-top:3px')
        a.ch1.options['series'][0]['data']= a.gdf.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).sort('month',descending=False)['`Fcst DF Final Rev'].to_list()
        a.ch1.options['series'][0]['name']='DF'
        a.ch1.options['series'][1]['data']= a.gdf.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).sort('month',descending=False)['`Fcst Stat Final Rev'].to_list()
        a.ch1.options['series'][1]['name']='Stat'
        a.ch1.options['tooltip']={}
        a.ch1.options['legend']={}
        for y in a.gdf['year'].unique():
            a.ch1.options['series'][k]['data']= a.gdf.filter(pl.col('year')==y).group_by('month').sum().sort('month',descending=False)['`Act Orders Rev'].to_list()
            a.ch1.options['series'][k]['name']=y
            k+=1
        #a.ch1.update()

def nf(e):
    a.count += 1
    ic.value = a.count
def pf(e):
    a.count -= 1
    ic.value = a.count

with r1:
    with ui.button(icon='menu'):
        with ui.menu() as menu:
            ui.menu_item("Pull Data",lambda: ui.navigate.to('/sql'))
    ic = ui.number(label="Rank",step=1,on_change=rdf).style('min-width: 50px; margin-top:-14px')
    prev = ui.button(icon='arrow_drop_up',text='Prev',on_click=pf)
    next = ui.button(icon='arrow_drop_down',text='Next',on_click=nf)
    lw= ui.select(label='Location',value='Region',options=['Area','Region','Country'],on_change=tmf).style('min-width: 140px; margin-top:-14px')
    cow= ui.select(label=lw.value,options=list(df[lw.value].unique()),on_change=gbd1).style('min-width: 160px; margin-top:-14px')
    eb=ui.button(text="Export",icon='file_download')
    lab = ui.label("        ").style('font-size:1.5em;width: 650px; margin-top:3px')
#gbd1(e=lw)

def scf(e):
    t={'Date':[date.today()],'Rank':[a.count],'CatalogNumber':[a.df1['CatalogNumber'][a.count]],'Country':[cow.value],'Stat fcst':[e.args["newValue"]],
       'month':[int(e.args["colId"])],'year':[a.pdf['year'][e.args["rowIndex"]]],'old fcst':[e.args["oldValue"]]}
    a.fdf=pl.concat([a.fdf,pl.DataFrame(t)])

def cf1(e):
    print(e)
    a.count = ic.value 

def edf(e):
    #a.fdf['Date']=pl.to_datetime(a.fdf['Date'])
    #a.fdf=a.fdf.with_columns((pl.col('Date').dt.date()).alias('Date'))
    a.fdf.write_excel('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\over.xlsx')
    #a.fdf['Date']=pl.to_datetime(a.fdf['Date'])
eb.on_click(edf)

@ui.page('/sql')
def sql():
    from sql import sqlpd
    import os
    class B():
        def __init__(self): 
            self.loc = ''
            self.reg = ''
            self.fr = ''
    b=B()
    with ui.row():
        sd=ui.select(label="Select Division",options=['APAC','EUROPE','Instruments','Trauma and Extremities','CMF','Medical']).style('min-width:210px; margin-top:-14px')
        nm=ui.select(label="Months",options=['-2','-36','-4'],value='-2').style('min-width:100px; margin-top:-14px')
        pb=ui.button(text='Pull Data')
        sb=ui.button(text='Save Data')
        sb.set_visibility(False)
    def selc(e):
        print(sd.value)
        match e.value:
            case "APAC":
                b.loc = "StrykerGroupRegion"
                b.reg=e.value
                b.fr = "'CMF','Endoscopy','Instruments','Joint Replacement','Spine','Trauma and Extremities'"
            case "EUROPE":
                b.loc = "StrykerGroupRegion"
                b.reg=e.value
                b.fr = "'CMF','Endoscopy','Instruments','Joint Replacement','Spine','Trauma and Extremities','Medical'"
            case "Instruments" | "Trauma and Extremities" | "CMF" | "Medical":
                b.loc = "Country"
                b.reg="UNITED STATES"
                b.fr = f"{e.value}"
    sd.on_value_change(selc)
    async def ssf(e):
        df= await sqlpd(b.loc,b.reg,sd.value,b.fr,int(nm.value))
        #print(df.columns)
        ch3=ui.echart({'xAxis':{'data':df['SALES_DATE'].unique().sort().to_list()},'yAxis': {},'series':[{'type': 'bar', 'data': df.group_by('SALES_DATE').sum().sort('SALES_DATE')['`Act Orders Rev'].to_list()}
                                                                                         ,{'type': 'bar', 'data': df.group_by('SALES_DATE').sum().sort('SALES_DATE')['`Fcst Stat Final Rev'].to_list()}]}).style('height:390px;width:950px; margin-top:3px')
        ch3.options['tooltip']={}
        ch3.options['legend']={}
        sb.set_visibility(True)
    def sdf(e):
        os.rename(f'C:\\Users\\smishra14\\OneDrive - Stryker\\data\\{sd.value}.parquet',f'C:\\Users\\smishra14\\OneDrive - Stryker\\data\\Envision\\{sd.value+"-"+today.strftime('%d%m%y')}.parquet')
        df.write_parquet(f'C:\\Users\\smishra14\\OneDrive - Stryker\\data\\{sd.value}.parquet')
    pb.on_click(ssf)
    sb.on_click(sdf)
ui.run(title="Forecast Review",binding_refresh_interval=1,reconnect_timeout=70,uvicorn_reload_includes='ngbrp.py')
