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
        self.cat = ''
a=A()
#a.fdf=a.fdf.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))

navcl='w-24 drop-shadow-none shadow-none text-[#FFF] hover:font-black hover:bg-[#FFF] hover:text-[#000] rounded-none transition-all duration-200 ease-linear'
def nav():
    with ui.row(wrap=False).style(f'width:100dvw;max-height: 35px; margin-top:-15px; margin-left:-16px').classes('bg-[#000]'):
        with ui.link(target='/'):
            ui.button('Home',color=None).classes(navcl).props('flat')
        with ui.link(target='/detail'):
            ui.button('Detail',color=None).classes(navcl).props('flat')
        with ui.link(target='/sql'):
            ui.button('SQL',color=None).classes(navcl).props('flat')

nav()
ui.add_css('''
    i {
        margin-right: -3px !important;
        margin-left: -6px !important;
    }''')

def data():
    df=pl.read_parquet('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\APAC.parquet')
    return df
df=data()
a.df1=df

def lv(e):
    cow.options=list(df[e].unique())
    cow.value = df[e].unique()[0]

def gbd1(df=a.df1):
    df1=df.clone()
    try:
        print(len(df1[['month']]))
    except:
        df1=df1.with_columns((pl.col('SALES_DATE').dt.month()).alias('month'))
        df1=df1.with_columns((pl.col('SALES_DATE').dt.year()).alias('year'))
    #try:
    #    df1=df.filter(pl.col(lw.value)==e.value)
    #    df1=df1.group_by([lw.value,'Franchise','Business Unit','IBP Level 5','CatalogNumber','SALES_DATE','month','year']).sum()
    #except:
    #    df1=df.filter(pl.col(pw.value)==e.value)
    #print(df1)
    df1=df1.with_columns(pl.col('`Fcst Stat Final Rev').cast(pl.Int32()))
    df1=df1.with_columns(pl.col('`Act Orders Rev').cast(pl.Int32()))
    df1=df1.with_columns((pl.when(pl.col('SALES_DATE')<=datetime(today.year,today.month,1)-relativedelta(months=1)).then(pl.col('`Act Orders Rev')).otherwise(
                pl.col('`Fcst DF Final Rev'))).alias('actwfc'))
    df1=df1.with_columns((pl.when(pl.col('SALES_DATE')<=datetime(today.year,today.month,1)-relativedelta(months=1)).then(pl.col('`Act Orders Rev')).otherwise(
                pl.col('`Fcst Stat Final Rev'))).alias('actwfcst'))
    df1=df1.filter(pl.col('SALES_DATE')<datetime(today.year,today.month,1)+relativedelta(months=12))
    #a.cdf=df1.filter((pl.col('SALES_DATE')>=datetime(today.year,today.month,1)-relativedelta(months=12)) & (pl.col('SALES_DATE')<=datetime(today.year,today.month,1)+relativedelta(months=12)))
    a.cdf=df1.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)-relativedelta(months=12))
    a.cdf=a.cdf.group_by('CatalogNumber').sum().sort(by='actwfc',descending=True)
    a.cdf=a.cdf.with_row_index()
    a.df1=df1
    a.df=df

def hs(e):
    try:
        print(len(a.df1[['month']]))
    except:
        df1=a.df1.with_columns((pl.col('SALES_DATE').dt.month()).alias('month'))
        df1=df1.with_columns((pl.col('SALES_DATE').dt.year()).alias('year'))
    df1=df1.filter(pl.col(lw.value)==e.value)
    df1=df1.group_by([lw.value,'Franchise','Business Unit','IBP Level 5','CatalogNumber','SALES_DATE','month','year']).sum()
    gbd1(df1)

def phs(e):
    try:
        print(len(a.df1[['month']]))
    except:
        df1=a.df1.with_columns((pl.col('SALES_DATE').dt.month()).alias('month'))
        df1=df1.with_columns((pl.col('SALES_DATE').dt.year()).alias('year'))
    df1=a.df1.filter(pl.col(pw.value)==e.value)
    gbd1(df1)

def tmf(e):
    cow.options=df[e.value].unique().to_list()
    cow._props={'label':e.value,'with_input':True}
    cow.update()
    r2.clear()
    r3.clear()

def pmf(e):
    pvw.options=df[e.value].unique().to_list()
    pvw._props={'label':e.value,'with_input':True}
    pvw.update()
    r2.clear()
    r3.clear()

#r0=ui.row().style('background:black;margin-top:2px;padding-top:2px;width:99%')
r1=ui.row()
r2=ui.row()
r3=ui.row()

def rdf1(cat:str):
    lab.text = str(a.cat) + " - "+ a.df.filter(pl.col('CatalogNumber')==cat)['Franchise'].unique()[0]
    a.gdf=a.df1.filter(pl.col('CatalogNumber')==cat)
    a.gdf=a.gdf.group_by(['month','year','SALES_DATE']).sum()
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
        a.ch1=ui.echart({'xAxis':{'data':a.df['month'].unique().to_list()},'yAxis': {},'series': ll}).style('height:360px;width:780px; margin-top:13px; margin-left:0px; margin-right:6px')
        a.ch1.options['series'][0]['data']= a.gdf.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).sort('month',descending=False)['`Fcst DF Final Rev'].to_list()
        a.ch1.options['series'][0]['name']='DF'
        a.ch1.options['series'][1]['data']= a.gdf.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).sort('month',descending=False)['`Fcst Stat Final Rev'].to_list()
        a.ch1.options['series'][1]['name']='Stat'
        a.ch1.options['tooltip']={}
        a.ch1.options['legend']={}
        a.ch1.options['grid']={'right':5,'left':51,'bottom':34,'top':39}
        a.ch1.options['title']={'text':a.cat}
        a.ch1.options['toolbox']={'show': 'true','feature': { 'saveAsImage': {}}}
        for y in a.gdf['year'].unique():
            a.ch1.options['series'][k]['data']= a.gdf.filter(pl.col('year')==y).group_by('month').sum().sort('month',descending=False)['`Act Orders Rev'].to_list()
            a.ch1.options['series'][k]['name']=y
            k+=1
        a.ch2=ui.echart({'xAxis':{'type':'time','axisLabel': {'formatter': '{MMM} {yy}'}},'yAxis': {},'series':[{'type': 'line', 'data': []},{'type': 'line', 'data': []},{'type': 'line', 'data': [],'markLine':{'itemStyle':{'color':'grey'},'data':[]}}] }).style(
            'height:360px;width:650px; margin-top:13px;margin-right:-6px;')
        a.ch2.options['series'][0]['data']= [[i['SALES_DATE'],i['`Fcst DF Final Rev']] for i in a.gdf.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).sort('SALES_DATE',descending=False).iter_rows(named=True)]
        a.ch2.options['series'][0]['name']='DF'
        a.ch2.options['series'][1]['data']= [[i['SALES_DATE'],i['`Fcst Stat Final Rev']] for i in a.gdf.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).sort('SALES_DATE',descending=False).iter_rows(named=True)]
        a.ch2.options['series'][1]['name']='Stat'
        a.ch2.options['series'][2]['data']= [[i['SALES_DATE'],i['`Act Orders Rev']] for i in a.gdf.sort('SALES_DATE',descending=False).iter_rows(named=True)]
        a.ch2.options['series'][2]['markLine']['data']= [{'xAxis':(datetime(today.year,today.month,1)+relativedelta(months=2)).isoformat(timespec="hours"),'name':'Lag2 month'}]
        a.ch2.options['series'][2]['name']='Orders'
        a.ch2.options['tooltip']={'trigger':'axis'}
        a.ch2.options['toolbox']={'show': 'true','feature': { 'saveAsImage': {}}}
        a.ch2.options['legend']={}
        a.ch2.options['title']={'text':a.cat}
        a.ch2.options['grid']={'right':0,'left':51,'bottom':34,'top':39}

def rdf(e):
    a.count = int(ic.value)
    #a.cat=a.cdf['CatalogNumber'][a.count]
    a.cat=a.cdf.filter(pl.col('index')==a.count)['CatalogNumber'].to_list()[0]
    rdf1(a.cat)
    #ic.value = a.count
    catw.value=a.cat

def scat(e):
    a.cat=e.value
    a.count=a.cdf.filter(pl.col('CatalogNumber')==a.cat)['index'].to_list()[0]
    ic.value = a.count
    #print(a.count)
    rdf1(a.cat)

def nf(e):
    a.count += 1
    ic.value = a.count
    a.cat=a.cdf.filter(pl.col('index')==a.count)['CatalogNumber'].to_list()[0]
    rdf1(a.cat)
def pf(e):
    a.count -= 1
    ic.value = a.count
    a.cat=a.cdf.filter(pl.col('index')==a.count)['CatalogNumber'].to_list()[0]
    rdf1(a.cat)

with r1:
    ic = ui.number(label="Rank",step=1,value=0,on_change=rdf).style('min-width: 55px;max-width: 55px; margin-top:-14px')
    prev = ui.button(icon='arrow_drop_up',text='Prev',on_click=pf).style('min-width: 63px;max-width: 63px; padding-left:0px;padding-right:0px; margin-left:0px; margin-right:0px')
    next = ui.button(icon='arrow_drop_down',text='Next',on_click=nf).style('min-width: 63px;max-width: 63px; padding-left:0px;padding-right:0px; margin-left:0px; margin-right:0px')
    lw = ui.select(label='Location',value='Region',with_input=True,options=['Area','Region','Country'],on_change=tmf).style('min-width: 100px;max-width: 100px; margin-top:-14px')
    cow = ui.select(label=lw.value,options=list(df[lw.value].unique()),on_change=hs,with_input=True).style('min-width: 160px; margin-top:-14px')
    pw = ui.select(label='Product',value='IBP Level 5',with_input=True,options=['Franchise','Business Unit','IBP Level 5'],on_change=pmf).style('min-width: 120px;max-width: 120px; margin-top:-14px')
    pvw = ui.select(label=pw.value,options=list(df[pw.value].unique()),on_change=phs,with_input=True).style('min-width: 160px; margin-top:-14px')
    eb = ui.button(text="Export",icon='file_download')
    catw = ui.select(label="CatalogNumber",options=list(df['CatalogNumber'].unique()),on_change=scat,with_input=True).style('min-width: 110px; margin-top:-14px')
    lab = ui.label("        ").style('font-size:1.5em;width: 360px; margin-top:3px')
#gbd1(e=lw)

def scf(e):
    t={'Date':[date.today()],'Rank':[a.count],'CatalogNumber':a.cat,'Country':[cow.value],'Stat fcst':[e.args["newValue"]],
       'month':[int(e.args["colId"])],'year':[a.pdf['year'][e.args["rowIndex"]]],'old fcst':[e.args["oldValue"]],'Franchise':[a.df.filter(pl.col('CatalogNumber')==a.cat)['Franchise'].unique()[0]]}
    a.fdf=pl.concat([a.fdf,pl.DataFrame(t)],how='vertical_relaxed')

def edf(e):
    a.fdf.write_excel('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\over.xlsx')
    ui.notify("Data Exported",type='positive')
eb.on_click(edf)

@ui.page('/sql')
def sql():
    from sql import sqlpd
    import os
    nav()
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

@ui.page('/detail')
def detail():
    nav()
    dr1=ui.row()
    dr2=ui.row()
    def cfg(e):
        ddf=df.filter(pl.col(lw.value)==cow.value)
        ddf=ddf.filter(pl.col('SALES_DATE')==datetime.strptime(date.value,"%Y-%m-%d"))
        ddf=ddf.group_by([lw.value,'IBP Level 5','Franchise','CatalogNumber','SALES_DATE']).sum()
        ddf=ddf.with_columns(pl.col('`Fcst Stat Final Rev').cast(pl.Int64))
        ddf=ddf.with_columns((pl.col('`Fcst Stat Final Rev')-pl.col('`Act Orders Rev')).abs().alias('Diff Act'))
        ddf=ddf.with_columns((pl.col('`Fcst Stat Final Rev')-pl.col('`Fcst DF Final Rev')).abs().alias('Diff Fcst')).sort('Diff Fcst',descending=True)
        ddf=ddf[[lw.value,'Franchise','CatalogNumber','SALES_DATE','`Fcst Stat Final Rev','`Fcst DF Final Rev','Diff Fcst','`Act Orders Rev','Diff Act']]
        dr2.clear()
        with dr2:
            dgr=ui.aggrid({'columnDefs': [{"field":cn,"editable": True,"filter":True, "sortable": True,"floatingFilter":True} for cn in ddf.columns], 'rowData': ddf.to_dicts()}).style('min-width:950px;min-height:1100px;margin-top:-5px')   
       
    with dr1:
        lw= ui.select(label='Location',value='Region',options=['Area','Region','Country'],on_change=tmf).style('min-width: 140px; margin-top:-14px')
        with ui.input('Date',value=(datetime(today.year,today.month,1)+relativedelta(months=2)).strftime("%Y-%m-%d"),on_change=cfg).style('min-width: 140px; margin-top:-14px') as date:
            with ui.menu().style('min-width: 160px;'):
                ui.date(datetime(today.year,today.month,1)-relativedelta(months=1)).bind_value(date)
        cow= ui.select(label=lw.value,options=list(df[lw.value].unique().sort()),on_change=cfg,with_input=True).style('min-width: 160px; margin-top:-14px')

ui.run(title="Forecast Review",binding_refresh_interval=1,reconnect_timeout=70,uvicorn_reload_includes='ngbrp.py')
