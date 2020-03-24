from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.experimental import QTradableStocksUS
from quantopian.pipeline.classifiers.fundamentals import Sector
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.factors import SimpleMovingAverage,AverageDollarVolume
from quantopian.pipeline.data.sentdex import sentiment
from quantopian.algorithm import attach_pipeline,pipeline_output

def initialize(context):
    schedule_function(my_rebalance,date_rules.week_start(),time_rules.market_open())
    my_pipe=make_pipeline()
    attach_pipeline(my_pipe,"COVID 20 pipeline")
    
                    
def my_rebalance(context,data):
    for security in context.portfolio.positions: 
        
        if security not in context.longs and security not in context.shorts and       data.can_trade(security):
            order_target_percent(security,0)
           
            
    for security in context.longs:
        if data.can_trade(security):
            order_target_percent(security,context.long_weight)
    
    for security in context.shorts:
        if data.can_trade(security):
            order_target_percent(security,context.short_weight)
def portfolio_weights(context):
    if len(context.longs)==0:
        long_weights=0
    else:
        long_weights=0.5/len(context.longs)
        
    if len(context.shorts)==0:
        short_weights=0
    else:
        short_weights=-0.5/len(context.shorts)
        
    return long_weights,short_weights


def before_trading_start(context,data):
    context.output = pipeline_output("COVID 20 pipeline")
    
    context.longs = context.output[context.output['longs']].index.tolist()
    
    context.shorts = context.output[context.output['shorts']].index.tolist()
    
    context.long_weight,context.short_weight = portfolio_weights(context)
    
    print(context.output)
        
    
def make_pipeline():
    base_universe=QTradableStocksUS()
    sector=Sector()
    sector2=morningstar.asset_classification.morningstar_sector_code.latest
    
    healthcare_sector=sector.eq(206)
    
    airline_sector=sector2.eq(31053108)
    
    
    sent=sentiment.sentiment_signal.latest
    
    
    
    longs= healthcare_sector and (sent > 3) 
    
    shorts = airline_sector and (sent < 1) 
    
    tradable_securities= (longs | shorts) and base_universe
    
    return Pipeline(
        columns={
            'longs':longs,
            'shorts':shorts
        },
        screen=tradable_securities
    
    )