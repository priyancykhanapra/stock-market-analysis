import pandas as pd

tickers = [
    "AAPL", "GOOGL", "AMZN", "MSFT", "META", "TSLA", "F", "NVDA", "KO", "NFLX", "INTC",
    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD", "LINK-USD", "AVAX-USD",
    "SQQQ", "SOXL", "TQQQ", "SPY", "QQQ", "IWM","TNA","TLT","TECE","LQD","EEM","GDX",
    "KRE","SPXS","SHTSL","EEM","LEO","BND","STETH","USDC","FDUSD","FIL","WETH","WBTC","ETRN","ADANI","INFY"

]

df = pd.DataFrame({'ticker': tickers})
df.to_csv('instruments.csv', index=False)
print("created..") 


