####################################################################################
##
##  Programa    :   pi2_analytics_etl.py
##  Fecha       :   Febrero 25; 2023
##  Sinopsis    :   REaliza la descarga diaria de los datos de los ETF
##                  ^GSPC | SPY | IVV | RSP
import pandas as pd
import yfinance as yf
import datetime

strRutaArchivos ='./data/'
lstTickers = ['^GSPC','SPY','IVV','RSP']
lstArchivos = ['spx_data.csv','spy_data.csv','ivv_data.csv','rsp_data.csv']
fechaFin=datetime.datetime.now().date()

for position in range(0,len(lstTickers)-1):
    print('actualizando :',lstTickers[position],' en '+lstArchivos[position])
    #Abre el archivo y busca la última fecha
    ticker_data = pd.read_csv(strRutaArchivos+lstArchivos[position], index_col='Date')
    fechaInicio = datetime.datetime.strptime(ticker_data.index.max(),'%Y-%m-%d') + datetime.timedelta(days=1)
    print('Desde : ',fechaInicio,', hasta :',fechaFin)
    
    #descarga el intervalo fechaInicio - fechaFin usando yfinance
    ticker = yf.download(lstTickers[position], start=fechaInicio, end=fechaFin)    

    #concatena el dataframe leído con el descargado
    ticker_data = pd.concat([ticker_data,ticker],axis=0)

    #guarda los datos del dataframe actualizado
    ticker_data.to_csv(strRutaArchivos+lstArchivos[position])

print('Proceso terminado')